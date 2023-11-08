defmodule Membrane.MPEG.TS.Demuxer do
  @moduledoc """
  Demuxes MPEG TS stream.

  After transition into playing state, this element will wait for [Program
  Association Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PAT)
  and [Program Mapping
  Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PMT).
  ```
  """
  use Membrane.Filter
  require Logger

  alias MPEG.TS

  @h264_time_base 90_000

  # Amount of seconds after which the demuxer will start filtering out streams
  # that are not being followed.
  @stream_filter_timeout 5_000

  def_input_pad(:input,
    accepted_format: %Membrane.RemoteStream{},
    demand_unit: :buffers,
    demand_mode: :manual
  )

  def_output_pad(:output,
    availability: :on_request,
    accepted_format: %module{} when module in [Membrane.RemoteStream, Membrane.H264]
  )

  @type state_t :: :waiting_pmt | :online

  @impl true
  def handle_init(_ctx, _opts) do
    {[], new_state()}
  end

  @impl true
  def handle_pad_added(pad, _ctx, state) do
    {[],
     %{
       state
       | is_last_aligned: Map.put(state.is_last_aligned, pad, nil),
         unsent_buffer_actions: Map.put(state.unsent_buffer_actions, pad, []),
         last_chunk_id_per_pad: Map.put(state.last_chunk_id_per_pad, pad, nil),
         first_chunk_ts_per_pad: Map.put(state.first_chunk_ts_per_pad, pad, nil)
     }}
  end

  @impl true
  def handle_playing(_ctx, state) do
    # Output pad is connected after a PMT table is recognized, hence we have to
    # ask for the first packets by ourselves.
    {[demand: Pad.ref(:input)], state}
  end

  @impl true
  def handle_demand(_pad, size, :buffers, _ctx, state) do
    {[demand: {:input, size}], state}
  end

  @impl true
  def handle_process(:input, buffer, ctx, state) do
    state = %{state | chunk_id: state.chunk_id + 1}
    process_buffer(buffer, state, Map.keys(ctx.pads))
  end

  @impl true
  def handle_end_of_stream(:input, ctx, state) do
    demuxer = TS.Demuxer.end_of_stream(state.demuxer)
    state = %{state | closed: true, demuxer: demuxer}

    actions =
      Map.keys(ctx.pads)
      |> Enum.filter(fn
        {Membrane.Pad, :output, _id} -> true
        _other -> false
      end)
      |> Enum.map(fn pad_name -> {:end_of_stream, pad_name} end)

    {actions, state}
    # TODO - eos can be handled not simuntaniously for all pads
  end

  @impl true
  def handle_event(:input, %Membrane.Event.Discontinuity{} = discontinuity, _ctx, state) do
    {[], %{state | discontinuty_duration: discontinuity.duration}}
  end

  @impl true
  def handle_event(:input, event, _ctx, state) do
    {[forward: event], state}
  end

  @impl true
  def handle_info(
        :start_stream_filter,
        ctx,
        state = %{pending_demand: demand}
      ) do
    # Remove unfollowed tracks.
    followed_stream_ids = Enum.map(demand, fn {{_, _, {:stream_id, sid}}, _} -> sid end)

    demuxer = %TS.Demuxer{state.demuxer | packet_filter: &(&1 in followed_stream_ids)}

    Logger.warning(
      "PES filtering enabled. Following streams #{inspect(followed_stream_ids)}",
      domain: __MODULE__
    )

    redemand_actions =
      ctx.pads
      |> Enum.filter(fn
        {Membrane.Pad, :output, _id} -> true
        _other -> false
      end)
      |> Enum.map(fn pad -> {:redemand, pad} end)

    {redemand_actions, %{state | demuxer: demuxer}}
  end

  defp process_buffer(buffer, state = %{state: :waiting_pmt}, _pads) do
    # Eventually I would prefer to drop the state differentiation and notify
    # the pipeline each time a different PMT table is parsed.
    state =
      update_in(
        state,
        [:demuxer],
        &TS.Demuxer.push_buffer(&1, buffer.payload, buffer.metadata.from, state.chunk_id)
      )

    pmt = state.demuxer.pmt

    if pmt != nil do
      Process.send_after(self(), :start_stream_filter, @stream_filter_timeout)
      actions = [{:notify_parent, {:mpeg_ts_pmt, pmt}}]
      state = %{state | state: :online}
      {actions, state}
    else
      actions = [{:demand, Pad.ref(:input)}]
      {actions, state}
    end
  end

  defp process_buffer(buffer, state = %{state: :online}, pads) do
    state =
      update_in(
        state,
        [:demuxer],
        &TS.Demuxer.push_buffer(&1, buffer.payload, buffer.metadata.from, state.chunk_id)
      )

    # Fetch packet buffers for each pad.
    {buf, demuxer} =
      pads
      |> Enum.filter(fn
        {Membrane.Pad, :output, _id} -> true
        _other -> false
      end)
      |> Enum.map_reduce(state.demuxer, fn pad = {Membrane.Pad, :output, {:stream_id, sid}},
                                           demuxer ->
        size = TS.Demuxer.size(demuxer, sid)
        {packets, demuxer} = TS.Demuxer.take(demuxer, sid, size)

        {{pad, packets}, demuxer}
      end)

    {buffer_actions, state} =
      buf
      |> Enum.filter(fn {_pad, packets} -> length(packets) > 0 end)
      |> Enum.map_reduce(state, fn {pad = {Membrane.Pad, _, {:stream_id, sid}}, packets}, state ->
        IO.inspect("NEW PAD")

        {buffers, {last_chunk_id, first_chunk_ts}} =
          Enum.map_reduce(
            packets,
            {state.last_chunk_id_per_pad[pad], state.first_chunk_ts_per_pad[pad]},
            fn x, {last_chunk_id, first_chunk_ts} ->
              {last_chunk_id, first_chunk_ts} =
                if last_chunk_id != x.chunk_id do
                  first_chunk_ts = (x.dts || x.pts) * Membrane.Time.second() / @h264_time_base
                  {x.chunk_id, first_chunk_ts}
                else
                  {last_chunk_id, first_chunk_ts}
                end

              pts =
                parse_pts_or_dts(x.pts, first_chunk_ts, Membrane.Time.seconds(Ratio.new(x.from)))

              dts =
                parse_pts_or_dts(x.dts, first_chunk_ts, Membrane.Time.seconds(Ratio.new(x.from)))

              IO.inspect({dts, x.from, first_chunk_ts, x.chunk_id}, label: :dts)
              # IO.inspect(x.from, label: :from123)
              # IO.inspect(x.chunk_id, label: :chunk_id123)
              # IO.inspect(first_chunk_ts, label: :first_chunk_ts123)

              {%Membrane.Buffer{
                 payload: x.data,
                 pts: pts,
                 dts: dts,
                 metadata: %{
                   stream_id: sid,
                   is_aligned: x.is_aligned
                 }
               }, {last_chunk_id, first_chunk_ts}}
            end
          )

        state = %{
          state
          | last_chunk_id_per_pad: Map.put(state.last_chunk_id_per_pad, pad, last_chunk_id),
            first_chunk_ts_per_pad: Map.put(state.first_chunk_ts_per_pad, pad, first_chunk_ts)
        }

        {{:buffer, {pad, buffers}}, state}
      end)

    {buffer_actions, state} = maybe_update_stream_format(buffer_actions, state)
    state = %{state | demuxer: demuxer, discontinuity_duration: nil}

    redemand_actions =
      pads
      |> Enum.filter(fn
        {Membrane.Pad, :output, _id} -> true
        _other -> false
      end)
      |> Enum.map(fn pad -> {:redemand, pad} end)

    {buffer_actions ++ redemand_actions, state}
  end

  defp maybe_update_stream_format(buffer_actions, state) do
    flat_buffer_actions_for_all_pads =
      Enum.flat_map(buffer_actions, fn
        {:buffer, {pad, buffers}} when is_list(buffers) ->
          Enum.map(buffers, &{:buffer, {pad, &1}})

        {:buffer, {pad, buffer}} ->
          [buffer: {pad, buffer}]
      end)
      |> Enum.chunk_by(fn {:buffer, {pad, _buffer}} -> pad end)

    Enum.flat_map_reduce(flat_buffer_actions_for_all_pads, state, fn flat_buffer_actions_per_pad,
                                                                     state ->
      [{:buffer, {pad, _buffer}} | _rest] = flat_buffer_actions_per_pad
      maybe_update_stream_format_per_pad(pad, state, flat_buffer_actions_per_pad)
    end)
  end

  defp maybe_update_stream_format_per_pad(pad, state, actions) do
    all_actions = state.unsent_buffer_actions[pad] ++ actions
    shifted_actions = Enum.slice(all_actions, 1..-1) ++ [nil]

    Enum.zip(all_actions, shifted_actions)
    |> Enum.flat_map_reduce(state, fn
      {this_action, nil}, state ->
        {[],
         %{
           state
           | unsent_buffer_actions: Map.put(state.unsent_buffer_actions, pad, [this_action])
         }}

      {this_action, next_action}, state ->
        {:buffer, {^pad, this_buffer}} = this_action
        {:buffer, {^pad, next_buffer}} = next_action

        {stream_format_actions, state} =
          cond do
            this_buffer.metadata.is_aligned and next_buffer.metadata.is_aligned and
                not (state.is_last_aligned[pad] || false) ->
              {[stream_format: {pad, get_format(pad, state, true)}],
               %{state | is_last_aligned: Map.put(state.is_last_aligned, pad, true)}}

            next_buffer.metadata.is_aligned == false and
                (state.is_last_aligned[pad] == true or state.is_last_aligned[pad] == nil) ->
              {[stream_format: {pad, get_format(pad, state, false)}],
               %{state | is_last_aligned: Map.put(state.is_last_aligned, pad, false)}}

            true ->
              {[], state}
          end

        {stream_format_actions ++ [this_action], state}
    end)
  end

  defp get_format(pad, state, is_aligned) do
    {_, _, {:stream_id, sid}} = pad

    cond do
      Map.fetch!(state.demuxer.pmt.streams, sid)[:stream_type] == :H264 and is_aligned ->
        %Membrane.H264{alignment: :au}

      true ->
        %Membrane.RemoteStream{}
    end
  end

  defp parse_pts_or_dts(nil, _initial_offset, _offset), do: nil

  defp parse_pts_or_dts(ts, initial_offset, offset) do
    use Ratio

    (ts * Membrane.Time.second() / @h264_time_base - initial_offset + (offset || 0))
    |> Ratio.trunc()
  end

  defp new_state() do
    %{
      state: :waiting_pmt,
      demuxer: TS.Demuxer.new(),
      pending_demand: %{},
      closed: false,
      unsent_buffer_actions: %{},
      is_last_aligned: %{},
      discontinuity_duration: nil,
      chunk_id: 0,
      last_chunk_id_per_pad: %{},
      first_chunk_ts_per_pad: %{}
    }
  end
end
