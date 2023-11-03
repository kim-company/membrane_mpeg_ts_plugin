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
         unsent_buffer_actions: Map.put(state.unsent_buffer_actions, pad, [])
     }}
  end

  @impl true
  def handle_playing(_ctx, state) do
    # Output pad is connected after a PMT table is recognized, hence we have to
    # ask for the first packets by ourselves.
    {[demand: Pad.ref(:input)], state}
  end

  @impl true
  def handle_demand(pad, size, :buffers, _ctx, state) do
    pending = Map.update(state.pending_demand, pad, size, fn old -> old + size end)
    fulfill_demand(%{state | pending_demand: pending})
  end

  @impl true
  def handle_process(:input, buffer, _ctx, state) do
    state
    |> update_in([:demuxer], &TS.Demuxer.push_buffer(&1, buffer.payload))
    |> fulfill_demand()
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    demuxer = TS.Demuxer.end_of_stream(state.demuxer)
    fulfill_demand(%{state | closed: true, demuxer: demuxer})
  end

  @impl true
  def handle_info(
        :start_stream_filter,
        _ctx,
        state = %{pending_demand: demand}
      ) do
    # Remove unfollowed tracks.
    followed_stream_ids = Enum.map(demand, fn {{_, _, {:stream_id, sid}}, _} -> sid end)

    demuxer = %TS.Demuxer{state.demuxer | packet_filter: &(&1 in followed_stream_ids)}

    Logger.warning(
      "PES filtering enabled. Following streams #{inspect(followed_stream_ids)}",
      domain: __MODULE__
    )

    {[], %{state | demuxer: demuxer}}
  end

  defp fulfill_demand(state = %{state: :waiting_pmt}) do
    # Eventually I would prefer to drop the state differentiation and notify
    # the pipeline each time a different PMT table is parsed.
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

  defp fulfill_demand(state = %{state: :online, demuxer: demuxer}) do
    # Fetch packet buffers for each pad.
    {buf, demuxer} =
      state.pending_demand
      |> Enum.map_reduce(demuxer, fn {pad = {_, _, {:stream_id, sid}}, size}, demuxer ->
        {packets, demuxer} = TS.Demuxer.take(demuxer, sid, size)
        left = TS.Demuxer.size(demuxer, sid)

        {{pad, packets, left}, demuxer}
      end)

    # Given the buffers obtained for each pad, update their demand.
    updated_demand =
      Enum.reduce(buf, state.pending_demand, fn {pad, packets, _}, acc ->
        Map.update!(acc, pad, fn size -> size - length(packets) end)
      end)

    # For each pad that could not be fulfilled, generate a demand action. As
    # soon as we do not know how the source pad is providing data to us (may be
    # a TS file for each demand for example), there is no strategy here on how
    # much to request. If the source has been closed and we do not have more
    # data, it means we're ready to close the pad.
    demand_or_close_actions =
      if state.closed do
        buf
        |> Enum.filter(fn {_pad, _packets, left} -> left == 0 end)
        |> Enum.filter(fn {pad, _packets, _} -> Map.has_key?(updated_demand, pad) end)
        |> Enum.map(fn {pad, _, _} -> {:end_of_stream, pad} end)
      else
        [{:demand, Pad.ref(:input)}]
      end

    # Now we know which pads will received an :end_of_stream action. Remove
    # them from the pending_demand map to avoid sending messages to these pads
    # again.
    updated_demand =
      demand_or_close_actions
      |> Enum.filter(fn {action, _} -> action == :end_of_stream end)
      |> Enum.reduce(state.pending_demand, fn {_, pad}, acc ->
        Map.delete(acc, pad)
      end)

    buffer_actions =
      buf
      |> Enum.filter(fn {_pad, packets, _} -> length(packets) > 0 end)
      |> Enum.map(fn {pad = {Membrane.Pad, _, {:stream_id, sid}}, packets, _} ->
        {:buffer,
         {pad,
          Enum.map(packets, fn x ->
            %Membrane.Buffer{
              payload: x.data,
              pts: parse_pts_or_dts(x.pts),
              dts: parse_pts_or_dts(x.dts),
              metadata: %{
                stream_id: sid,
                is_aligned: x.is_aligned
              }
            }
          end)}}
      end)

    {buffer_actions, state} = maybe_update_stream_format(buffer_actions, state)

    actions = buffer_actions ++ demand_or_close_actions
    state = %{state | pending_demand: updated_demand, demuxer: demuxer}

    {actions, state}
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

  defp parse_pts_or_dts(nil), do: nil

  defp parse_pts_or_dts(ts) do
    use Ratio
    (ts * Membrane.Time.second() / @h264_time_base) |> Ratio.trunc()
  end

  defp new_state() do
    %{
      state: :waiting_pmt,
      demuxer: TS.Demuxer.new(),
      pending_demand: %{},
      closed: false,
      unsent_buffer_actions: %{},
      is_last_aligned: %{}
    }
  end
end
