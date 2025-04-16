defmodule Membrane.MPEG.TS.Demuxer do
  @moduledoc """
  Demuxes MPEG TS stream.

  After transition into playing state, this element will wait for [Program
  Association Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PAT)
  and [Program Mapping
  Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PMT).
  """
  use Membrane.Filter
  require Logger

  alias MPEG.TS

  # Amount of seconds after which the demuxer will start filtering out streams
  # that are not being followed.
  @stream_filter_timeout 5_000

  def_input_pad(:input,
    accepted_format: %Membrane.RemoteStream{},
    demand_unit: :buffers,
    flow_control: :manual
  )

  def_output_pad(:output,
    availability: :on_request,
    accepted_format: %module{} when module in [Membrane.RemoteStream, Membrane.H264],
    flow_control: :manual
  )

  @type state_t :: :waiting_pmt | :waiting_for_pads | :online

  @impl true
  def handle_init(_ctx, _opts) do
    {[], new_state()}
  end

  @impl true
  def handle_pad_added(pad, ctx, state) do
    new_fsm_state =
      if state.state == :waiting_for_pads and
           Map.keys(state.demuxer.pmt.streams)
           |> Enum.map(&{Membrane.Pad, :output, {:stream_id, &1}})
           |> Enum.all?(&(&1 in Map.keys(ctx.pads))) do
        :online
      else
        state.state
      end

    state = %{
      state
      | is_last_aligned: Map.put(state.is_last_aligned, pad, nil),
        unsent_buffer_actions_per_pad: Map.put(state.unsent_buffer_actions_per_pad, pad, []),
        state: new_fsm_state
    }

    if state.closed do
      process_end_of_stream(state, ctx.pads)
    else
      {actions, state} = get_buffers(state, Map.keys(ctx.pads))
      redemands = get_redemands(Map.keys(ctx.pads))
      {actions ++ redemands, state}
    end
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
  def handle_buffer(:input, buffer, ctx, state) do
    state =
      update_in(
        state,
        [:demuxer],
        &TS.Demuxer.push_buffer(
          &1,
          buffer.payload,
          Map.get(buffer.metadata, :discontinuity) || false
        )
      )

    {actions, state} = get_buffers(state, Map.keys(ctx.pads))

    redemands = get_redemands(Map.keys(ctx.pads))

    {actions ++ redemands, state}
  end

  defp get_redemands(pad_names) do
    Enum.filter(pad_names, fn
      {Membrane.Pad, :output, _id} -> true
      _other -> false
    end)
    |> Enum.map(fn pad -> {:redemand, pad} end)
  end

  @impl true
  def handle_end_of_stream(:input, ctx, state) do
    demuxer = TS.Demuxer.end_of_stream(state.demuxer)
    state = %{state | closed: true, demuxer: demuxer}
    process_end_of_stream(state, ctx.pads)
  end

  defp process_end_of_stream(state, pads) do
    {flushed_actions, state} = flush_buffers(state, Map.keys(pads))

    eos_actions =
      Enum.filter(pads, fn
        {{Membrane.Pad, :output, _id}, pad} when pad.end_of_stream? == false ->
          true

        {_other, _pad} ->
          false
      end)
      |> Enum.map(fn {pad_name, _pad} -> {:end_of_stream, pad_name} end)

    actions = flushed_actions ++ eos_actions
    {actions, state}
  end

  defp flush_buffers(state, pad_names) do
    {buffers_actions, state} = get_buffers(state, pad_names)

    {last_buffer_actions, state} =
      Enum.flat_map(state.unsent_buffer_actions_per_pad, fn {_pad, actions} -> actions end)
      |> update_actions_with_events(state)

    unsent_buffer_actions_per_pad =
      Enum.map(state.unsent_buffer_actions_per_pad, fn {k, _v} -> {k, []} end)
      |> Enum.into(Map.new())

    state = %{state | unsent_buffer_actions_per_pad: unsent_buffer_actions_per_pad}
    {buffers_actions ++ last_buffer_actions, state}
  end

  @impl true
  def handle_event(:input, event, _ctx, state) do
    {[forward: event], state}
  end

  @impl true
  def handle_info(
        :start_stream_filter,
        ctx,
        state
      ) do
    # Remove unfollowed tracks.
    followed_stream_ids =
      Map.keys(ctx.pads)
      |> Enum.filter(fn
        {Membrane.Pad, :output, _id} -> true
        _other -> false
      end)
      |> Enum.map(fn {_, _, {:stream_id, sid}} -> sid end)

    demuxer = %TS.Demuxer{state.demuxer | packet_filter: &(&1 in followed_stream_ids)}

    Logger.debug(
      "PES filtering enabled. Following streams #{inspect(followed_stream_ids)}",
      domain: __MODULE__
    )

    redemand_actions =
      ctx.pads
      |> Enum.filter(fn {_ref, pad} -> not pad.end_of_stream? and pad.direction == :output end)
      |> Enum.map(fn {ref, _pad} -> {:redemand, ref} end)

    {redemand_actions, %{state | demuxer: demuxer, state: :online}}
  end

  defp get_buffers(state = %{state: :waiting_pmt}, _pad_names) do
    # Eventually I would prefer to drop the state differentiation and notify
    # the pipeline each time a different PMT table is parsed.

    pmt = state.demuxer.pmt

    if pmt != nil do
      Process.send_after(self(), :start_stream_filter, @stream_filter_timeout)
      actions = [{:notify_parent, {:mpeg_ts_pmt, pmt}}]
      state = %{state | state: :waiting_for_pads}
      {actions, state}
    else
      actions = [{:demand, Pad.ref(:input)}]
      {actions, state}
    end
  end

  defp get_buffers(state, pads_names) do
    # Fetch packet buffers for each pad.

    {buf, demuxer} =
      Enum.filter(pads_names, fn
        {Membrane.Pad, :output, _id} -> true
        _other -> false
      end)
      |> Enum.map_reduce(state.demuxer, fn pad = {Membrane.Pad, :output, {:stream_id, sid}},
                                           demuxer ->
        size = TS.Demuxer.size(demuxer, sid)
        {packets, demuxer} = TS.Demuxer.take(demuxer, sid, size)

        {{pad, packets}, demuxer}
      end)

    buffer_actions =
      buf
      |> Enum.filter(fn {_pad, packets} -> length(packets) > 0 end)
      |> Enum.map(fn {pad = {Membrane.Pad, _, {:stream_id, sid}}, packets} ->
        buffers =
          Enum.map(
            packets,
            fn x ->
              %Membrane.Buffer{
                payload: x.data,
                pts: x.pts,
                dts: x.dts,
                metadata: %{
                  stream_id: sid,
                  is_aligned: x.is_aligned,
                  discontinuity: x.discontinuity
                }
              }
            end
          )

        {:buffer, {pad, buffers}}
      end)

    {actions_with_events, state} = update_actions_with_events(buffer_actions, state)
    state = %{state | demuxer: demuxer}

    {actions_with_events, state}
  end

  defp update_actions_with_events(buffer_actions, state) do
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

      {actions, state} =
        maybe_update_stream_format_per_pad(pad, state, flat_buffer_actions_per_pad)

      maybe_send_discontinuity_per_pad(pad, state, actions)
    end)
  end

  defp maybe_update_stream_format_per_pad(pad, state, actions) do
    all_actions = state.unsent_buffer_actions_per_pad[pad] ++ actions
    shifted_actions = Enum.slice(all_actions, 1..-1//1) ++ [nil]

    Enum.zip(all_actions, shifted_actions)
    |> Enum.flat_map_reduce(state, fn
      {this_action, nil}, state ->
        {[],
         %{
           state
           | unsent_buffer_actions_per_pad:
               Map.put(state.unsent_buffer_actions_per_pad, pad, [this_action])
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

  defp maybe_send_discontinuity_per_pad(pad, state, actions) do
    Enum.flat_map_reduce(actions, state, fn action, state ->
      case action do
        {:buffer, {^pad, buffer}} = action ->
          actions =
            if buffer.metadata.discontinuity and
                 (state.discontinuity_per_pad[pad] || false) == false do
              [event: {pad, %Membrane.Event.Discontinuity{}}] ++ [action]
            else
              [action]
            end

          state = %{
            state
            | discontinuity_per_pad:
                Map.put(state.discontinuity_per_pad, pad, buffer.metadata.discontinuity)
          }

          {actions, state}

        other_action ->
          {[other_action], state}
      end
    end)
  end

  defp get_format(_pad, _state, _is_aligned) do
    %Membrane.RemoteStream{}
  end

  defp new_state() do
    %{
      state: :waiting_pmt,
      demuxer: TS.Demuxer.new(),
      closed: false,
      unsent_buffer_actions_per_pad: %{},
      is_last_aligned: %{},
      discontinuity_per_pad: %{}
    }
  end
end
