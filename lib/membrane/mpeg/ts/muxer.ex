defmodule Membrane.MPEG.TS.Muxer do
  @moduledoc """
  Experimental MPEG-TS Muxer. Supports 1 program with AAC and H264 in it only for now.

  Inputs must be attached before the element enters the playing state. Audio&Video
  are going to be interleaved by their timing.

  Each buffer is going to end in its own PES packet, hence NALu units must be grouped
  accordingly, as well as ADTS AAC frames.
  """

  use Membrane.Filter
  alias MPEG.TS
  alias Membrane.TimestampQueue

  @queue_buffer Membrane.Time.milliseconds(200)

  def_input_pad(:input,
    accepted_format: _any,
    availability: :on_request,
    options: [
      stream_type: [
        spec: atom(),
        description: """
        Each input is going to become a stream in the PMT with this assigned type.
        See MPEG.TS.PMT.
        """
      ]
    ]
  )

  def_output_pad(:output,
    accepted_format: Membrane.RemoteStream
  )

  @impl true
  def handle_init(_ctx, _opts) do
    queue =
      TimestampQueue.new(
        pause_demand_boundary: {:time, @queue_buffer},
        synchronization_strategy: :explicit_offsets
      )

    state = %{
      muxer: TS.Muxer.new(),
      pad_to_stream: %{},
      queue: queue
    }

    {[], state}
  end

  @impl true
  def handle_stream_format(_pad, _format, _ctx, state) do
    {[], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
    format_actions = [stream_format: {:output, %Membrane.RemoteStream{}}]
    # Write PAT and PMT at startup, then periodically or at each PMT update.
    {buffer_actions, state} = mux_pat_pmt(state)
    {format_actions ++ buffer_actions, state}
  end

  @impl true
  def handle_end_of_stream(pad, _ctx, state) do
    state.queue
    |> TimestampQueue.push_end_of_stream(pad)
    |> TimestampQueue.pop_available_items()
    |> handle_queue_output(state)
  end

  @impl true
  def handle_pad_added(pad, ctx, state) do
    stream_type = ctx.pad_options[:stream_type]
    stream_category = TS.PMT.get_stream_category(stream_type)

    {pid, state} =
      get_and_update_in(state, [:muxer], fn muxer ->
        # TODO: we could indicated this stream as PCR carrier if needed.
        TS.Muxer.add_elementary_stream(muxer, stream_type)
      end)

    state =
      state
      |> put_in([:pad_to_stream, pad], %{pid: pid, type: stream_type, category: stream_category})
      |> update_in([:queue], &TimestampQueue.register_pad(&1, pad))

    if ctx.playback == :playing do
      # Each time a pad is added at runtime, update the PMT.
      mux_pat_pmt(state)
    else
      {[], state}
    end
  end

  @impl true
  def handle_buffer(pad, buffer, _ctx, state) do
    state.queue
    |> TimestampQueue.push_buffer_and_pop_available_items(pad, buffer)
    |> handle_queue_output(state)
  end

  defp handle_queue_output({suggested_actions, items, queue}, state) do
    state = %{state | queue: queue}
    {actions, state} = Enum.flat_map_reduce(items, state, &handle_queue_item/2)
    {suggested_actions ++ actions ++ maybe_end_of_stream(state), state}
  end

  defp handle_queue_item({pad, {:buffer, buffer}}, state) do
    stream = get_in(state, [:pad_to_stream, pad])

    {packet_or_packets, state} =
      get_and_update_in(state, [:muxer], fn muxer ->
        case stream.category do
          :video ->
            is_keyframe? = Map.get(buffer.metadata, :is_keyframe?, false)

            {packets, muxer} =
              TS.Muxer.mux_sample(muxer, stream.pid, buffer.payload, ns_to_ts(buffer.pts),
                dts: ns_to_ts(buffer.dts),
                sync?: is_keyframe?
              )

            if is_keyframe? do
              {pat, muxer} = TS.Muxer.mux_pat(muxer)
              {pmt, muxer} = TS.Muxer.mux_pmt(muxer)
              {[pat, pmt] ++ packets, muxer}
            else
              {packets, muxer}
            end

          :audio ->
            TS.Muxer.mux_sample(muxer, stream.pid, buffer.payload, ns_to_ts(buffer.pts),
              sync?: true
            )

          _ ->
            psi = get_in(buffer, [Access.key!(:metadata), :psi])

            if psi != nil do
              TS.Muxer.mux_psi(muxer, stream.pid, psi)
            else
              Membrane.Logger.warning(
                "Could not mux packet on stream #{inspect(stream)}: expected buffer.metadata.psi"
              )

              {[], muxer}
            end
        end
      end)

    buffers =
      packet_or_packets
      |> List.wrap()
      |> Enum.map(fn x ->
        {pts, dts} = if x.pusi, do: {buffer.pts, buffer.dts}, else: {nil, nil}

        %Membrane.Buffer{
          payload: marshal_payload(x),
          pts: pts,
          dts: dts,
          metadata: extract_metadata(x)
        }
      end)

    {[buffer: {:output, buffers}], state}
  end

  defp handle_queue_item(_, state), do: {[], state}

  defp maybe_end_of_stream(state) do
    if TimestampQueue.pads(state.queue) |> MapSet.size() == 0 do
      [end_of_stream: :output]
    else
      []
    end
  end

  defp ns_to_ts(nil), do: nil
  defp ns_to_ts(ns), do: round(ns * 90_000 / 1.0e9)

  defp marshal_payload(packet) do
    packet
    |> TS.Marshaler.marshal()
    |> IO.iodata_to_binary()
  end

  defp extract_metadata(packet) do
    %{
      pusi: packet.pusi,
      pid: packet.pid,
      rai: packet.random_access_indicator,
      pid_class: packet.pid_class
    }
  end

  defp mux_pat_pmt(state) do
    {pat, state} = get_and_update_in(state, [:muxer], &TS.Muxer.mux_pat(&1))
    {pmt, state} = get_and_update_in(state, [:muxer], &TS.Muxer.mux_pmt(&1))

    buffers =
      Enum.map([pat, pmt], fn x ->
        %Membrane.Buffer{payload: marshal_payload(x), metadata: extract_metadata(x)}
      end)

    {[buffer: {:output, buffers}], state}
  end
end
