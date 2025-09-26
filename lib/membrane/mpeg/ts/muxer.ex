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
        spec: atom() | nil,
        default: nil,
        description: """
        Each input is going to become a stream in the PMT with this assigned type.
        See MPEG.TS.PMT.
        """
      ],
      wait_on_buffers?: [
        spec: boolean(),
        default: true,
        description: "Block muxer until a buffer on this pad arrives."
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
        synchronization_strategy: :synchronize_on_arrival
      )

    state = %{
      muxer: TS.Muxer.new(),
      pad_to_stream: %{},
      queue: queue
    }

    {[], state}
  end

  @impl true
  def handle_stream_format(
        pad,
        %Membrane.RemoteStream{content_format: %TS.StreamFormat{stream_type: stream_type}},
        ctx,
        state
      ) do
    handle_stream(pad, stream_type, ctx, state)
  end

  def handle_stream_format(pad, _format, _ctx, state) when is_map_key(state.pad_to_stream, pad) do
    {[], state}
  end

  def handle_stream_format(pad, _format, _ctx, _state) do
    raise RuntimeError, """
    Pad #{inspect(pad)} did not specify its stream_type neither with pad_options
    not with stream_format.
    """
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
    handle_stream(pad, stream_type, ctx, state)
  end

  @impl true
  def handle_buffer(pad, buffer, _ctx, state) do
    state.queue
    |> TimestampQueue.push_buffer_and_pop_available_items(pad, buffer)
    |> handle_queue_output(state)
    |> unblock_awaiting_pads()
  end

  defp unblock_awaiting_pads({actions, state}) do
    state =
      update_in(state, [:queue, :awaiting_pads], fn pads ->
        Enum.filter(pads, fn pad ->
          stream = get_in(state, [:pad_to_stream, pad])
          stream[:wait_on_buffers?]
        end)
      end)

    {actions, state}
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
              TS.Muxer.mux_sample(muxer, stream.pid, buffer.payload, buffer.pts,
                dts: buffer.dts,
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
            TS.Muxer.mux_sample(muxer, stream.pid, buffer.payload, buffer.pts, sync?: true)

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
        %Membrane.Buffer{
          payload: marshal_payload(x),
          pts: buffer.pts,
          dts: buffer.dts,
          metadata: extract_metadata(x)
        }
      end)

    {[buffer: {:output, buffers}], state}
  end

  defp handle_queue_item(_, state), do: {[], state}

  # We have to stream_type information, we'll wait for the stream_format event.
  defp handle_stream(_pad, nil, _ctx, state), do: {[], state}

  # The stream has already been added.
  defp handle_stream(pad, _stream_format, _ctx, state) when is_map_key(state.pad_to_stream, pad),
    do: {[], state}

  defp handle_stream(pad, stream_type, ctx, state) do
    stream_category = TS.PMT.get_stream_category(stream_type)
    wait_on_buffers? = ctx.pads[pad].options[:wait_on_buffers?]

    {pid, state} =
      get_and_update_in(state, [:muxer], fn muxer ->
        # TODO: PCR?
        program_info =
          case stream_type do
            :SCTE_35_SPLICE -> [%{tag: 5, data: "CUEI"}]
            _ -> []
          end

        TS.Muxer.add_elementary_stream(muxer, stream_type, program_info: program_info)
      end)

    Membrane.Logger.info("Binding #{inspect(pad)} to #{pid} (#{inspect(stream_type)})")

    state =
      state
      |> put_in([:pad_to_stream, pad], %{
        pid: pid,
        type: stream_type,
        category: stream_category,
        wait_on_buffers?: wait_on_buffers?
      })
      |> update_in(
        [:queue],
        &TimestampQueue.register_pad(&1, pad, wait_on_buffers?: wait_on_buffers?)
      )

    if ctx.playback == :playing do
      # Each time a pad is added at runtime, update the PMT.
      mux_pat_pmt(state)
    else
      {[], state}
    end
  end

  defp maybe_end_of_stream(state) do
    if TimestampQueue.pads(state.queue) |> MapSet.size() == 0 do
      [end_of_stream: :output]
    else
      []
    end
  end

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
