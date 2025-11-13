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
  alias Membrane.MPEG.TS.Muxer.CorruptionError

  require Membrane.Logger

  @queue_buffer Membrane.Time.milliseconds(200)

  # PAT/PMT insertion interval per ISO/IEC 13818-1 spec (max 500ms)
  @pat_pmt_interval Membrane.Time.milliseconds(500)

  # Standard MPEG-TS system PIDs
  @pat_pid 0x0000
  @cat_pid 0x0001
  @pmt_pid 0x1000

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
      pid: [
        spec: pos_integer() | nil,
        default: nil,
        description: """
        Allows to specify the PID in which this stream should be muxed.
        """
      ],
      wait_on_buffers?: [
        spec: boolean(),
        default: true,
        description: "Block muxer until a buffer on this pad arrives."
      ],
      pcr?: [
        spec: boolean(),
        default: false,
        description: """
        Mark this stream as the PCR (Program Clock Reference) stream.
        This sets the stream's PID as pcr_pid in the PMT and embeds PCR values
        in the adaptation field of packets. Typically used for video streams.
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
      queue: queue,
      last_dts: nil,
      last_pat_pmt_dts: nil,
      # Track all valid PIDs that should appear in the stream
      # Initially includes system PIDs (PAT, CAT, PMT)
      valid_pids: MapSet.new([@pat_pid, @cat_pid, @pmt_pid])
    }

    {[], state}
  end

  @impl true
  def handle_stream_format(
        pad,
        %Membrane.RemoteStream{
          content_format: %Membrane.MPEG.TS.StreamFormat{stream_type: stream_type}
        },
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
    # Send only stream format; PAT/PMT will be sent with first media buffer
    {[stream_format: {:output, %Membrane.RemoteStream{}}], state}
  end

  @impl true
  def handle_end_of_stream(pad, ctx, state) do
    state.queue
    |> TimestampQueue.push_end_of_stream(pad)
    |> TimestampQueue.pop_available_items()
    |> handle_queue_output(ctx, state)
  end

  @impl true
  def handle_pad_added(pad, ctx, state) do
    stream_type = ctx.pad_options[:stream_type]
    handle_stream(pad, stream_type, ctx, state)
  end

  @impl true
  def handle_buffer(pad, buffer, ctx, state) do
    state.queue
    |> TimestampQueue.push_buffer_and_pop_available_items(pad, buffer)
    |> handle_queue_output(ctx, state)
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

  defp handle_queue_output({suggested_actions, items, queue}, ctx, state) do
    state = %{state | queue: queue}
    {actions, state} = Enum.flat_map_reduce(items, state, &handle_queue_item/2)
    {suggested_actions ++ actions ++ maybe_end_of_stream(ctx, state), state}
  end

  defp handle_queue_item({pad, {:buffer, buffer}}, state) do
    stream = get_in(state, [:pad_to_stream, pad])

    # Extract DTS from buffer (use buffer.dts for video, buffer.pts for audio)
    current_dts =
      case stream.category do
        :video -> buffer.dts || buffer.pts
        :audio -> buffer.pts
        _ -> nil
      end

    # Update last_dts if we have a valid one
    state = if current_dts != nil, do: %{state | last_dts: current_dts}, else: state

    # Determine if we should send PAT/PMT based on:
    # 1. Never sent before (last_pat_pmt_dts == nil)
    # 2. 500ms elapsed since last send (per ISO/IEC 13818-1 spec)
    # 3. Video keyframe (good practice for stream switching)
    should_send_pat_pmt? =
      current_dts != nil and
        (state.last_pat_pmt_dts == nil or
           current_dts - state.last_pat_pmt_dts >= @pat_pmt_interval or
           (stream.category == :video and Map.get(buffer.metadata, :is_keyframe?, false)))

    # Send PAT/PMT if needed
    {pat_pmt_actions, state} =
      if should_send_pat_pmt? do
        {actions, state} = mux_pat_pmt(state, current_dts)
        {actions, %{state | last_pat_pmt_dts: current_dts}}
      else
        {[], state}
      end

    {packet_or_packets, state} =
      get_and_update_in(state, [:muxer], fn muxer ->
        case stream.category do
          :video ->
            is_keyframe? = Map.get(buffer.metadata, :is_keyframe?, false)
            send_pcr? = Map.get(stream, :pcr?, false)

            TS.Muxer.mux_sample(muxer, stream.pid, buffer.payload, buffer.pts,
              dts: buffer.dts,
              sync?: is_keyframe?,
              send_pcr?: send_pcr?
            )

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

    # Validate all muxed packets before converting to buffers
    packets = List.wrap(packet_or_packets)

    validate_packets!(
      packets,
      state.valid_pids,
      "#{stream.category} stream (PID #{stream.pid})"
    )

    buffers =
      packets
      |> Enum.map(fn x ->
        %Membrane.Buffer{
          payload: marshal_payload(x),
          pts: buffer.pts,
          dts: buffer.dts,
          metadata: extract_metadata(x)
        }
      end)

    {pat_pmt_actions ++ [buffer: {:output, buffers}], state}
  end

  defp handle_queue_item(_, state), do: {[], state}

  # We have to stream_type information, we'll wait for the stream_format event.
  defp handle_stream(_pad, nil, _ctx, state), do: {[], state}

  # The stream has already been added.
  defp handle_stream(pad, _stream_format, _ctx, state)
       when is_map_key(state.pad_to_stream, pad),
       do: {[], state}

  defp handle_stream(pad, stream_type, ctx, state) do
    stream_category = TS.PMT.get_stream_category(stream_type)
    wait_on_buffers? = ctx.pads[pad].options[:wait_on_buffers?]
    pcr? = ctx.pads[pad].options[:pcr?]

    {pid, state} =
      get_and_update_in(state, [:muxer], fn muxer ->
        program_info =
          case stream_type do
            :SCTE_35_SPLICE -> [%{tag: 5, data: "CUEI"}]
            _ -> []
          end

        pid = ctx.pads[pad].options[:pid]

        stream_opts =
          List.flatten([
            [program_info: program_info],
            if(pid != nil, do: [pid: pid], else: []),
            if(pcr?, do: [pcr?: true], else: [])
          ])

        TS.Muxer.add_elementary_stream(muxer, stream_type, stream_opts)
      end)

    Membrane.Logger.info(
      "Binding #{inspect(pad)} to PID #{pid} (0x#{Integer.to_string(pid, 16)}) " <>
        "for #{inspect(stream_type)}#{if pcr?, do: ", PCR", else: ""}"
    )

    state =
      state
      |> put_in([:pad_to_stream, pad], %{
        pid: pid,
        type: stream_type,
        category: stream_category,
        wait_on_buffers?: wait_on_buffers?,
        pcr?: pcr?
      })
      |> update_in(
        [:queue],
        &TimestampQueue.register_pad(&1, pad, wait_on_buffers?: wait_on_buffers?)
      )
      |> update_in([:valid_pids], &MapSet.put(&1, pid))

    if ctx.playback == :playing and state.last_pat_pmt_dts != nil and state.last_dts != nil do
      # Each time a pad is added at runtime, update the PMT with current timing
      {actions, state} = mux_pat_pmt(state, state.last_dts)
      {actions, %{state | last_pat_pmt_dts: state.last_dts}}
    else
      # PAT/PMT will be sent with first media buffer
      {[], state}
    end
  end

  defp maybe_end_of_stream(ctx, state) do
    if TimestampQueue.pads(state.queue) |> MapSet.size() == 0 and
         not ctx.pads.output.end_of_stream? do
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
      pid_class: packet.pid_class || calculate_pid_class(packet.pid)
    }
  end

  # Calculate pid_class based on PID value (same logic as mpeg_ts library parser)
  # This is needed because the mpeg_ts muxer doesn't set pid_class when creating packets
  defp calculate_pid_class(0x0000), do: :pat
  defp calculate_pid_class(pid) when pid in 0x0020..0x1FFA or pid in 0x1FFC..0x1FFE, do: :psi
  defp calculate_pid_class(0x1FFF), do: :null_packet
  defp calculate_pid_class(_), do: :unsupported

  defp mux_pat_pmt(state, dts) do
    {pat, state} = get_and_update_in(state, [:muxer], &TS.Muxer.mux_pat(&1))
    {pmt, state} = get_and_update_in(state, [:muxer], &TS.Muxer.mux_pmt(&1))

    # Validate PAT/PMT packets
    validate_packets!([pat, pmt], state.valid_pids, "PAT/PMT")

    buffers =
      Enum.map([pat, pmt], fn x ->
        %Membrane.Buffer{
          payload: marshal_payload(x),
          pts: dts,
          dts: dts,
          metadata: extract_metadata(x)
        }
      end)

    {[buffer: {:output, buffers}], state}
  end

  # Validates that all packets have PIDs that were registered in the PMT.
  # Raises CorruptionError if any invalid PIDs are found.
  defp validate_packets!(packets, valid_pids, context) do
    invalid_packets =
      packets
      |> List.wrap()
      |> Enum.reject(fn packet ->
        MapSet.member?(valid_pids, packet.pid)
      end)

    if invalid_packets != [] do
      invalid_pids = Enum.map(invalid_packets, & &1.pid) |> Enum.uniq()

      Membrane.Logger.error("""
      MPEG-TS Corruption detected in #{context}!
      Invalid PIDs: #{inspect(invalid_pids)}
      Valid PIDs: #{inspect(MapSet.to_list(valid_pids))}
      """)

      raise CorruptionError,
        invalid_pids: invalid_pids,
        valid_pids: MapSet.to_list(valid_pids),
        packet_count: length(invalid_packets)
    end
  end
end
