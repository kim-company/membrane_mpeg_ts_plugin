defmodule Membrane.MPEG.TS.Muxer do
  @moduledoc """
  Experimental MPEG-TS Muxer. Supports 1 program with AAC and H264 in it only for now.

  Inputs must be attached before the element enters the playing state. Audio&Video
  are going to be interleaved by their timing.

  Each buffer is going to end in its own PES packet, hence NALu units must be grouped
  accordingly, as well as ADTS AAC frames.

  Use `profile:` for well-known stream_type+descriptor combinations (e.g. `:opus_mpeg_ts`).
  Custom payloads can be muxed by supplying `stream_type:` and `descriptors:`.

  ## Timestamp sanitization for non-AV streams

  Non-audio/video streams (e.g. subtitles, data, cues) may produce out-of-order
  timestamps — for instance, a speech-to-text editor emitting a corrected sentence
  whose start time precedes an already-emitted sentence's end time.

  Rather than crashing (as `TimestampQueue` requires monotonic per-pad timestamps),
  the muxer applies best-effort sanitization for these streams:

    * **Clamp**: when `buffer.pts < last_pts` but the buffer still has remaining
      duration (`metadata.to > last_pts`), `pts` is clamped to `last_pts`.
    * **Drop**: when the buffer is entirely in the past (`metadata.to <= last_pts`),
      or when `metadata.to` is absent and `buffer.pts < last_pts`, the buffer is
      dropped.

  Both cases emit a warning log. Audio and video pads are never sanitized — an
  out-of-order buffer on those pads indicates a serious upstream bug and will
  crash as before.
  """

  use Membrane.Filter
  alias MPEG.TS
  alias Membrane.TimestampQueue
  alias Membrane.MPEG.TS.Muxer.CorruptionError

  require Membrane.Logger

  @queue_buffer Membrane.Time.milliseconds(200)

  # PAT/PMT insertion interval per ISO/IEC 13818-1 spec (max 500ms)
  @pat_pmt_interval Membrane.Time.milliseconds(100)

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
      profile: [
        spec: atom() | nil,
        default: nil,
        description: "Well-known stream profile (e.g. :opus_mpeg_ts, :scte35)."
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
      descriptors: [
        spec: list(),
        default: [],
        description: "List of ES descriptors to add to the PMT for this stream."
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
      valid_pids: MapSet.new([@pat_pid, @cat_pid, @pmt_pid]),
      stream_format_sent?: false
    }

    {[], state}
  end

  @impl true
  def handle_stream_format(
        pad,
        %Membrane.RemoteStream{
          content_format: %Membrane.MPEG.TS.StreamFormat{
            stream_type: stream_type,
            descriptors: descriptors
          }
        } = format,
        ctx,
        state
      ) do
    handle_stream(pad, stream_type, ctx, state, descriptors, format)
  end

  def handle_stream_format(pad, %Membrane.H264{} = format, ctx, state) do
    handle_stream(pad, :H264_AVC, ctx, state, [], format)
  end

  def handle_stream_format(pad, format, _ctx, state) when is_map_key(state.pad_to_stream, pad) do
    state =
      update_in(state, [:pad_to_stream, pad], fn stream ->
        if Map.get(stream, :upstream_format) == nil do
          Map.put(stream, :upstream_format, format)
        else
          stream
        end
      end)

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
    # Stream format emission is deferred to the first buffer via
    # maybe_emit_stream_format/3 so that upstream_format fields on
    # elementary streams are populated (they arrive in handle_stream_format
    # which may run after handle_playing).
    {[], state}
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
    handle_stream(pad, stream_type, ctx, state, [], nil)
  end

  @impl true
  def handle_buffer(pad, buffer, ctx, state) do
    case maybe_sanitize_ts(pad, buffer, state) do
      {:ok, buffer, state} ->
        state.queue
        |> TimestampQueue.push_buffer_and_pop_available_items(pad, buffer)
        |> handle_queue_output(ctx, state)
        |> unblock_awaiting_pads()

      {:drop, state} ->
        {[], state}
    end
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

  # Sanitize timestamps for non-AV pads. Audio/video pads pass through unchanged
  # (out-of-order on those is a serious upstream bug). For other categories,
  # out-of-order buffers are clamped or dropped to prevent TimestampQueue crashes.
  defp maybe_sanitize_ts(pad, %Membrane.Buffer{} = buffer, state) do
    stream = Map.get(state.pad_to_stream, pad)

    if stream == nil or stream.category in [:audio, :video] do
      # AV pads or unknown pads: pass through, update last_pts
      state = maybe_update_last_pts(state, pad, buffer.pts)
      {:ok, buffer, state}
    else
      last_pts = stream.last_pts

      if last_pts == nil or buffer.pts == nil or buffer.pts >= last_pts do
        # First buffer on this pad or in-order: pass through
        state = maybe_update_last_pts(state, pad, buffer.pts)
        {:ok, buffer, state}
      else
        # Out-of-order: check if buffer has remaining duration via metadata.to
        handle_out_of_order_buffer(buffer, stream, last_pts, pad, state)
      end
    end
  end

  defp handle_out_of_order_buffer(%Membrane.Buffer{} = buffer, stream, last_pts, _pad, state) do
    buffer_to = get_in(buffer.metadata, [:to])
    delta_ms = div(last_pts - buffer.pts, 1_000_000)

    if is_integer(buffer_to) and buffer_to > last_pts do
      # Buffer partially in the past — clamp pts to last_pts
      Membrane.Logger.warning(
        "Clamped out-of-order buffer on PID #{stream.pid} (#{inspect(stream.type)}): " <>
          "pts #{buffer.pts} < last_pts #{last_pts}, delta #{delta_ms}ms"
      )

      clamped = %Membrane.Buffer{buffer | pts: last_pts}
      {:ok, clamped, state}
    else
      # Buffer entirely in the past (to <= last_pts) or no duration info — drop
      reason =
        if is_integer(buffer_to),
          do: "to #{buffer_to} <= last_pts #{last_pts}",
          else: "no duration info"

      Membrane.Logger.warning(
        "Dropped out-of-order buffer on PID #{stream.pid} (#{inspect(stream.type)}): " <>
          "pts #{buffer.pts} < last_pts #{last_pts}, delta #{delta_ms}ms (#{reason})"
      )

      {:drop, state}
    end
  end

  defp maybe_update_last_pts(state, pad, pts) when is_integer(pts) do
    put_in(state, [:pad_to_stream, pad, :last_pts], pts)
  end

  defp maybe_update_last_pts(state, _pad, _pts), do: state

  defp handle_queue_output({suggested_actions, items, queue}, ctx, state) do
    state = %{state | queue: queue}
    {actions, state} = Enum.flat_map_reduce(items, state, &handle_queue_item/2)

    output_actions = suggested_actions ++ actions ++ maybe_end_of_stream(ctx, state)
    {output_actions, state} = maybe_emit_stream_format(output_actions, items, state)

    {output_actions, state}
  end

  defp maybe_emit_stream_format(actions, items, state)
       when state.stream_format_sent? or items == [] do
    {actions, state}
  end

  defp maybe_emit_stream_format(actions, _items, state) do
    stream_format_action =
      {:stream_format,
       {:output, %Membrane.RemoteStream{content_format: build_output_stream_format(state)}}}

    {[stream_format_action | actions], %{state | stream_format_sent?: true}}
  end

  # Match H264.Parser metadata
  defp best_effort_is_keyframe?(%Membrane.Buffer{metadata: %{h264: %{key_frame?: flag}}}),
    do: flag

  # Match NALU.ParserBin metadata
  defp best_effort_is_keyframe?(%Membrane.Buffer{metadata: %{is_keyframe?: flag}}), do: flag
  defp best_effort_is_keyframe?(_), do: false

  defp handle_queue_item({pad, {:buffer, %Membrane.Buffer{} = buffer}}, state) do
    stream = get_in(state, [:pad_to_stream, pad])
    payload = maybe_packetize(buffer.payload, stream)
    buffer = %Membrane.Buffer{buffer | payload: payload}

    current_dts = extract_dts(stream, buffer)
    state = if current_dts != nil, do: %{state | last_dts: current_dts}, else: state

    {pat_pmt_actions, state} = maybe_send_pat_pmt(stream, buffer, current_dts, state)

    {packet_or_packets, state} =
      get_and_update_in(state, [:muxer], fn muxer ->
        mux_stream(muxer, stream, buffer)
      end)

    packets = List.wrap(packet_or_packets)

    validate_packets!(
      packets,
      state.valid_pids,
      "#{stream.category} stream (PID #{stream.pid})"
    )

    buffers = packets_to_buffers(packets, buffer)
    {pat_pmt_actions ++ [buffer: {:output, buffers}], state}
  end

  defp handle_queue_item(_, state), do: {[], state}

  defp extract_dts(stream, buffer) do
    case stream.category do
      :video -> buffer.dts || buffer.pts
      :audio -> buffer.pts
      _ -> buffer.dts || buffer.pts
    end
  end

  defp maybe_send_pat_pmt(stream, buffer, current_dts, state) do
    should_send? =
      current_dts != nil and
        (state.last_pat_pmt_dts == nil or
           current_dts - state.last_pat_pmt_dts >= @pat_pmt_interval or
           (stream.category == :video and best_effort_is_keyframe?(buffer)))

    if should_send? do
      {actions, state} = mux_pat_pmt(state, current_dts)
      {actions, %{state | last_pat_pmt_dts: current_dts}}
    else
      {[], state}
    end
  end

  defp packets_to_buffers(packets, buffer) do
    Enum.map(packets, fn x ->
      %Membrane.Buffer{
        payload: marshal_payload(x),
        pts: buffer.pts,
        dts: buffer.dts,
        metadata: extract_metadata(x)
      }
    end)
  end

  defp mux_stream(muxer, %{category: :video} = stream, buffer) do
    is_keyframe? = best_effort_is_keyframe?(buffer)
    send_pcr? = Map.get(stream, :pcr?, false)

    TS.Muxer.mux_sample(muxer, stream.pid, buffer.payload, buffer.pts,
      dts: buffer.dts,
      sync?: is_keyframe?,
      send_pcr?: send_pcr?
    )
  end

  defp mux_stream(muxer, %{category: :audio} = stream, buffer) do
    TS.Muxer.mux_sample(muxer, stream.pid, buffer.payload, buffer.pts, sync?: true)
  end

  defp mux_stream(muxer, stream, buffer) do
    stream_info = %{stream_type: stream.type, descriptors: stream.descriptors}

    if TS.PMT.pes_stream?(stream_info) do
      TS.Muxer.mux_sample(muxer, stream.pid, buffer.payload, buffer.pts, sync?: true)
    else
      mux_psi_stream(muxer, stream, buffer)
    end
  end

  defp mux_psi_stream(muxer, stream, buffer) do
    psi = get_in(buffer, [Access.key!(:metadata), :psi])

    if psi != nil do
      TS.Muxer.mux_psi(muxer, stream.pid, psi)
    else
      Membrane.Logger.warning(
        "Could not mux non-PES stream #{inspect(stream.type)} on PID #{stream.pid}: " <>
          "expected buffer.metadata.psi"
      )

      {[], muxer}
    end
  end

  # The stream has already been added.
  defp handle_stream(pad, _stream_format, _ctx, state, _format_descriptors, upstream_format)
       when is_map_key(state.pad_to_stream, pad) do
    state =
      if is_nil(upstream_format) do
        state
      else
        maybe_set_upstream_format(state, pad, upstream_format)
      end

    {[], state}
  end

  defp handle_stream(pad, stream_type, ctx, state, format_descriptors, upstream_format) do
    profile_data = resolve_profile(ctx.pads[pad].options[:profile])
    stream_type = stream_type || profile_data[:stream_type]

    if stream_type == nil do
      {[], state}
    else
      add_elementary_stream(
        pad,
        stream_type,
        ctx,
        state,
        format_descriptors,
        upstream_format,
        profile_data
      )
    end
  end

  defp add_elementary_stream(
         pad,
         stream_type,
         ctx,
         state,
         format_descriptors,
         upstream_format,
         profile_data
       ) do
    descriptors =
      merge_descriptors(
        profile_data[:descriptors],
        merge_descriptors(format_descriptors, ctx.pads[pad].options[:descriptors])
      )

    stream_category = profile_data[:stream_category] || TS.PMT.get_stream_category(stream_type)
    wait_on_buffers? = ctx.pads[pad].options[:wait_on_buffers?]
    pcr? = ctx.pads[pad].options[:pcr?]

    {pid, state} =
      get_and_update_in(state, [:muxer], fn muxer ->
        stream_opts =
          build_stream_opts(stream_type, ctx.pads[pad].options[:pid], pcr?, descriptors)

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
        pcr?: pcr?,
        packetizer: profile_data[:packetizer],
        descriptors: descriptors,
        upstream_format: upstream_format,
        last_pts: nil
      })
      |> update_in(
        [:queue],
        &TimestampQueue.register_pad(&1, pad, wait_on_buffers?: wait_on_buffers?)
      )
      |> update_in([:valid_pids], &MapSet.put(&1, pid))

    maybe_emit_runtime_pat_pmt(ctx, state)
  end

  defp build_stream_opts(stream_type, pid, pcr?, descriptors) do
    program_info =
      case stream_type do
        :SCTE_35_SPLICE -> [%{tag: 5, data: "CUEI"}]
        _ -> []
      end

    List.flatten([
      [program_info: program_info],
      if(pid != nil, do: [pid: pid], else: []),
      if(pcr?, do: [pcr?: true], else: []),
      if(descriptors != [], do: [descriptors: descriptors], else: [])
    ])
  end

  defp maybe_emit_runtime_pat_pmt(ctx, state) do
    if ctx.playback == :playing and state.last_pat_pmt_dts != nil and state.last_dts != nil do
      {actions, state} = mux_pat_pmt(state, state.last_dts)
      {actions, %{state | last_pat_pmt_dts: state.last_dts}}
    else
      {[], state}
    end
  end

  defp maybe_set_upstream_format(state, pad, upstream_format) do
    update_in(state, [:pad_to_stream, pad], fn stream ->
      if Map.get(stream, :upstream_format) == nil do
        Map.put(stream, :upstream_format, upstream_format)
      else
        stream
      end
    end)
  end

  defp build_output_stream_format(state) do
    elementary_streams =
      state.pad_to_stream
      |> Map.values()
      |> Enum.sort_by(& &1.pid)
      |> Enum.map(fn stream ->
        %{
          pid: stream.pid,
          stream_type: stream.type,
          descriptors: stream.descriptors,
          upstream_format: Map.get(stream, :upstream_format)
        }
      end)

    %Membrane.MPEG.TS.StreamFormat{elementary_streams: elementary_streams}
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

  defp maybe_packetize(payload, %{packetizer: nil}), do: payload

  defp maybe_packetize(payload, %{packetizer: packetizer}) do
    packetizer.packetize(payload)
  end

  defp resolve_profile(nil), do: %{}

  defp resolve_profile(profile) do
    case Membrane.MPEG.TS.Profiles.fetch(profile) do
      {:ok, data} -> data
      :error -> raise RuntimeError, "Unknown MPEG-TS profile: #{inspect(profile)}"
    end
  end

  defp merge_descriptors(profile_descriptors, descriptors) do
    profile_descriptors = profile_descriptors || []
    descriptors = descriptors || []
    profile_descriptors ++ descriptors
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
