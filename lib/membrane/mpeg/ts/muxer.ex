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

  @pmt_pid 4096
  @pat_pid 0x0
  @pid_counter_max 2 ** 4
  @pes_packet_size_max 2 ** 16

  # We'll start using PIDs from this offset.
  @stream_pid_offset 256
  @stream_id_audio_offset 0xC0
  @stream_id_video_offset 0xE0

  @ts_packet_size 188
  @ts_header_size 4
  @ts_adaptation_header_size 2
  @ts_pcr_size 6

  def_input_pad(:input,
    accepted_format: %Membrane.RemoteStream{},
    availability: :on_request,
    options: [
      stream_type: [
        spec: :AAC | :H264 | :H265,
        description: """
        Each input is going to become a stream in the PMT with this assigned type.
        """
      ]
    ]
  )

  def_output_pad(:output,
    accepted_format: Membrane.RemoteStream
  )

  @impl true
  def handle_init(_ctx, _opts) do
    pat = %{
      1 => @pmt_pid
    }

    pmt = %TS.PMT{
      streams: %{},
      program_info: [],
      # The packet identifier that contains the program clock reference used to
      # improve the random access accuracy of the stream's timing that is derived from
      # the program timestamp. If this is unused. then it is set to 0x1FFF (all bits
      # on).
      pcr_pid: nil
    }

    {:ok, {:interval, timer}} = :timer.send_interval(500, :pcr, self())

    {[],
     %{
       pat: pat,
       pmt: pmt,
       pad_to_pid: %{},
       pid_to_queue: %{},
       pid_to_stream_id: %{},
       pid_to_counter: %{@pmt_pid => 0, @pat_pid => 0},
       pcr_timer: timer,
       pat_written?: false
     }}
  end

  @impl true
  def handle_end_of_stream({Membrane.Pad, :input, ref}, ctx, state) do
    end_of_stream? =
      ctx.pads
      |> Enum.map(fn {_ref, pad} -> pad end)
      |> Enum.filter(fn pad -> pad.direction == :input end)
      |> Enum.map(fn pad -> pad.end_of_stream? or pad == ref end)
      |> Enum.all?()

    if end_of_stream? do
      if state.pcr_timer, do: Process.cancel_timer(state.pcr_timer)
      mux_and_forward_end_of_stream(state)
    else
      {[], state}
    end
  end

  @impl true
  def handle_info(:pcr, %{playback: :playing}, state) do
    {pcr, state} = pcr_buffer(state)
    {[buffer: {:output, pcr}], state}
  end

  def handle_info(:pcr, _ctx, state) do
    {[], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
    {[stream_format: {:output, %Membrane.RemoteStream{}}], state}
  end

  @impl true
  def handle_stream_format(_pad, _format, _ctx, state) do
    {[], state}
  end

  @impl true
  def handle_buffer({Membrane.Pad, :input, ref}, buffer, _ctx, state) do
    pid = get_in(state, [:pad_to_pid, ref])

    state
    |> update_in([:pid_to_queue, pid], fn q -> :queue.in(buffer, q) end)
    |> mux_and_forward_oldest([])
  end

  defp mux_and_forward_end_of_stream(state) do
    {buffers, state} =
      state.pid_to_queue
      |> Enum.flat_map(fn {pid, queue} ->
        queue
        |> :queue.to_list()
        |> Enum.map(fn buffer -> {pid, buffer} end)
      end)
      |> Enum.sort(fn {_, left}, {_, right} ->
        Membrane.Buffer.get_dts_or_pts(left) < Membrane.Buffer.get_dts_or_pts(right)
      end)
      |> Enum.flat_map_reduce(state, fn {pid, buffer}, state ->
        mux_and_forward(pid, buffer, state)
      end)

    state =
      update_in(state, [:pid_to_queue], fn m ->
        m
        |> Enum.map(fn {pid, _queue} -> {pid, :queue.new()} end)
        |> Map.new()
      end)

    {[buffer: {:output, buffers}, end_of_stream: :output], state}
  end

  defp mux_and_forward_oldest(state, acc) do
    any_empty? =
      state.pid_to_queue
      |> Enum.map(fn {_, q} -> :queue.is_empty(q) end)
      |> Enum.any?()

    if any_empty? do
      {[buffer: {:output, acc}], state}
    else
      # Find the queue containing the oldest item.
      {next_pid, _} =
        state.pid_to_queue
        |> Enum.map(fn {pid, q} ->
          {:value, x} = :queue.peek(q)
          {pid, x.pts}
        end)
        |> Enum.sort(fn {_, left}, {_, right} -> left < right end)
        |> List.first()

      {{:value, buffer}, state} =
        get_and_update_in(state, [:pid_to_queue, next_pid], fn q -> :queue.out(q) end)

      {buffers, state} = mux_and_forward(next_pid, buffer, state)
      mux_and_forward_oldest(state, acc ++ buffers)
    end
  end

  defp mux_and_forward(pid, buffer, state) do
    is_keyframe? = Map.get(buffer.metadata, :is_keyframe?, false)
    {pes, state} = pes_buffers(pid, buffer, state)

    {buffers, state} =
      if is_keyframe? or not state.pat_written? do
        {pat, state} = pat_buffer(state)
        {pmt, state} = pmt_buffer(state)

        {List.flatten([pat, pmt, pes]), put_in(state, [:pat_written?], true)}
      else
        {pes, state}
      end

    {buffers, state}
  end

  @impl true
  def handle_pad_added({Membrane.Pad, :input, id}, ctx, state) do
    stream_type = ctx.pad_options[:stream_type]

    if stream_type not in [:AAC, :H264, :H265] do
      raise RuntimeError, "Linking stream type #{stream_type} is not supported"
    end

    pid = @stream_pid_offset + Enum.count(state.pmt.streams)

    stream_id_count =
      state.pmt.streams
      |> Map.values()
      |> Enum.group_by(fn x -> x.stream_type end)
      |> Map.get(stream_type, [])
      |> Enum.count()

    stream_id_offset =
      case stream_type do
        :AAC -> @stream_id_audio_offset
        video when video in [:H264, :H265] -> @stream_id_video_offset
      end

    stream_id = stream_id_offset + stream_id_count

    state =
      state
      |> put_in([:pmt, Access.key!(:streams), pid], %{
        stream_type: stream_type,
        stream_type_id: stream_type_id(stream_type)
      })
      |> update_in([:pmt, Access.key!(:pcr_pid)], fn old ->
        # We're writing the PCR in the first video stream connected.
        if is_nil(old) and stream_type in [:H264, :H265] do
          pid
        else
          old
        end
      end)
      |> put_in([:pad_to_pid, id], pid)
      |> put_in([:pid_to_stream_id, pid], stream_id)
      |> put_in([:pid_to_counter, pid], 0)
      |> put_in([:pid_to_queue, pid], :queue.new())

    {[], state}
  end

  defp stream_type_id(:AAC), do: 0x0F
  defp stream_type_id(:H264), do: 0x1B
  defp stream_type_id(:H265), do: 0x24

  def pcr_buffer(state) do
    <<>>
    |> marshal_ts(state.pmt.pcr_pid, state, pcr: 1)
    |> then(fn {packets, state} -> {packet_to_buffer(packets), state} end)
  end

  def pmt_buffer(state) do
    state.pmt
    |> marshal_pmt()
    |> marshal_ts(@pmt_pid, state)
    |> then(fn {packets, state} -> {packet_to_buffer(packets), state} end)
  end

  def pat_buffer(state) do
    state.pat
    |> marshal_pat()
    |> marshal_ts(@pat_pid, state)
    |> then(fn {packets, state} -> {packet_to_buffer(packets), state} end)
  end

  def pes_buffers(pid, buffer, state) do
    stream_id = get_in(state, [:pid_to_stream_id, pid])

    is_keyframe? = Map.get(buffer.metadata, :is_keyframe?, false)
    is_audio? = stream_id == @stream_id_audio_offset

    buffer
    |> update_in([Access.key!(:dts)], fn dts ->
      unless is_audio?, do: dts
    end)
    |> marshal_pes(stream_id)
    |> marshal_ts(pid, state, rai: if(is_keyframe? or is_audio?, do: 1, else: 0))
    |> then(fn {packets, state} -> {packet_to_buffer(packets), state} end)
  end

  def marshal_ts(payload, pid, state, opts \\ []) do
    opts =
      Keyword.validate!(opts,
        discontinuity: 0,
        rai: 0,
        pcr: 0
      )

    do_marshal_ts(payload, pid, state, opts)
  end

  def do_marshal_ts(<<>>, pid, state, opts) do
    pad_size = @ts_packet_size - @ts_header_size - @ts_adaptation_header_size
    pad_size = if(opts[:pcr] == 1, do: pad_size - @ts_pcr_size, else: pad_size)
    adaptation_field = marshal_adaptation_field(pad_size, opts)

    # Counter is only updated when a payload is provided.
    counter =
      state
      |> get_in([:pid_to_counter, pid])
      |> rem(@pid_counter_max)

    packet = <<
      0x47::8,
      # TEI
      0::1,
      # PUSI
      0::1,
      # Priority
      0::1,
      pid::13,
      # Scrambling
      0::2,
      # Adaptation Field only
      0b10::2,
      counter::4,
      adaptation_field::binary
    >>

    {[packet], state}
  end

  def do_marshal_ts(payload, pid, state, opts) do
    do_marshal_ts(payload, pid, state, opts, [])
  end

  def do_marshal_ts(<<>>, _pid, state, _opts, acc), do: {Enum.reverse(acc), state}

  def do_marshal_ts(payload, pid, state, opts, []) do
    # First packet of the series. We need an adaptation field as this might be a
    # random access unit.

    min_size = @ts_packet_size - @ts_header_size - @ts_adaptation_header_size

    {payload, pad_size, rest} =
      if byte_size(payload) < min_size do
        # This packet is small and might require padding.
        pad_size = min_size - byte_size(payload)
        {payload, pad_size, <<>>}
      else
        # Fits into one or more packets.
        <<payload::binary-size(min_size)-unit(8), rest::binary>> = payload
        {payload, 0, rest}
      end

    adaptation_field = marshal_adaptation_field(pad_size, opts)
    {counter, state} = counter(pid, state)

    packet =
      <<
        0x47::8,
        # TEI
        0::1,
        # PUSI enabled
        1::1,
        # Priority
        0::1,
        pid::13,
        # TSC
        0::2,
        # Adaptation and Payload
        0b11::2,
        # Continuity Counter
        counter::4,
        adaptation_field::binary,
        payload::binary
      >>

    do_marshal_ts(rest, pid, state, opts, [packet])
  end

  def do_marshal_ts(payload, pid, state, opts, acc) do
    {counter, state} = counter(pid, state)

    size = byte_size(payload)

    {payload, adaptation_field, rest} =
      cond do
        size > @ts_packet_size - @ts_header_size ->
          <<payload::binary-size(@ts_packet_size - @ts_header_size)-unit(8), rest::binary>> =
            payload

          {payload, <<>>, rest}

        size < @ts_packet_size - @ts_header_size - @ts_adaptation_header_size ->
          # The packet can be finished with a TS + adaptation and possibly padding.
          pad_size = @ts_packet_size - @ts_header_size - @ts_adaptation_header_size - size

          adaptation_field =
            marshal_adaptation_field(pad_size,
              # We override the options as those are useful for the first packet of the series only.
              pcr: 0,
              rai: 0,
              discontinuity: 0
            )

          {payload, adaptation_field, <<>>}

        true ->
          # This packet is 183 bytes, meaning it cannot hold an adaptation field but
          # cannot either fill and entire packet -- we split it in two.
          size = div(size, 2)
          <<payload::binary-size(size)-unit(8), rest::binary>> = payload
          pad_size = @ts_packet_size - @ts_header_size - @ts_adaptation_header_size - size

          adaptation_field =
            marshal_adaptation_field(pad_size,
              # We override the options as those are useful for the first packet of the series only.
              pcr: 0,
              rai: 0,
              discontinuity: 0
            )

          {payload, adaptation_field, rest}
      end

    adaptation =
      case adaptation_field do
        <<>> -> 0b01
        _ -> 0b11
      end

    packet =
      <<
        0x47::8,
        0::1,
        # PUSI disabled
        0::1,
        0::1,
        pid::13,
        0::2,
        adaptation::2,
        counter::4,
        adaptation_field::binary,
        payload::binary
      >>

    do_marshal_ts(rest, pid, state, opts, [packet | acc])
  end

  defp counter(pid, state) do
    get_and_update_in(state, [:pid_to_counter, pid], fn old ->
      {rem(old, @pid_counter_max), old + 1}
    end)
  end

  defp marshal_adaptation_field(pad_size, opts) do
    {pcr_flag, pcr} =
      if opts[:pcr] == 1 do
        pcr = marshal_pcr(pcr(:erlang.monotonic_time(:nanosecond)))
        {1, pcr}
      else
        {0, <<>>}
      end

    pad =
      if pad_size > 0 do
        [0xFF]
        |> List.duplicate(pad_size)
        |> :binary.list_to_bin()
      else
        <<>>
      end

    # TODO: handle discontinuity: what happens when the pipieline crashes but
    # the ffmpeg process is still alive? We should be able to recover using the
    # discontinuity flag.
    adaptation_field_no_length = <<
      # Discontinuity
      opts[:discontinuity]::1,
      # Random Access Indicator
      opts[:rai]::1,
      # Prio
      0::1,
      # PCR
      pcr_flag::1,
      0::4,
      pcr::binary,
      pad::binary
    >>

    adaptation_length = byte_size(adaptation_field_no_length)
    <<adaptation_length::8, adaptation_field_no_length::binary>>
  end

  defp marshal_pes(buffer, stream_id) do
    pts_dts_indicator =
      cond do
        not is_nil(buffer.pts) and not is_nil(buffer.dts) -> 0x3
        not is_nil(buffer.pts) -> 0x2
        true -> 0x0
      end

    optional_fields = marshal_pts_dts(pts_dts_indicator, buffer.pts, buffer.dts)

    optional_pes_header = <<
      # Marker bits
      0x02::2,
      # Scrambling control
      0::2,
      # Priority
      0::1,
      # Data alignment indicator
      1::1,
      # Copyright
      0::1,
      # Original or Copy
      1::1,
      pts_dts_indicator::2,
      # ESCR, ES rate, DSM trick mode, additional copy info, CRC, extension
      0::6,
      byte_size(optional_fields)::8,
      optional_fields::binary
    >>

    packet_length = byte_size(optional_pes_header) + byte_size(buffer.payload)

    # packet length of 0 means unbounded PES and its only valid for video streams.
    packet_length = if(stream_id == @stream_id_video_offset, do: 0, else: packet_length)

    if packet_length > @pes_packet_size_max do
      raise RuntimeError, "Attempted to generate a PES that exceeds max size"
    end

    <<
      1::24,
      stream_id::8,
      packet_length::16,
      optional_pes_header::binary,
      buffer.payload::binary
    >>
  end

  defp marshal_pts_dts(0x2, pts, nil), do: marshal_timestamp(0x2, pts)

  defp marshal_pts_dts(0x3, pts, dts) do
    <<marshal_timestamp(0x3, pts)::bitstring, marshal_timestamp(0x1, dts)::bitstring>>
  end

  defp marshal_timestamp(prefix, ts) when prefix in 0x1..0x3 do
    ts = div(ts * 90_000, 1_000_000_000)

    import Bitwise
    # Extract bits
    t32_30 = ts >>> 30 &&& 0x7
    t29_15 = ts >>> 15 &&& 0x7FFF
    t14_0 = ts &&& 0x7FFF

    <<prefix::4, t32_30::3, 1::1, t29_15::15, 1::1, t14_0::15, 1::1>>
  end

  def marshal_pat(pat) do
    payload =
      for {program_number, pid} <- pat, do: <<program_number::16, 0x07::3, pid::13>>, into: <<>>

    marshal_psi(payload, 0x0)
  end

  defp marshal_pmt(pmt) do
    header = <<0x07::3, pmt.pcr_pid::13, 0x0F::4, 0::2, 0::10>>

    # TODO: ffmpeg adds a stream info field for the audio specifying the 'und' language.
    # TODO: we can signal the presence of subtitles in the video stream: https://chatgpt.com/share/67f4ff05-2234-8004-9395-d1fe8b9cb992
    streams =
      for {pid, %{stream_type_id: stream_type}} <- pmt.streams,
          do: <<stream_type::8, 0x07::3, pid::13, 0x0F::4, 0::2, 0::10>>,
          into: <<>>

    payload = <<header::bitstring, streams::bitstring>>

    marshal_psi(payload, 0x02)
  end

  defp marshal_psi(payload, table_id) do
    section_long_header = <<
      # Supplemental identifier. The PAT uses this for the transport stream identifier and the PMT uses this for the Program number.
      1::16,
      0x03::2,
      0::5,
      1::1,
      0::8,
      0::8
    >>

    # +4 is for the CRC (32 bits)
    size = byte_size(section_long_header) + byte_size(payload) + 4

    section_header = <<
      table_id::8,
      # We only expect PAT or PMT tables, both have to set this flag to 1.
      1::1,
      0::1,
      0x03::2,
      0::2,
      # he number of bytes that follow, including long header, data, and CRC value. Must be <=1021 for PAT, CAT, and PMT, but can be 4093 for private sections and some others.
      size::10
    >>

    section = <<section_header::bitstring, section_long_header::bitstring, payload::bitstring>>

    crc = compute_crc(section)
    section = <<0::8, section::bitstring>>

    <<section::binary, crc::32>>
  end

  defp marshal_pcr({base, ext}) do
    <<base::size(33), 0::size(6), ext::size(9)>>
  end

  defp compute_crc(string) do
    # https://stackoverflow.com/questions/76233763/formation-of-crc-32-for-sdt-to-ts-file
    # width=32 poly=0x04c11db7 init=0xffffffff refin=false refout=false xorout=0x00000000 check=0x0376e6e7 residue=0x00000000 name="CRC-32/MPEG-2"
    CRC.calculate(
      string,
      %{
        width: 32,
        poly: 0x04C11DB7,
        init: 0xFFFFFFFF,
        refin: false,
        refout: false,
        xorout: 0x00000000
      }
    )
  end

  defp pcr(time_ns) do
    pcr_base = div(time_ns * 90_000, 1_000_000_000)
    pcr_ext = rem(div(time_ns * 27_000_000, 1_000_000_000), 300)

    {pcr_base, pcr_ext}
  end

  defp packet_to_buffer(packets) when is_list(packets) do
    Enum.map(packets, &packet_to_buffer/1)
  end

  defp packet_to_buffer(packet) do
    if byte_size(packet) != @ts_packet_size do
      raise RuntimeError,
            "Invalid packet produced (size=#{byte_size(packet)}): #{inspect(packet)}"
    end

    if not is_binary(packet) do
      raise RuntimeError,
            "Tried to output a non-binary TS payload: #{inspect(packet, limit: :infinity)}"
    end

    %Membrane.Buffer{payload: packet}
  end
end
