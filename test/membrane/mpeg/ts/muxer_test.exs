defmodule Membrane.MPEG.TS.MuxerTest do
  use ExUnit.Case, async: true
  import Membrane.Testing.Assertions
  import Membrane.ChildrenSpec
  alias Membrane.Testing
  require Membrane.Pad
  alias Membrane.MPEG.TS

  # Helper function to run tsanalyze and parse JSON output
  defp run_tsanalyze(file_path) do
    {output, 0} = System.cmd("tsanalyze", ["--json", file_path])
    JSON.decode!(output)
  end

  defp assert_ffprobe_streams(file_path, expected_codecs) do
    ffprobe = System.find_executable("ffprobe")
    if ffprobe == nil do
      IO.warn("ffprobe not available, skipping ffprobe validation")
      :ok
    else
      {output, 0} =
        System.cmd(ffprobe, [
          "-v",
          "error",
          "-show_entries",
          "stream=codec_type,codec_name",
          "-of",
          "json",
          file_path
        ])

      %{"streams" => streams} = JSON.decode!(output)

      codec_set =
        streams
        |> Enum.map(fn %{"codec_type" => type, "codec_name" => name} -> "#{type}:#{name}" end)
        |> MapSet.new()

      for codec <- expected_codecs do
        assert MapSet.member?(codec_set, codec),
               "ffprobe did not report expected stream #{codec}. Got: #{inspect(codec_set)}"
      end
    end
  end

  # Validate service/PID structure
  defp assert_service_structure(analysis, expected_pids) do
    pids = analysis["pids"] || []

    for {expected_pid, expected_codec} <- expected_pids do
      pid_info = Enum.find(pids, fn p -> p["id"] == expected_pid end)

      assert pid_info != nil,
             "Expected PID #{expected_pid} (0x#{Integer.to_string(expected_pid, 16)}) not found in stream"

      codec = pid_info["codec"] || pid_info["description"] || ""

      assert String.contains?(codec, expected_codec),
             "PID #{expected_pid}: expected codec containing '#{expected_codec}', got '#{codec}'"
    end
  end

  # Validate audio stream is present and valid
  defp assert_audio_stream_valid(analysis, expected_pid) do
    pids = analysis["pids"] || []
    audio_info = Enum.find(pids, fn p -> p["id"] == expected_pid end)

    assert audio_info != nil, "Audio PID #{expected_pid} not found"
    assert audio_info["audio"] == true, "PID #{expected_pid} is not marked as audio"

    packet_count = get_in(audio_info, ["packets", "total"]) || 0
    assert packet_count > 0, "Audio PID #{expected_pid} has no packets"
  end

  # Validate PCR is present and valid
  defp assert_pcr_valid(analysis, expected_pcr_pid) do
    services = analysis["services"] || []
    assert length(services) > 0, "No services found in stream"

    service = List.first(services)
    pcr_pid = service["pcr-pid"]

    assert pcr_pid == expected_pcr_pid,
           "Expected PCR PID #{expected_pcr_pid} (0x#{Integer.to_string(expected_pcr_pid, 16)}), got #{pcr_pid}"

    # Verify PCR packets exist
    pids = analysis["pids"] || []
    pcr_info = Enum.find(pids, fn p -> p["id"] == expected_pcr_pid end)

    if pcr_info != nil do
      pcr_count = get_in(pcr_info, ["packets", "pcr"]) || 0
      assert pcr_count > 0, "PCR PID #{expected_pcr_pid} has no PCR values"
    end
  end

  # Validate no transport errors
  defp assert_no_errors(analysis) do
    pids = analysis["pids"] || []

    for pid_info <- pids do
      pid = pid_info["id"]
      discontinuities = get_in(pid_info, ["packets", "discontinuities"]) || 0
      invalid_scrambling = get_in(pid_info, ["packets", "invalid-scrambling"]) || 0

      assert discontinuities == 0,
             "PID #{pid} (0x#{Integer.to_string(pid, 16)}) has #{discontinuities} discontinuities"

      assert invalid_scrambling == 0,
             "PID #{pid} (0x#{Integer.to_string(pid, 16)}) has #{invalid_scrambling} invalid scrambling errors"
    end
  end

  # Validate that ONLY the expected PIDs are present (excluding standard PSI/SI)
  defp assert_only_expected_pids(analysis, expected_pids) do
    pids = analysis["pids"] || []

    # Filter out standard PSI/SI PIDs (PAT, PMT, SDT, etc.)
    standard_pids = [0, 17, 4096]
    stream_pids = Enum.filter(pids, fn p -> not Enum.member?(standard_pids, p["id"]) end)

    actual_pid_ids = Enum.map(stream_pids, fn p -> p["id"] end) |> Enum.sort()
    expected_pid_ids = Map.keys(expected_pids) |> Enum.sort()

    assert actual_pid_ids == expected_pid_ids,
           """
           PID mismatch in stream:
           Expected PIDs: #{inspect(expected_pid_ids)}
           Actual PIDs:   #{inspect(actual_pid_ids)}

           Expected (hex): #{Enum.map(expected_pid_ids, &"0x#{Integer.to_string(&1, 16)}") |> Enum.join(", ")}
           Actual (hex):   #{Enum.map(actual_pid_ids, &"0x#{Integer.to_string(&1, 16)}") |> Enum.join(", ")}
           """
  end

  # Get detailed PID information for debugging
  defp get_pid_details(analysis, pid) do
    pids = analysis["pids"] || []
    pid_info = Enum.find(pids, fn p -> p["id"] == pid end)

    if pid_info do
      %{
        pid: pid,
        hex: "0x#{Integer.to_string(pid, 16)}",
        description: pid_info["description"] || "N/A",
        codec: pid_info["codec"] || "N/A",
        is_video: pid_info["video"] || false,
        is_audio: pid_info["audio"] || false,
        packet_count: get_in(pid_info, ["packets", "total"]) || 0,
        pcr_count: get_in(pid_info, ["packets", "pcr"]) || 0
      }
    else
      nil
    end
  end

  @tag :tmp_dir
  test "re-muxes avsync", %{tmp_dir: tmp_dir} do
    output_path = Path.join(tmp_dir, "output.ts")

    spec = [
      child(:source, %Membrane.File.Source{location: "test/data/avsync.ts"})
      |> child(:demuxer, TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> child({:h264, :parser}, %Membrane.NALU.ParserBin{alignment: :aud, assume_aligned: true})
      |> via_in(:input, options: [stream_type: :H264_AVC, pcr?: true])
      |> get_child(:muxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x101])
      |> child({:aac, :parser}, %Membrane.AAC.Parser{})
      |> via_in(:input, options: [stream_type: :AAC_ADTS])
      |> get_child(:muxer),
      child(:muxer, Membrane.MPEG.TS.Muxer)
      |> child(:sink, %Membrane.File.Sink{location: output_path})
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pid, :sink)
    :ok = Membrane.Pipeline.terminate(pid)

    # Validate the output with tsanalyze - comprehensive checks
    analysis = run_tsanalyze(output_path)

    # Define exactly what PIDs we expect in the output
    expected_pids = %{
      0x100 => "AVC",  # Video on PID 0x100 (256)
      0x101 => "AAC"   # Audio on PID 0x101 (257)
    }

    # Verify ONLY these PIDs are present (no unexpected PIDs)
    assert_only_expected_pids(analysis, expected_pids)

    # Verify service structure matches expected codecs
    assert_service_structure(analysis, expected_pids)

    # Detailed validation for each stream
    # Video stream (PID 0x100)
    video_details = get_pid_details(analysis, 0x100)
    assert video_details != nil, "Video PID 0x100 not found"
    assert video_details.is_video == true, "PID 0x100 is not marked as video: #{inspect(video_details)}"
    assert video_details.packet_count > 0, "Video PID 0x100 has no packets: #{inspect(video_details)}"

    # Audio stream (PID 0x101)
    audio_details = get_pid_details(analysis, 0x101)
    assert audio_details != nil, "Audio PID 0x101 not found"
    assert audio_details.is_audio == true, "PID 0x101 is not marked as audio: #{inspect(audio_details)}"
    assert audio_details.packet_count > 0, "Audio PID 0x101 has no packets: #{inspect(audio_details)}"

    # Verify PCR is present on video stream
    assert_pcr_valid(analysis, 0x100)
    assert video_details.pcr_count > 0,
           "Video PID 0x100 should have PCR packets: #{inspect(video_details)}"

    # Verify audio stream is valid
    assert_audio_stream_valid(analysis, 0x101)

    # Verify no transport errors
    assert_no_errors(analysis)
  end

  @tag :tmp_dir
  test "muxes and demuxes JSON private data", %{tmp_dir: tmp_dir} do
    output_path = Path.join(tmp_dir, "json.ts")

    json_1 = ~s({"text":"hello","from":0,"to":1000})
    json_2 = ~s({"text":"world","from":1000,"to":2000})

    buffers = [
      %Membrane.Buffer{payload: json_1, pts: Membrane.Time.seconds(5)},
      %Membrane.Buffer{payload: json_2, pts: Membrane.Time.seconds(6)}
    ]

    spec = [
      child(:source, %Membrane.File.Source{location: "test/data/avsync.ts"})
      |> child(:demuxer, TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> child({:h264, :parser}, %Membrane.NALU.ParserBin{alignment: :aud, assume_aligned: true})
      |> via_in(:input, options: [stream_type: :H264_AVC, pcr?: true])
      |> get_child(:muxer),
      child(:json_source, %Membrane.Testing.Source{output: buffers})
      |> via_in(:input,
        options: [
          stream_type: :PES_PRIVATE_DATA,
          descriptors: [%{tag: 0x05, data: "JSON"}],
          wait_on_buffers?: false
        ]
      )
      |> get_child(:muxer),
      child(:muxer, Membrane.MPEG.TS.Muxer)
      |> child(:sink, %Membrane.File.Sink{location: output_path})
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pid, :sink)
    :ok = Membrane.Pipeline.terminate(pid)

    demux_spec = [
      child(:source, %Membrane.File.Source{location: output_path})
      |> child(:demuxer, Membrane.MPEG.TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [stream_type: :PES_PRIVATE_DATA])
      |> child(:sink, %Membrane.Testing.Sink{})
    ]

    demux_pid = Testing.Pipeline.start_link_supervised!(spec: demux_spec)
    assert_pipeline_notified(demux_pid, :demuxer, {:pmt, %MPEG.TS.PMT{}})

    assert_sink_stream_format(demux_pid, :sink, %Membrane.RemoteStream{
      content_format: %Membrane.MPEG.TS.StreamFormat{
        stream_type: :PES_PRIVATE_DATA,
        descriptors: [%{tag: 0x05, data: "JSON"}]
      }
    })

    assert_sink_buffer(demux_pid, :sink, %Membrane.Buffer{payload: ^json_1})
    assert_sink_buffer(demux_pid, :sink, %Membrane.Buffer{payload: ^json_2})
    assert_end_of_stream(demux_pid, :sink, :input)
    :ok = Testing.Pipeline.terminate(demux_pid)
  end

  @tag :tmp_dir
  test "muxes, demuxes and parses audio+video+JSON", %{tmp_dir: tmp_dir} do
    output_path = Path.join(tmp_dir, "av_json.ts")

    json_1 = ~s({"text":"hello","from":0,"to":1000})
    json_2 = ~s({"text":"world","from":1000,"to":2000})

    json_buffers = [
      %Membrane.Buffer{payload: json_1, pts: Membrane.Time.seconds(5)},
      %Membrane.Buffer{payload: json_2, pts: Membrane.Time.seconds(6)}
    ]

    spec = [
      child(:source, %Membrane.File.Source{location: "test/data/avsync.ts"})
      |> child(:demuxer, TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> child({:h264, :parser}, %Membrane.NALU.ParserBin{alignment: :aud, assume_aligned: true})
      |> via_in(:input, options: [stream_type: :H264_AVC, pcr?: true])
      |> get_child(:muxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x101])
      |> child({:aac, :parser}, %Membrane.AAC.Parser{})
      |> via_in(:input, options: [stream_type: :AAC_ADTS])
      |> get_child(:muxer),
      child(:json_source, %Membrane.Testing.Source{output: json_buffers})
      |> via_in(:input,
        options: [
          stream_type: :PES_PRIVATE_DATA,
          descriptors: [%{tag: 0x05, data: "JSON"}],
          wait_on_buffers?: false
        ]
      )
      |> get_child(:muxer),
      child(:muxer, Membrane.MPEG.TS.Muxer)
      |> child(:sink, %Membrane.File.Sink{location: output_path})
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pid, :sink)
    :ok = Membrane.Pipeline.terminate(pid)

    assert_ffprobe_streams(output_path, ["video:h264", "audio:aac"])

    demux_spec = [
      child(:source, %Membrane.File.Source{location: output_path})
      |> child(:demuxer, Membrane.MPEG.TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [stream_type: :H264_AVC])
      |> child(:h264_sink, %Membrane.Testing.Sink{}),
      get_child(:demuxer)
      |> via_out(:output, options: [stream_type: :AAC_ADTS])
      |> child(:aac_sink, %Membrane.Testing.Sink{}),
      get_child(:demuxer)
      |> via_out(:output,
        options: [stream_type: :PES_PRIVATE_DATA, registration_descriptor: "JSON"]
      )
      |> child(:json_sink, %Membrane.Testing.Sink{})
    ]

    demux_pid = Testing.Pipeline.start_link_supervised!(spec: demux_spec)

    assert_sink_stream_format(demux_pid, :h264_sink, %Membrane.RemoteStream{
      content_format: %Membrane.MPEG.TS.StreamFormat{stream_type: :H264_AVC}
    })

    assert_sink_stream_format(demux_pid, :aac_sink, %Membrane.RemoteStream{
      content_format: %Membrane.MPEG.TS.StreamFormat{stream_type: :AAC_ADTS}
    })

    assert_sink_stream_format(demux_pid, :json_sink, %Membrane.RemoteStream{
      content_format: %Membrane.MPEG.TS.StreamFormat{
        stream_type: :PES_PRIVATE_DATA,
        descriptors: [%{tag: 0x05, data: "JSON"}]
      }
    })

    assert_sink_buffer(demux_pid, :h264_sink, %Membrane.Buffer{})
    assert_sink_buffer(demux_pid, :aac_sink, %Membrane.Buffer{})
    assert_sink_buffer(demux_pid, :json_sink, %Membrane.Buffer{payload: ^json_1})
    assert_sink_buffer(demux_pid, :json_sink, %Membrane.Buffer{payload: ^json_2})

    assert_end_of_stream(demux_pid, :h264_sink, :input)
    assert_end_of_stream(demux_pid, :aac_sink, :input)
    assert_end_of_stream(demux_pid, :json_sink, :input)
    :ok = Testing.Pipeline.terminate(demux_pid)

    parse_spec = [
      child(:source, %Membrane.File.Source{location: output_path})
      |> child(:demuxer, Membrane.MPEG.TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [stream_type: :H264_AVC])
      |> child({:h264_out, :parser}, %Membrane.NALU.ParserBin{alignment: :aud, assume_aligned: true})
      |> child(:h264_parsed_sink, %Membrane.Testing.Sink{}),
      get_child(:demuxer)
      |> via_out(:output, options: [stream_type: :AAC_ADTS])
      |> child({:aac_out, :parser}, %Membrane.AAC.Parser{})
      |> child(:aac_parsed_sink, %Membrane.Testing.Sink{})
    ]

    parse_pid = Testing.Pipeline.start_link_supervised!(spec: parse_spec)
    assert_sink_buffer(parse_pid, :h264_parsed_sink, %Membrane.Buffer{})
    assert_sink_buffer(parse_pid, :aac_parsed_sink, %Membrane.Buffer{})
    assert_end_of_stream(parse_pid, :h264_parsed_sink, :input)
    assert_end_of_stream(parse_pid, :aac_parsed_sink, :input)
    :ok = Testing.Pipeline.terminate(parse_pid)
  end

  @tag :tmp_dir
  test "re-muxes avsync video, w/o specifying the stream_type", %{tmp_dir: tmp_dir} do
    output_path = Path.join(tmp_dir, "output.ts")

    spec = [
      child(:source, %Membrane.File.Source{location: "test/data/avsync.ts"})
      |> child(:demuxer, TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> via_in(:input)
      |> get_child(:muxer),
      child(:muxer, Membrane.MPEG.TS.Muxer)
      |> child(:sink, %Membrane.File.Sink{location: output_path})
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pid, :sink)
    :ok = Membrane.Pipeline.terminate(pid)

    # Validate the output with tsanalyze
    analysis = run_tsanalyze(output_path)

    # This test has only video (auto-detected from stream_format)
    expected_pids = %{
      0x100 => "AVC"  # Video on PID 0x100 (256), auto-detected
    }

    # Verify ONLY video PID is present (no unexpected PIDs)
    assert_only_expected_pids(analysis, expected_pids)

    # Verify service structure
    assert_service_structure(analysis, expected_pids)

    # Detailed validation for video stream
    video_details = get_pid_details(analysis, 0x100)
    assert video_details != nil, "Video PID 0x100 not found"
    assert video_details.is_video == true,
           "PID 0x100 is not marked as video (auto-detection failed): #{inspect(video_details)}"
    assert video_details.packet_count > 0, "Video PID 0x100 has no packets: #{inspect(video_details)}"

    # Verify no transport errors
    assert_no_errors(analysis)
  end

  @tag :tmp_dir
  test "re-muxes scte", %{tmp_dir: tmp_dir} do
    output_path = Path.join(tmp_dir, "output.ts")

    spec = [
      child(:source, %Membrane.File.Source{location: "test/data/scte35.ts"})
      |> child(:demuxer, TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> via_in(:input,
        options: [stream_type: :SCTE_35_SPLICE, pid: 500, wait_on_buffers?: false]
      )
      |> get_child(:muxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x101])
      |> child({:h264, :parser}, %Membrane.NALU.ParserBin{alignment: :aud, assume_aligned: true})
      |> via_in(:input, options: [stream_type: :H264_AVC, pid: 256, pcr?: true])
      |> get_child(:muxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x102])
      |> child({:aac, :parser}, %Membrane.AAC.Parser{})
      |> via_in(:input, options: [stream_type: :AAC_ADTS, pid: 257])
      |> get_child(:muxer),
      child(:muxer, Membrane.MPEG.TS.Muxer)
      |> child(:sink, %Membrane.File.Sink{
        location: output_path
      })
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pid, :sink)
    :ok = Membrane.Pipeline.terminate(pid)

    # Double check that the SCTE unit is indeed there.
    units =
      output_path
      |> MPEG.TS.Demuxer.stream_file!()
      |> Enum.into([])

    assert length(units) > 0

    assert Enum.filter(units, fn %{pid: x} -> x == 500 end) == [
             %MPEG.TS.Demuxer.Container{
               pid: 500,
               t: 6_400_000_000,
               payload: %MPEG.TS.PSI{
                 header: %{
                   table_id: 252,
                   section_syntax_indicator: false,
                   transport_stream_id: nil,
                   version_number: nil,
                   current_next_indicator: nil,
                   section_number: nil,
                   last_section_number: nil,
                   section_length: 37
                 },
                 table_type: :scte35,
                 table: %MPEG.TS.SCTE35{
                   protocol_version: 0,
                   encrypted_packet: 0,
                   encryption_algorithm: 0,
                   pts_adjustment: 0,
                   cw_index: 0,
                   tier: 4095,
                   splice_command_type: :splice_insert,
                   splice_command: %MPEG.TS.SCTE35.SpliceInsert{
                     event_id: 100,
                     cancel_indicator: 0,
                     out_of_network_indicator: 1,
                     program_splice_flag: 1,
                     duration_flag: 1,
                     splice_immediate_flag: 0,
                     event_id_compliance_flag: 1,
                     splice_time: %{pts: 60_000_000_000},
                     break_duration: %{auto_return: 0, duration: 60_000_000_000},
                     unique_program_id: 1,
                     avail_num: 18,
                     avails_expected: 255
                   },
                   splice_descriptors: [],
                   e_crc32: ""
                 },
                 crc: <<93, 125, 122, 192>>
               }
             }
           ]

    # Validate with tsanalyze - comprehensive PID checks
    analysis = run_tsanalyze(output_path)

    # Define exactly what PIDs we expect in the output
    expected_pids = %{
      256 => "AVC",   # Video on PID 0x100 (256)
      257 => "AAC",   # Audio on PID 0x101 (257)
      500 => "SCTE"   # SCTE-35 on PID 500
    }

    # Verify ONLY these PIDs are present (no unexpected PIDs)
    assert_only_expected_pids(analysis, expected_pids)

    # Verify service structure matches expected codecs
    assert_service_structure(analysis, expected_pids)

    # Detailed validation for each stream
    # Video stream (PID 256)
    video_details = get_pid_details(analysis, 256)
    assert video_details != nil, "Video PID 256 not found"
    assert video_details.is_video == true, "PID 256 is not marked as video: #{inspect(video_details)}"
    assert video_details.packet_count > 0, "Video PID 256 has no packets: #{inspect(video_details)}"

    # Audio stream (PID 257)
    audio_details = get_pid_details(analysis, 257)
    assert audio_details != nil, "Audio PID 257 not found"
    assert audio_details.is_audio == true, "PID 257 is not marked as audio: #{inspect(audio_details)}"
    assert audio_details.packet_count > 0, "Audio PID 257 has no packets: #{inspect(audio_details)}"

    # SCTE-35 stream (PID 500)
    scte_details = get_pid_details(analysis, 500)
    assert scte_details != nil, "SCTE PID 500 not found"
    assert scte_details.packet_count > 0, "SCTE PID 500 has no packets: #{inspect(scte_details)}"
    assert String.contains?(scte_details.description, "SCTE"),
           "PID 500 is not SCTE-35: #{inspect(scte_details)}"

    # Verify PCR is present on video stream (PID 256)
    assert_pcr_valid(analysis, 256)
    assert video_details.pcr_count > 0,
           "Video PID 256 should have PCR packets: #{inspect(video_details)}"

    # Verify audio stream is valid
    assert_audio_stream_valid(analysis, 257)

    # Verify no transport errors
    assert_no_errors(analysis)
  end

  @tag :tmp_dir
  test "muxes and demuxes SCTE-35 as PSI", %{tmp_dir: tmp_dir} do
    output_path = Path.join(tmp_dir, "scte35_psi.ts")

    splice_insert = %MPEG.TS.SCTE35.SpliceInsert{
      event_id: 100,
      cancel_indicator: 0,
      out_of_network_indicator: 1,
      program_splice_flag: 1,
      duration_flag: 1,
      splice_immediate_flag: 0,
      event_id_compliance_flag: 1,
      splice_time: %{pts: Membrane.Time.seconds(60)},
      break_duration: %{auto_return: 0, duration: Membrane.Time.seconds(60)},
      unique_program_id: 1,
      avail_num: 18,
      avails_expected: 255
    }

    scte35 = %MPEG.TS.SCTE35{
      protocol_version: 0,
      encrypted_packet: 0,
      encryption_algorithm: 0,
      pts_adjustment: 0,
      cw_index: 0,
      tier: 0xFFF,
      splice_command_type: :splice_insert,
      splice_command: splice_insert,
      splice_descriptors: [],
      e_crc32: <<>>
    }

    psi = %MPEG.TS.PSI{
      header: %{
        table_id: 0xFC,
        section_syntax_indicator: false,
        transport_stream_id: nil,
        version_number: nil,
        current_next_indicator: nil,
        section_number: nil,
        last_section_number: nil
      },
      table_type: :scte35,
      table: scte35,
      crc: <<>>
    }

    buffers = [
      %Membrane.Buffer{
        payload: <<>>,
        pts: Membrane.Time.seconds(5),
        metadata: %{psi: psi}
      }
    ]

    spec = [
      child(:scte_source, %Membrane.Testing.Source{output: buffers})
      |> via_in(:input,
        options: [
          stream_type: :SCTE_35_SPLICE,
          pid: 500,
          wait_on_buffers?: false
        ]
      )
      |> get_child(:muxer),
      child(:muxer, Membrane.MPEG.TS.Muxer)
      |> child(:sink, %Membrane.File.Sink{location: output_path})
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pid, :sink)
    :ok = Membrane.Pipeline.terminate(pid)

    demux_spec = [
      child(:source, %Membrane.File.Source{location: output_path})
      |> child(:demuxer, Membrane.MPEG.TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [stream_type: :SCTE_35_SPLICE])
      |> child(:sink, %Membrane.Testing.Sink{})
    ]

    demux_pid = Testing.Pipeline.start_link_supervised!(spec: demux_spec)

    assert_sink_stream_format(demux_pid, :sink, %Membrane.RemoteStream{
      content_format: %Membrane.MPEG.TS.StreamFormat{
        stream_type: :SCTE_35_SPLICE
      }
    })

    assert_sink_buffer(demux_pid, :sink, %Membrane.Buffer{
      metadata: %{
        psi: %MPEG.TS.PSI{
          table_type: :scte35,
          table: %MPEG.TS.SCTE35{
            splice_command_type: :splice_insert,
            splice_command: %MPEG.TS.SCTE35.SpliceInsert{event_id: 100}
          }
        }
      }
    })

    assert_end_of_stream(demux_pid, :sink, :input)
    :ok = Testing.Pipeline.terminate(demux_pid)
  end

  @tag :tmp_dir
  test "validates PID correctness and detects corruption", %{tmp_dir: tmp_dir} do
    output_path = Path.join(tmp_dir, "pid_validation.ts")

    spec = [
      child(:source, %Membrane.File.Source{location: "test/data/avsync.ts"})
      |> child(:demuxer, TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> child({:h264, :parser}, %Membrane.NALU.ParserBin{alignment: :aud, assume_aligned: true})
      |> via_in(:input, options: [stream_type: :H264_AVC, pid: 300, pcr?: true])
      |> get_child(:muxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x101])
      |> child({:aac, :parser}, %Membrane.AAC.Parser{})
      |> via_in(:input, options: [stream_type: :AAC_ADTS, pid: 301])
      |> get_child(:muxer),
      child(:muxer, Membrane.MPEG.TS.Muxer)
      |> child(:sink, %Membrane.File.Sink{location: output_path})
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pid, :sink)
    :ok = Membrane.Pipeline.terminate(pid)

    # Validate with tsanalyze - this is the critical corruption detection test
    analysis = run_tsanalyze(output_path)

    # Define exactly what PIDs we expect in the stream
    expected_pids = %{
      300 => "AVC",  # Video on PID 300
      301 => "AAC"   # Audio on PID 301
    }

    # This is the critical test - verify ONLY expected PIDs are present
    # If the corruption bug returns, this will catch it
    assert_only_expected_pids(analysis, expected_pids)

    # Verify service structure
    assert_service_structure(analysis, expected_pids)

    # Verify PCR is present on video stream
    assert_pcr_valid(analysis, 300)

    # Verify no transport errors
    assert_no_errors(analysis)
  end

  @tag :tmp_dir
  test "PAT/PMT insertion follows 500ms spec requirement", %{tmp_dir: tmp_dir} do
    output_path = Path.join(tmp_dir, "pat_pmt_timing.ts")

    spec = [
      child(:source, %Membrane.File.Source{location: "test/data/avsync.ts"})
      |> child(:demuxer, TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> child({:h264, :parser}, %Membrane.NALU.ParserBin{alignment: :aud, assume_aligned: true})
      |> via_in(:input, options: [stream_type: :H264_AVC, pcr?: true])
      |> get_child(:muxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x101])
      |> child({:aac, :parser}, %Membrane.AAC.Parser{})
      |> via_in(:input, options: [stream_type: :AAC_ADTS])
      |> get_child(:muxer),
      child(:muxer, Membrane.MPEG.TS.Muxer)
      |> child(:sink, %Membrane.File.Sink{location: output_path})
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pid, :sink)
    :ok = Membrane.Pipeline.terminate(pid)

    # Parse the output and collect all PAT packets with timestamps
    # Note: Filter out nil timestamps (first PAT before any media packets)
    pat_packets =
      output_path
      |> MPEG.TS.Demuxer.stream_file!()
      |> Enum.filter(fn container ->
        container.pid == 0x0000 and container.t != nil  # PAT PID with valid timestamp
      end)
      |> Enum.to_list()

    # Verify we have PAT packets
    assert length(pat_packets) > 0, "No PAT packets with timestamps found in output"

    # Check intervals between consecutive PAT packets
    pat_intervals =
      pat_packets
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.map(fn [prev, curr] -> curr.t - prev.t end)

    # Per spec, PAT must be inserted at least every 500ms (500_000_000 nanoseconds)
    max_interval = 500_000_000

    # Check that no interval exceeds 500ms
    oversized_intervals = Enum.filter(pat_intervals, fn interval -> interval > max_interval end)

    assert oversized_intervals == [],
           """
           Found PAT intervals exceeding 500ms spec requirement:
           #{Enum.map(oversized_intervals, fn i -> "#{div(i, 1_000_000)}ms" end) |> Enum.join(", ")}

           All intervals: #{Enum.map(pat_intervals, fn i -> "#{div(i, 1_000_000)}ms" end) |> Enum.join(", ")}
           """

    # Also verify basic structure with tsanalyze
    analysis = run_tsanalyze(output_path)
    expected_pids = %{0x100 => "AVC", 0x101 => "AAC"}
    assert_service_structure(analysis, expected_pids)
    assert_no_errors(analysis)
  end

  @tag :tmp_dir
  test "PAT/PMT insertion works for audio-only streams", %{tmp_dir: tmp_dir} do
    output_path = Path.join(tmp_dir, "audio_only_pat_pmt.ts")

    spec = [
      child(:source, %Membrane.File.Source{location: "test/data/avsync.ts"})
      |> child(:demuxer, TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x101])
      |> child({:aac, :parser}, %Membrane.AAC.Parser{})
      |> via_in(:input, options: [stream_type: :AAC_ADTS, pid: 0x101])
      |> get_child(:muxer),
      child(:muxer, Membrane.MPEG.TS.Muxer)
      |> child(:sink, %Membrane.File.Sink{location: output_path})
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pid, :sink)
    :ok = Membrane.Pipeline.terminate(pid)

    # Parse the output and collect all PAT/PMT packets
    # First check if PAT/PMT packets exist at all
    all_pat_packets =
      output_path
      |> MPEG.TS.Demuxer.stream_file!()
      |> Enum.filter(fn container -> container.pid == 0x0000 end)
      |> Enum.to_list()

    all_pmt_packets =
      output_path
      |> MPEG.TS.Demuxer.stream_file!()
      |> Enum.filter(fn container -> container.pid == 0x1000 end)
      |> Enum.to_list()

    # For audio-only streams without PCR, the demuxer cannot assign timestamps to PSI packets
    # We verify that PAT/PMT packets exist and are frequent enough based on packet count
    assert length(all_pat_packets) > 0, "No PAT packets found in audio-only stream"
    assert length(all_pmt_packets) > 0, "No PMT packets found in audio-only stream"

    # Get audio stream duration to calculate expected PAT/PMT count
    audio_packets =
      output_path
      |> MPEG.TS.Demuxer.stream_file!()
      |> Enum.filter(fn c -> c.pid == 0x101 and c.t != nil end)
      |> Enum.to_list()

    if length(audio_packets) > 1 do
      first_audio_t = List.first(audio_packets).t
      last_audio_t = List.last(audio_packets).t
      duration_ms = div(last_audio_t - first_audio_t, 1_000_000)

      # With 500ms intervals, we expect at least (duration / 500) PAT/PMT pairs
      # Allow for off-by-one due to timing boundaries
      expected_min_pat = max(1, div(duration_ms, 500) - 1)

      assert length(all_pat_packets) >= expected_min_pat,
             """
             Expected at least #{expected_min_pat} PAT packets for #{duration_ms}ms stream (500ms interval),
             but found #{length(all_pat_packets)}
             """

      assert length(all_pmt_packets) >= expected_min_pat,
             """
             Expected at least #{expected_min_pat} PMT packets for #{duration_ms}ms stream (500ms interval),
             but found #{length(all_pmt_packets)}
             """
    end

    # Verify basic structure with tsanalyze
    analysis = run_tsanalyze(output_path)
    expected_pids = %{0x101 => "AAC"}
    assert_service_structure(analysis, expected_pids)
    assert_no_errors(analysis)
  end

  @tag :tmp_dir
  test "muxer correctly handles timestamp rollover conversion", %{tmp_dir: tmp_dir} do
    output_path = Path.join(tmp_dir, "rollover_muxed.ts")

    # Get original timestamps from input file
    original_containers =
      "test/data/rollover.ts"
      |> MPEG.TS.Demuxer.stream_file!()
      |> Stream.filter(fn %{pid: pid} -> pid == 0x100 end)
      |> Enum.to_list()

    spec = [
      child(:source, %Membrane.File.Source{location: "test/data/rollover.ts"})
      |> child(:demuxer, TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> child({:h264, :parser}, %Membrane.NALU.ParserBin{alignment: :aud, assume_aligned: true})
      |> via_in(:input, options: [stream_type: :H264_AVC])
      |> get_child(:muxer),
      child(:muxer, Membrane.MPEG.TS.Muxer)
      |> child(:sink, %Membrane.File.Sink{location: output_path})
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pid, :sink)
    :ok = Membrane.Pipeline.terminate(pid)

    # Get timestamps from muxed output file
    output_containers =
      output_path
      |> MPEG.TS.Demuxer.stream_file!()
      |> Stream.filter(fn %{pid: pid} -> pid == 0x100 end)
      |> Enum.to_list()

    assert length(output_containers) > 0
    assert length(original_containers) == length(output_containers)

    # Verify that original and output timestamps match exactly
    Enum.zip(original_containers, output_containers)
    |> Enum.each(fn {original, output} ->
      assert original.t == output.t,
             "Timestamp mismatch: original=#{original.t}, output=#{output.t}"
    end)

    # Additional validation with tsanalyze
    analysis = run_tsanalyze(output_path)

    # This test has only video
    expected_pids = %{
      0x100 => "AVC"  # Video on PID 0x100 (256)
    }

    # Verify ONLY video PID is present (no unexpected PIDs)
    assert_only_expected_pids(analysis, expected_pids)

    # Verify service structure
    assert_service_structure(analysis, expected_pids)

    # Detailed validation for video stream
    video_details = get_pid_details(analysis, 0x100)
    assert video_details != nil, "Video PID 0x100 not found"
    assert video_details.is_video == true, "PID 0x100 is not marked as video: #{inspect(video_details)}"
    assert video_details.packet_count > 0, "Video PID 0x100 has no packets: #{inspect(video_details)}"

    # Verify no transport errors (critical for timestamp rollover test)
    assert_no_errors(analysis)
  end

end
