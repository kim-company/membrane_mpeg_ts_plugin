defmodule Membrane.MPEG.TS.MuxerTest do
  use ExUnit.Case, async: true
  import Membrane.Testing.Assertions
  import Membrane.ChildrenSpec
  require Membrane.Pad
  alias Membrane.MPEG.TS

  # Helper function to run tsanalyze and parse JSON output
  defp run_tsanalyze(file_path) do
    {output, 0} = System.cmd("tsanalyze", ["--json", file_path])
    JSON.decode!(output)
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
