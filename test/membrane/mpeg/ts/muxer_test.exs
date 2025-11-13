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

    # Validate the output with tsanalyze
    analysis = run_tsanalyze(output_path)

    # Verify service structure: H264 video on 0x100, AAC audio on 0x101
    assert_service_structure(analysis, %{0x100 => "AVC", 0x101 => "AAC"})

    # Verify audio stream is valid
    assert_audio_stream_valid(analysis, 0x101)

    # Verify PCR is present on video stream
    assert_pcr_valid(analysis, 0x100)

    # Verify no transport errors
    assert_no_errors(analysis)
  end

  @tag :tmp_dir
  test "re-muxes avsync video, w/o specifying the stream_type", %{tmp_dir: tmp_dir} do
    spec = [
      child(:source, %Membrane.File.Source{location: "test/data/avsync.ts"})
      |> child(:demuxer, TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> via_in(:input)
      |> get_child(:muxer),
      child(:muxer, Membrane.MPEG.TS.Muxer)
      |> child(:sink, %Membrane.File.Sink{
        location: Path.join(tmp_dir, "output.ts")
      })
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pid, :sink)

    :ok = Membrane.Pipeline.terminate(pid)
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
      |> via_in(:input, options: [stream_type: :H264_AVC, pcr?: true])
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

    # Validate with tsanalyze
    analysis = run_tsanalyze(output_path)

    # Verify PCR is present on video stream (PID 0x101 / 257)
    assert_pcr_valid(analysis, 0x101)

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
  end
end
