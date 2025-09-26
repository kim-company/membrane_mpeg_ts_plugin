defmodule Membrane.MPEG.TS.MuxerTest do
  use ExUnit.Case, async: true
  import Membrane.Testing.Assertions
  import Membrane.ChildrenSpec
  require Membrane.Pad
  alias Membrane.MPEG.TS

  @tag :tmp_dir
  test "re-muxes avsync", %{tmp_dir: tmp_dir} do
    spec = [
      child(:source, %Membrane.File.Source{location: "test/data/avsync.ts"})
      |> child(:demuxer, TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> child({:h264, :parser}, %Membrane.NALU.ParserBin{alignment: :aud, assume_aligned: true})
      |> via_in(:input, options: [stream_type: :H264_AVC])
      |> get_child(:muxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x101])
      |> child({:aac, :parser}, %Membrane.AAC.Parser{})
      |> via_in(:input, options: [stream_type: :AAC_ADTS])
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
      |> via_in(:input, options: [stream_type: :SCTE_35_SPLICE])
      |> get_child(:muxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x101])
      |> child({:h264, :parser}, %Membrane.NALU.ParserBin{alignment: :aud, assume_aligned: true})
      |> via_in(:input, options: [stream_type: :H264_AVC])
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

    assert Enum.filter(units, fn %{pid: x} -> x == 256 end) == [
             %MPEG.TS.Demuxer.Container{
               pid: 256,
               t: 60_000_000_000,
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
  end
end
