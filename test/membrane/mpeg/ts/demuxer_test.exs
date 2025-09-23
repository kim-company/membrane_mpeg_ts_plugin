defmodule Membrane.MPEG.TS.DemuxerTest do
  use ExUnit.Case, async: true

  import Membrane.Testing.Assertions
  import Membrane.ChildrenSpec
  alias Membrane.Testing

  @tag :tmp_dir
  test "demuxes avsync specifying PIDs", %{tmp_dir: tmp_dir} do
    aac_out = Path.join(tmp_dir, "output.aac")
    h264_out = Path.join(tmp_dir, "output.h264")

    spec = [
      child(:source, %Membrane.File.Source{
        location: "test/data/avsync.ts"
      })
      |> child(:demuxer, Membrane.MPEG.TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> child({:sink, :h264}, %Membrane.File.Sink{
        location: h264_out
      }),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x101])
      |> child({:sink, :aac}, %Membrane.File.Sink{
        location: aac_out
      })
    ]

    pid = Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_pipeline_notified(pid, :demuxer, {:pmt, %MPEG.TS.PMT{}})
    assert_end_of_stream(pid, {:sink, :h264}, :input)
    assert_end_of_stream(pid, {:sink, :aac}, :input)
    :ok = Testing.Pipeline.terminate(pid)

    assert_files_equal(aac_out, "test/data/avsync.aac")
    assert_files_equal(h264_out, "test/data/avsync.h264")
  end

  @tag :tmp_dir
  test "demuxes video from avsync specifing its stream_type", %{tmp_dir: tmp_dir} do
    h264_out = Path.join(tmp_dir, "output.h264")

    spec = [
      child(:source, %Membrane.File.Source{
        location: "test/data/avsync.ts"
      })
      |> child(:demuxer, Membrane.MPEG.TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [stream_type: :H264_AVC])
      |> child({:sink, :h264}, %Membrane.File.Sink{
        location: h264_out
      })
    ]

    pid = Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_pipeline_notified(pid, :demuxer, {:pmt, %MPEG.TS.PMT{}})
    assert_end_of_stream(pid, {:sink, :h264}, :input)
    :ok = Testing.Pipeline.terminate(pid)

    assert_files_equal(h264_out, "test/data/avsync.h264")
  end

  @tag :tmp_dir
  test "demuxes video from avsync specifing its stream category", %{tmp_dir: tmp_dir} do
    h264_out = Path.join(tmp_dir, "output.h264")

    spec = [
      child(:source, %Membrane.File.Source{
        location: "test/data/avsync.ts"
      })
      |> child(:demuxer, Membrane.MPEG.TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [stream_category: :video])
      |> child({:sink, :h264}, %Membrane.File.Sink{
        location: h264_out
      })
    ]

    pid = Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_pipeline_notified(pid, :demuxer, {:pmt, %MPEG.TS.PMT{}})
    assert_end_of_stream(pid, {:sink, :h264}, :input)
    :ok = Testing.Pipeline.terminate(pid)

    assert_files_equal(h264_out, "test/data/avsync.h264")
  end

  @tag :tmp_dir
  test "demuxes audio from avsync specifing its stream type", %{tmp_dir: tmp_dir} do
    aac_out = Path.join(tmp_dir, "output.aac")

    spec = [
      child(:source, %Membrane.File.Source{
        location: "test/data/avsync.ts"
      })
      |> child(:demuxer, Membrane.MPEG.TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [stream_type: :AAC_ADTS])
      |> child({:sink, :aac}, %Membrane.File.Sink{
        location: aac_out
      })
    ]

    pid = Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_pipeline_notified(pid, :demuxer, {:pmt, %MPEG.TS.PMT{}})
    assert_end_of_stream(pid, {:sink, :aac}, :input)
    :ok = Testing.Pipeline.terminate(pid)

    assert_files_equal(aac_out, "test/data/avsync.aac")
  end

  defp assert_files_equal(file_a, file_b) do
    assert {:ok, a} = File.read(file_a)
    assert {:ok, b} = File.read(file_b)
    assert byte_size(a) == byte_size(b)
    assert a == b
    assert a > 0
  end
end
