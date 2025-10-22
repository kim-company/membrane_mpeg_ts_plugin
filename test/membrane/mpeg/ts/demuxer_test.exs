defmodule Membrane.MPEG.TS.DemuxerTest do
  use ExUnit.Case, async: true

  import Membrane.Testing.Assertions
  import Membrane.ChildrenSpec
  alias Membrane.Testing

  test "content_format contains the correct format" do
    spec = [
      child(:source, %Membrane.File.Source{
        location: "test/data/avsync.ts"
      })
      |> child(:demuxer, Membrane.MPEG.TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> child({:sink, :h264}, %Membrane.Testing.Sink{}),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x101])
      |> child({:sink, :aac}, %Membrane.Testing.Sink{})
    ]

    pid = Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pid, {:sink, :h264}, :input)
    assert_end_of_stream(pid, {:sink, :aac}, :input)
    :ok = Membrane.Pipeline.terminate(pid)

    assert_sink_stream_format(pid, {:sink, :h264}, %Membrane.RemoteStream{
      content_format: %Membrane.MPEG.TS.StreamFormat{stream_type: :H264_AVC}
    })

    assert_sink_stream_format(pid, {:sink, :aac}, %Membrane.RemoteStream{
      content_format: %Membrane.MPEG.TS.StreamFormat{stream_type: :AAC_ADTS}
    })
  end

  test "correctly handles the mpegts rollover and converts it into monotonic pts/dts" do
    # NOTE: This test file was generated using the following ffmpeg command:
    # ```bash
    # ffmpeg -f lavfi -i "testsrc2=size=128x72:rate=1" -t 20 \
    #    -c:v libx264 -preset veryslow -crf 42 -pix_fmt yuv420p \
    #    -g 30 -bf 3 -sc_threshold 0 -x264-params "keyint=30:min-keyint=30:scenecut=0" \
    #    -an \
    #    -mpegts_copyts 1 \
    #    -output_ts_offset 95433.7176889 \
    #    -pat_period 1.0 -sdt_period 5.0 \
    #    -f mpegts rollover.ts
    # ```
    spec = [
      child(:source, %Membrane.File.Source{
        location: "test/data/rollover.ts"
      })
      |> child(:demuxer, Membrane.MPEG.TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> child({:sink, :h264}, %Membrane.Testing.Sink{})
    ]

    rollover_period_ns = 95_443_717_688_889

    Stream.resource(
      fn ->
        Testing.Pipeline.start_link_supervised!(spec: spec)
      end,
      fn pid ->
        receive do
          {Membrane.Testing.Pipeline, ^pid,
           {:handle_child_notification, {{:buffer, buffer}, {:sink, :h264}}}} ->
            {[buffer], pid}

          {Membrane.Testing.Pipeline, ^pid,
           {:handle_child_notification, {{:end_of_stream, :input}, {:sink, :h264}}}} ->
            {:halt, pid}
        after
          3_000 ->
            raise "test timeout"
        end
      end,
      fn pid -> Membrane.Testing.Pipeline.terminate(pid, force?: true) end
    )
    |> Enum.reduce(fn buf, prev_buf ->
      # Assert that the timestamps are monotonically increasing
      assert buf.dts > prev_buf.dts

      # Ensure that its a consistent timeline
      assert_in_delta buf.pts, buf.pts, Membrane.Time.second()
      assert_in_delta buf.dts, prev_buf.dts, Membrane.Time.seconds(1)
      assert_in_delta buf.pts, prev_buf.pts, Membrane.Time.seconds(5)

      # Ensure that we dont go above a certain value
      assert buf.dts < rollover_period_ns + Membrane.Time.minutes(1)
      assert buf.pts < rollover_period_ns + Membrane.Time.minutes(1)

      buf
    end)
  end

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

  test "demuxes SCTE35 data" do
    spec = [
      child(:source, %Membrane.File.Source{
        location: "test/data/scte35.ts"
      })
      |> child(:demuxer, Membrane.MPEG.TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [stream_type: :SCTE_35_SPLICE])
      |> child(:sink, Membrane.Testing.Sink)
    ]

    pid = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_sink_buffer(
      pid,
      :sink,
      %Membrane.Buffer{
        payload:
          <<0, 252, 48, 37, 0, 0, 0, 0, 0, 0, 0, 255, 240, 20, 5, 0, 0, 0, 100, 127, 239, 254, 0,
            82, 101, 192, 126, 0, 82, 101, 192, 0, 1, 18, 255, 0, 0, 93, 125, 122, 192>>,
        dts: 6_440_000_000,
        metadata: %{psi: %{table_type: :scte35, table: %MPEG.TS.SCTE35{}}}
      }
    )

    assert_end_of_stream(pid, :sink, :input)
    :ok = Testing.Pipeline.terminate(pid)
  end

  defp assert_files_equal(file_a, file_b) do
    assert {:ok, a} = File.read(file_a)
    assert {:ok, b} = File.read(file_b)
    assert byte_size(a) == byte_size(b)
    assert a == b
    assert a > 0
  end
end
