defmodule Membrane.MPEG.TS.MuxerTest do
  use ExUnit.Case, async: true
  import Membrane.Testing.Assertions
  import Membrane.ChildrenSpec
  require Membrane.Pad
  alias Membrane.MPEG.TS

  @input "test/data/avsync.ts"

  @tag :tmp_dir
  test "re-muxes avsync", %{tmp_dir: tmp_dir} do
    spec = [
      child(:source, %Membrane.File.Source{location: @input})
      |> child(:demuxer, TS.Demuxer),
      get_child(:demuxer)
      |> via_out(:output, options: [pid: 0x100])
      |> child(:parser, %Membrane.NALU.ParserBin{alignment: :aud, assume_aligned: true})
      |> via_in(:input, options: [stream_type: :H264_AVC])
      |> get_child(:muxer),
      # TODO: AAC
      # get_child(:demuxer)
      # |> via_out(:output, pad_options: [pid: 0x101])
      # |> child(:parser, %Membrane.NALU.ParserBin{alignment: :aud, assume_aligned: true})
      # |> via_in(:input, pad_options: [stream_type: :H264_AAC])
      # get_child(:muxer),
      child(:muxer, Membrane.MPEG.TS.Muxer)
      |> child(:sink, %Membrane.File.Sink{
        location: Path.join(tmp_dir, "output.ts")
      })
    ]

    pid = Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pid, :sink)

    :ok = Membrane.Pipeline.terminate(pid)
  end
end
