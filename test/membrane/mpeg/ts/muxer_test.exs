defmodule Membrane.MPEG.TS.MuxerTest do
  use ExUnit.Case, async: true
  import Membrane.Testing.Assertions
  import Membrane.ChildrenSpec
  require Membrane.Pad
  alias Membrane.MPEG.TS

  @input "test/fixtures/avsync.ts"

  @tag :tmp_dir
  test "muxes h264 into segments", %{tmp_dir: tmp_dir} do
    pid = start_supervised!({Agent, fn -> 0 end})

    spec = [
      child(:source, %Membrane.File.Source{location: @input})
      |> child(:demuxer, TS.AVDemuxer)
      |> via_out(:video)
      |> child(:parser, %Membrane.NALU.ParserBin{alignment: :aud, assume_aligned: true})
      |> via_in(:input)
      |> child(:muxer, Membrane.MPEG.TS.Muxer)
      |> child(:aggregator, %Membrane.MPEG.TS.Aggregator{
        target_duration: Membrane.Time.seconds(99999)
      })
      |> child(:debug, %Membrane.Debug.Filter{
        handle_buffer: fn x ->
          index = Agent.get_and_update(pid, fn x -> {x, x + 1} end)
          filename = String.pad_leading("#{index}", 3, "0") <> ".ts"
          path = Path.join(tmp_dir, filename)
          File.write!(path, x.payload)
        end
      })
      |> child(:sink, Membrane.Testing.Sink),
      get_child(:demuxer)
      |> via_out(:audio)
      |> child(:void, Membrane.Fake.Sink)
    ]

    pipeline = Membrane.Testing.Pipeline.start_link_supervised!(spec: spec)
    assert_end_of_stream(pipeline, :sink)

    :ok = Membrane.Pipeline.terminate(pipeline)
  end
end
