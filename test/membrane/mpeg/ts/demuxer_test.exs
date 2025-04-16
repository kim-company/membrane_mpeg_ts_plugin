defmodule Membrane.MPEG.TS.DemuxerTest do
  use ExUnit.Case

  import Membrane.Testing.Assertions
  alias Membrane.Testing

  defmodule Pipeline do
    @moduledoc false

    use Membrane.Pipeline
    alias Membrane.File

    @impl true
    def handle_init(_, %{input_path: input_path, chunk_size: chunk_size} = options) do
      links = [
        child(:in, %File.Source{location: input_path, chunk_size: chunk_size})
        |> child(:demuxer, Membrane.MPEG.TS.Demuxer)
      ]

      {[spec: links], options}
    end

    @impl true
    def handle_child_notification({:mpeg_ts_pmt, pmt}, _element, _context, state) do
      streams =
        pmt.streams
        |> Enum.filter(fn {_, %{stream_type: type}} ->
          Enum.member?([:H264, :MPEG1_AUDIO, :AAC], type)
        end)

      links =
        streams
        |> Enum.map(fn {sid, %{stream_type: type}} ->
          link_to = link_from_stream_type(type)

          get_child(:demuxer)
          |> via_out(Pad.ref(:output, {:stream_id, sid}))
          |> child(link_to, %File.Sink{location: Map.get(state, link_to)})
        end)

      {[spec: links], state}
    end

    defp link_from_stream_type(:H264), do: :video_out
    defp link_from_stream_type(:MPEG1_AUDIO), do: :audio_out
    defp link_from_stream_type(:AAC), do: :audio_out
  end

  defp test_pipeline(fixture_dir, tmp_dir) do
    [input, audio, video] =
      ["all.ts", "audio.ts", "video.ts"]
      |> Enum.map(&Path.join(["test", "fixtures", fixture_dir, &1]))

    video_out = Path.join([tmp_dir, "video.ts"])
    audio_out = Path.join([tmp_dir, "audio.ts"])

    # Using a chunk_size != 188 ensures that the pipeline is capable of
    # handling buffers that are not exactly the size of TS packet.
    options = [
      module: Pipeline,
      custom_args: %{
        chunk_size: 512,
        input_path: input,
        audio_out: audio_out,
        video_out: video_out
      }
    ]

    {:ok, _, pipeline} = Testing.Pipeline.start_link(options)
    assert_end_of_stream(pipeline, :video_out, :input)
    assert_end_of_stream(pipeline, :audio_out, :input)
    Testing.Pipeline.terminate(pipeline)

    assert_files_equal(audio, audio_out)
    assert_files_equal(video, video_out)
  end

  defp assert_files_equal(file_a, file_b) do
    assert {:ok, a} = File.read(file_a)
    assert {:ok, b} = File.read(file_b)
    assert byte_size(a) == byte_size(b)
    assert a == b
  end

  @tag :tmp_dir
  test "demuxes bundestag example", %{tmp_dir: tmp_dir} do
    test_pipeline("bundestag", tmp_dir)
  end
end
