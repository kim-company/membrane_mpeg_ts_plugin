defmodule Membrane.MPEG.TS.DemuxerTest do
  use ExUnit.Case
  import Membrane.Testing.Assertions
  alias Membrane.Testing.Pipeline

  @tag :tmp_dir
  test "demuxes reference", %{tmp_dir: dir} do
    run_pipeline("reference", dir)
  end

  defp run_pipeline(data_dir, tmp_dir) do
    [input, audio, video] =
      ["all.ts", "audio.ts", "video.ts"]
      |> Enum.map(&Path.join(["test", "fixtures", data_dir, &1]))

    video_out = Path.join([tmp_dir, "video.ts"])
    audio_out = Path.join([tmp_dir, "audio.ts"])

    # Using a chunk_size != 188 ensures that the pipeline is capable of
    # handling buffers that are not exactly the size of TS packet.
    options = [
      module: Support.DynamicPipeline,
      custom_args: %{
        chunk_size: 512,
        input_path: input,
        audio_out: audio_out,
        video_out: video_out
      }
    ]

    {:ok, pipeline} = Pipeline.start_link(options)

    assert_end_of_stream(pipeline, :video_out, :input)
    assert_end_of_stream(pipeline, :audio_out, :input)
    Pipeline.terminate(pipeline, blocking?: true)

    assert_files_equal(audio, audio_out)
    assert_files_equal(video, video_out)
  end

  defp assert_files_equal(file_a, file_b) do
    assert {:ok, a} = File.read(file_a)
    assert {:ok, b} = File.read(file_b)
    assert byte_size(a) == byte_size(b)
    assert a == b
  end
end
