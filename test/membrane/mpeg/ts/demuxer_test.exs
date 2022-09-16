defmodule Membrane.MPEG.TS.DemuxerTest do
  use ExUnit.Case
  import Membrane.Testing.Assertions
  alias Membrane.Testing.Pipeline

  test "demuxes bundestages example" do
    run_pipeline("reference-0")
  end

  test "demuxes reference" do
    run_pipeline("reference-1")
  end

  defp run_pipeline(data_dir) do
    [input, audio, video] =
      ["all.ts", "audio.ts", "video.ts"]
      |> Enum.map(&Path.join(["test", "fixtures", data_dir, &1]))

    video_out = Path.join([System.tmp_dir!(), "video.ts"])
    audio_out = Path.join([System.tmp_dir!(), "audio.ts"])

    options = [
      module: Support.DynamicPipeline,
      custom_args: %{
        input_path: input,
        audio_out: audio_out,
        video_out: video_out
      }
    ]

    {:ok, pipeline} = Pipeline.start_link(options)

    # TODO: this function is deprecated but if not called the pipeline is not
    # starting \o/
    Pipeline.play(pipeline)

    assert_start_of_stream(pipeline, :video_out)
    assert_start_of_stream(pipeline, :audio_out)

    assert_end_of_stream(pipeline, :video_out, :input, 10_000)
    assert_end_of_stream(pipeline, :audio_out, :input, 10_000)

    assert_files_equal(audio, audio_out)
    assert_files_equal(video, video_out)
  end

  defp assert_files_equal(file_a, file_b) do
    assert {:ok, a} = File.read(file_a)
    assert {:ok, b} = File.read(file_b)
    assert a == b
  end
end
