defmodule Membrane.MPEG.TS.DemuxerTest do
  use ExUnit.Case
  import Membrane.Testing.Assertions
  alias Membrane.Testing.Pipeline

  @input_path "./test/fixtures/all_packets.ts"
  @reference_audio "./test/fixtures/reference_audio.ts"
  @reference_video "./test/fixtures/reference_video.ts"

  test "extracts audio and video using a dynamically linked pipeline" do
    video_out = Path.join([System.tmp_dir!(), "membrane_video.ts"])
    audio_out = Path.join([System.tmp_dir!(), "membrane_audio.ts"])

    options = [
      module: Support.DynamicPipeline,
      custom_args: %{
        input_path: @input_path,
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

    assert_end_of_stream(pipeline, :video_out)
    assert_end_of_stream(pipeline, :audio_out)

    assert_files_equal(@reference_audio, audio_out)
    assert_files_equal(@reference_video, video_out)
  end

  defp assert_files_equal(file_a, file_b) do
    assert {:ok, a} = File.read(file_a)
    assert {:ok, b} = File.read(file_b)
    assert a == b
  end
end
