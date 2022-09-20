defmodule Support.DynamicPipeline do
  @moduledoc false

  use Membrane.Pipeline
  alias Membrane.File

  @impl true
  def handle_init(%{input_path: input_path, chunk_size: chunk_size} = options) do
    elements = [
      in: %File.Source{location: input_path, chunk_size: chunk_size},
      demuxer: Membrane.MPEG.TS.Demuxer
    ]

    links = [
      link(:in) |> to(:demuxer)
    ]

    spec = %ParentSpec{
      children: elements,
      links: links
    }

    {{:ok, spec: spec, playback: :playing}, options}
  end

  @impl true
  def handle_notification({:mpeg_ts_pmt, pmt}, _element, _context, state) do
    streams =
      pmt.streams
      |> Enum.filter(fn {_, %{stream_type: type}} ->
        Enum.member?([:H264, :MPEG1_AUDIO, :AAC], type)
      end)

    elements =
      streams
      |> Enum.map(fn {_, %{stream_type: type}} ->
        link_to = link_from_stream_type(type)
        {link_to, %File.Sink{location: Map.get(state, link_to)}}
      end)

    links =
      streams
      |> Enum.map(fn {sid, %{stream_type: type}} ->
        link_to = link_from_stream_type(type)

        link(:demuxer)
        |> via_out(Pad.ref(:output, {:stream_id, sid}))
        |> to(link_to)
      end)

    spec = %ParentSpec{
      children: elements,
      links: links
    }

    {{:ok, spec: spec}, state}
  end

  defp link_from_stream_type(:H264), do: :video_out
  defp link_from_stream_type(:MPEG1_AUDIO), do: :audio_out
  defp link_from_stream_type(:AAC), do: :audio_out
end
