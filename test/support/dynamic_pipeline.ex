defmodule Support.DynamicPipeline do
  @moduledoc false

  use Membrane.Pipeline
  alias Membrane.File

  @impl true
  def handle_init(%{input_path: input_path} = options) do
    elements = [
      in: %File.Source{location: input_path, chunk_size: 188},
      demuxer: Membrane.MPEG.TS.Demuxer
    ]

    links = [
      link(:in) |> to(:demuxer)
    ]

    spec = %ParentSpec{
      children: elements,
      links: links
    }

    {{:ok, spec: spec}, options}
  end

  @impl true
  def handle_notification(
        {:mpeg_ts_pmt_stream, %{stream_id: sid, stream_type: type}},
        _element,
        _context,
        state
      )
      when type in [:H264, :MPEG1_AUDIO] do
    link_to = link_from_stream_type(type)

    elements = [
      {link_to, %File.Sink{location: Map.get(state, link_to)}}
    ]

    links = [
      link(:demuxer)
      |> via_out(Pad.ref(:output, {:stream_id, sid}))
      |> to(link_to)
    ]

    spec = %ParentSpec{
      children: elements,
      links: links
    }

    {{:ok, spec: spec}, state}
  end

  def handle_notification(_notification, _element, _context, state) do
    {:ok, state}
  end

  defp link_from_stream_type(:H264), do: :video_out
  defp link_from_stream_type(:MPEG1_AUDIO), do: :audio_out
end
