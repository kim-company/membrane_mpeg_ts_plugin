defmodule Support.DynamicPipeline do
  @moduledoc false
  use Membrane.Pipeline
  require Logger

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
  def handle_notification({:mpeg_ts_stream_info, table}, _element, _context, state) do
    streams =
      table
      |> Enum.map(fn x -> x.streams end)
      |> Enum.reduce(%{}, fn x, acc -> Map.merge(x, acc) end)

    {video_stream_id, _} = Enum.find(streams, fn {_, %{stream_type: type}} -> type == :H264 end)

    {audio_stream_id, _} =
      Enum.find(streams, fn {_, %{stream_type: type}} -> type == :MPEG1_AUDIO end)

    elements = [
      audio_out: %File.Sink{location: state.audio_out},
      video_out: %File.Sink{location: state.video_out}
    ]

    links = [
      link(:demuxer)
      |> via_out(Pad.ref(:output, {:stream_id, video_stream_id}))
      |> to(:video_out),
      link(:demuxer)
      |> via_out(Pad.ref(:output, {:stream_id, audio_stream_id}))
      |> to(:audio_out)
    ]

    spec = %ParentSpec{
      children: elements,
      links: links
    }

    {{:ok, spec: spec}, state}
  end

  def handle_notification(_notification, _from, state), do: {:ok, state}
end
