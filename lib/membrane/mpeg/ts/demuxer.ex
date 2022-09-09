defmodule Membrane.MPEG.TS.Demuxer do
  @moduledoc """
  Demuxes MPEG TS stream.

  After transition into playing state, this element will wait for
  [Program Association Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PAT) and
  [Program Mapping Table](https://en.wikipedia.org/wiki/MPEG_transport_stream#PMT).
  Upon succesfful parsing of those tables it will send a message to the pipeline in format
  `{:mpeg_ts_stream_info, configuration}`, where configuration contains data read from tables.

  Configuration sent by element to pipeline should have following shape
  ```
  %{
    program_id => %Membrane.MPEG.TS.ProgramMapTable{
      pcr_pid: 256,
      program_info: [],
      streams: %{
        256 => %{stream_type: :H264, stream_type_id: 27},
        257 => %{stream_type: :MPEG1_AUDIO, stream_type_id: 3}
      }
    }
  }
  ```
  """
  use Membrane.Filter

  def_input_pad(:input, caps: :any, demand_unit: :buffers)
  def_output_pad(:output, availability: :on_request, caps: :any, demand_unit: :buffers)

  @impl true
  def handle_init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_demand(_pad, _size, _unit, _ctx, state) do
    {:ok, state}
  end

  @impl true
  def handle_process(:input, _buffer, _ctx, state) do
    {:ok, state}
  end
end
