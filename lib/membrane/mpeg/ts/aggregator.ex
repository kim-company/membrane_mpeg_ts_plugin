defmodule Membrane.MPEG.TS.Aggregator do
  use Membrane.Filter

  def_input_pad(:input,
    accepted_format: Membrane.RemoteStream
  )

  def_output_pad(:output,
    accepted_format: Membrane.RemoteStream
  )

  def_options(
    min_duration: [
      spec: Membrane.Time.t(),
      default: Membrane.Time.seconds(2),
      description: """
      After accumulating enough buffers to cover this duration, the output buffer
      will be finalized as soon as the next PUSI unit is found.
      """
    ]
  )

  @impl true
  def handle_init(_ctx, opts) do
    state = %{
      min_duration: opts.min_duration,
      acc: [],
      pts: nil,
      dts: nil
    }

    {[], state}
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state = %{acc: []}) do
    {[end_of_stream: :output], state}
  end

  def handle_end_of_stream(:input, _ctx, state) do
    {buffer, state} = finalize_segment(state)
    {[buffer: {:output, buffer}, end_of_stream: :output], state}
  end

  @impl true
  def handle_buffer(
        :input,
        buffer = %{metadata: %{pusi: true}},
        _ctx,
        state = %{acc: []}
      ) do
    {[], init_segment(state, buffer)}
  end

  def handle_buffer(:input, buffer = %{metadata: %{pusi: true}}, _ctx, state) do
    duration = buffer.pts - state.pts

    if duration >= state.min_duration do
      {output, state} = finalize_segment(state, duration)
      state = init_segment(state, buffer)
      {[buffer: {:output, output}], state}
    else
      state = add_frame(state, buffer)
      {[], state}
    end
  end

  def handle_buffer(:input, buffer, _ctx, state = %{acc: []}) do
    Membrane.Logger.warning(
      "Received buffer before PUSI indicator -- dropping (pts: #{buffer.pts})"
    )

    # We're waiting until the first buffer with PUSI indicator set before
    # starting a new segment.
    {[], state}
  end

  def handle_buffer(:input, buffer, _ctx, state) do
    state = add_frame(state, buffer)
    {[], state}
  end

  defp init_segment(state, buffer) do
    state
    |> update_in([:acc], fn acc -> [buffer | acc] end)
    |> put_in([:pts], buffer.pts)
    |> put_in([:dts], buffer.dts)
  end

  defp add_frame(state, buffer) do
    state
    |> update_in([:acc], fn acc -> [buffer | acc] end)
  end

  defp finalize_segment(state) do
    buffers = Enum.reverse(state.acc)
    duration = List.last(buffers).pts - state.pts
    finalize_segment(state, duration)
  end

  defp finalize_segment(state, duration) do
    buffers = Enum.reverse(state.acc)

    units = Enum.flat_map(buffers, fn x -> Map.get(x.metadata, :units, []) end)

    payload =
      buffers
      |> Enum.map(fn x -> x.payload end)
      |> Enum.join(<<>>)

    buffer = %Membrane.Buffer{
      payload: payload,
      pts: state.pts,
      dts: state.dts,
      metadata: %{pusi: true, duration: duration, units: units}
    }

    state =
      state
      |> put_in([:acc], [])
      |> put_in([:pts], nil)
      |> put_in([:dts], nil)

    {buffer, state}
  end
end
