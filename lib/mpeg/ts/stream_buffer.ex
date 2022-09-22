defmodule MPEG.TS.StreamBuffer do
  alias MPEG.TS.Packet
  require Logger

  @max_stream_buffer_size 2_000_000

  @type t :: %__MODULE__{
          buf: RingBuffer.t(),
          next_continuity: non_neg_integer()
        }

  defstruct buf: RingBuffer.new(@max_stream_buffer_size), next_continuity: 0

  def push(state, x = %Packet{is_unit_start: is_start, continuity_counter: counter})
      when is_start or counter == 0 do
    buf = RingBuffer.push(state.buf, x)
    %__MODULE__{state | buf: buf, next_continuity: counter + 1}
  end

  def push(state = %__MODULE__{next_continuity: next}, x = %Packet{continuity_counter: counter})
      when next == counter do
    buf = RingBuffer.push(state.buf, x)
    %__MODULE__{state | buf: buf, next_continuity: x.continuity_counter + 1}
  end

  def push(state, x) do
    # TODO: Telemetry event maybe?
    Logger.warn(
      "MPEG-TS StreamBuffer: partial packet #{inspect(x)} was received before its unit start"
    )

    state
  end

  def size(%__MODULE__{buf: %RingBuffer{size: size}}), do: size

  def take(state = %__MODULE__{buf: buf}, size) do
    {items, buf} = RingBuffer.take(buf, size)
    {items, %__MODULE__{state | buf: buf}}
  end
end
