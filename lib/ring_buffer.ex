defmodule RingBuffer do
  require Logger

  @type t :: %__MODULE__{max_size: pos_integer(), size: non_neg_integer(), queue: Qex.t()}
  defstruct [:max_size, size: 0, queue: Qex.new()]

  def new(size) when size > 0, do: %__MODULE__{max_size: size}

  def size(%__MODULE__{size: size}), do: size

  def push(%__MODULE__{size: size, max_size: max}, _item) when size > max do
    raise ArgumentError,
          "RingBuffer exceeded its maximum size: have #{inspect(size)}, max was #{inspect(max)}"
  end

  def push(rb = %__MODULE__{size: size, max_size: max, queue: q}, item) when size == max do
    {_, q} = Qex.pop(q)
    Logger.warn("Ring buffer: discarding packet #{inspect(q)}")
    push(%__MODULE__{rb | queue: q, size: size - 1}, item)
  end

  def push(rb = %__MODULE__{queue: q, size: size}, item) do
    q = Qex.push(q, item)
    %__MODULE__{rb | queue: q, size: size + 1}
  end

  def pop(rb = %__MODULE__{size: size}) when size <= 0, do: {:empty, rb}

  def pop(rb = %__MODULE__{queue: q, size: size}) do
    {item, q} = Qex.pop(q)
    {item, %__MODULE__{rb | queue: q, size: size - 1}}
  end

  def take(rb = %__MODULE__{queue: q, size: size}, amount) do
    {items, q} = take_from_queue(q, amount, [])
    {items, %__MODULE__{rb | queue: q, size: size - length(items)}}
  end

  defp take_from_queue(queue, 0, items) do
    {Enum.reverse(items), queue}
  end

  defp take_from_queue(queue, n, items) do
    case Qex.pop(queue) do
      {:empty, queue} ->
        take_from_queue(queue, 0, items)

      {{:value, item}, queue} ->
        take_from_queue(queue, n - 1, [item | items])
    end
  end
end
