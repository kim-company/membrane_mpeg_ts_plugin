defmodule MPEG.TS.Depayloader do
  alias MPEG.TS.Packet
  alias MPEG.TS.Unmarshaler

  defmodule Buffer do
    @type t :: %__MODULE__{queue: Qex.t()}
    defstruct queue: Qex.new()

    def new(initial \\ []) do
      %__MODULE__{
        queue: Qex.new(initial)
      }
    end

    def push(buffer, packet) do
      %__MODULE__{buffer | queue: Qex.push(buffer.queue, packet)}
    end

    def is_unmarshable?(buffer, unmarshaler) do
      buffer.queue
      |> Enum.map(fn x -> x.payload end)
      |> Enum.map(fn x -> unmarshaler.is_unmarshable?(x) end)
      |> Enum.any?()
    end

    def pop(buffer, unmarshaler) do
      {acc, done} =
        buffer.queue
        |> Enum.reduce({[], []}, fn x, {acc, done} ->
          acc = acc ++ [x]

          data =
            acc
            |> Enum.map(fn x -> x.payload end)
            |> Enum.join()

          case unmarshaler.unmarshal(data) do
            {:ok, value} -> {[], [value | done]}
            {:error, :not_enough_data} -> {acc, done}
            # NOTE: silently dropping packets!
            {:error, _other} -> {[], done}
          end
        end)

      buffer = %__MODULE__{buffer | queue: Qex.new(acc)}
      {:ok, buffer, Enum.reverse(done)}
    end
  end

  @type error_t :: {:error, any()}

  @opaque t :: %__MODULE__{
            buffers: %{required(Packet.pid_t()) => Buffer.t()}
          }
  defstruct buffers: %{}

  @spec new(Keyword.t()) :: t
  def new(_opts \\ []) do
    %__MODULE__{}
  end

  @spec load(t, [Packet.t()]) :: t
  def load(state, packets) do
    packets
    |> Enum.reduce(state, fn x, state = %__MODULE__{buffers: buffers} ->
      buffers =
        Map.update(buffers, x.pid, Buffer.new([x]), fn queue ->
          Buffer.push(queue, x)
        end)

      %__MODULE__{state | buffers: buffers}
    end)
  end

  @spec find_unmarshable(t, Unmarshaler.t()) :: [Packet.pid_t()]
  def find_unmarshable(%__MODULE__{buffers: buffers}, unmarshaler) do
    buffers
    |> Enum.map(fn {k, v} -> {k, Buffer.is_unmarshable?(v, unmarshaler)} end)
    |> Enum.filter(fn {_k, v} -> v == true end)
    |> Enum.map(fn {k, _v} -> k end)
  end

  @spec pop(t, Packet.pid_t(), Unmarshaler.t()) ::
          {:ok, t, [Unmarshaler.result_t()]} | {:error, any()}
  def pop(state, pid, unmarshaler) do
    with {:ok, buffer} <- Map.fetch(state.buffers, pid),
         {:ok, buffer, unmarshaled} <- Buffer.pop(buffer, unmarshaler) do
      state = %__MODULE__{buffers: Map.put(state.buffers, pid, buffer)}
      {:ok, state, unmarshaled}
    else
      :error -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end
end
