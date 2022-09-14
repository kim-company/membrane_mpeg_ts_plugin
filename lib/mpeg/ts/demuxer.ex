defmodule MPEG.TS.Demuxer do
  alias MPEG.TS.Packet
  alias MPEG.TS.Unmarshaler
  alias MPEG.TS.PMT

  @type t :: %__MODULE__{unmarshalers: [MPEG.TS.Unmarshaler.t()], queue: Qex.t()}
  defstruct unmarshalers: [], queue: Qex.new()

  @type unmarshaler_t :: Unmarshaler.t() | {Unmarshaler.t(), PMT.stream_id_t()}

  @spec new([unmarshaler_t()]) :: t()
  def new(unmarshalers \\ []) do
    Enum.reduce(unmarshalers, %__MODULE__{}, fn x, state ->
      add_unmarshaler(state, x)
    end)
  end

  def add_unmarshaler(state, x = {_unmarshaler, _stream_id}) do
    %__MODULE__{state | unmarshalers: [x | state.unmarshalers]}
  end

  def add_unmarshaler(state, unmarshaler) do
    add_unmarshaler(state, {unmarshaler, nil})
  end

  @doc """
  Enqueues the packets it can unmarshal and hence demultiplex. Returns the new
  state and the packets that not unmarshabled with the provided list of
  unmarshalers.
  """
  @spec push(t(), [Packet.t()]) :: {t(), [{Packet.t(), any()}]}
  def push(state = %__MODULE__{queue: queue}, packets) do
    valid_invalid =
      packets
      |> Enum.map(&unmarshal_packet(state, &1))
      |> Enum.group_by(fn
        {:error, _, _} -> :invalid
        {:ok, _, _} -> :valid
      end)

    valid = Map.get(valid_invalid, :valid, [])
    invalid = Map.get(valid_invalid, :invalid, [])

    queue =
      Enum.reduce(valid, queue, fn {:ok, module, value}, queue ->
        Qex.push(queue, {module, value})
      end)

    rejected = Enum.map(invalid, fn {:error, packet, reason} -> {packet, reason} end)

    {%{state | queue: queue}, rejected}
  end

  @spec pop(t()) :: {t, :empty} | {t, {Unmarshaler.t(), Unmarshaler.result_t()}}
  def pop(state = %{queue: queue}) do
    case Qex.pop(queue) do
      {:empty, _queue} -> {state, :empty}
      {{:value, item}, queue} -> {%{state | queue: queue}, item}
    end
  end

  @spec flush(t()) :: {t(), [{Unmarshaler.t(), Unmarshaler.result_t()}]}
  def flush(state = %{queue: queue}) do
    {%{state | queue: Qex.new()}, Enum.to_list(queue)}
  end

  defp unmarshal_packet(state, pkt) do
    wrap_nil = fn
      nil -> {nil, nil}
      unm -> unm
    end

    state.unmarshalers
    |> Enum.find(fn {x, _} -> x.is_unmarshable?(pkt.payload, pkt.is_unit_start) end)
    |> wrap_nil.()
    |> unmarshal_packet_with_unmarshaler(pkt)
  end

  defp unmarshal_packet_with_unmarshaler({nil, _nil}, pkt) do
    {:error, pkt, :no_unmarshaler}
  end

  defp unmarshal_packet_with_unmarshaler({unm, sid}, pkt = %Packet{pid: pid})
       when sid == nil or sid == pid do
    case unm.unmarshal(pkt.payload, pkt.is_unit_start) do
      {:error, reason} ->
        {:error, pkt, reason}

      {:ok, result} when sid == nil ->
        {:ok, unm, result}

      {:ok, result} ->
        {:ok, {unm, sid}, result}
    end
  end

  defp unmarshal_packet_with_unmarshaler({_unm, _stream_id}, pkt) do
    {:error, pkt, :unmatched_stream_id}
  end
end
