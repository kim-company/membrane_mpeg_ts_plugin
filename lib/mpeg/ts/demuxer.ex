defmodule MPEG.TS.Demuxer do
  alias MPEG.TS.Packet
  alias MPEG.TS.Unmarshaler
  alias MPEG.TS.PMT
  alias MPEG.TS.PartialPES

  require Logger

  @max_streams 20
  @max_stream_buffer_size 2_000_000

  @type t :: %__MODULE__{
          pmt: PMT.t(),
          streams: %{required(PMT.stream_id_t()) => RingBuffer.t()}
        }
  defstruct [:pmt, streams: %{}, buffered_bytes: <<>>, waiting_random_access_indicator: true]

  @type unmarshaler_t :: Unmarshaler.t() | {Unmarshaler.t(), PMT.stream_id_t()}

  @spec new(Keyword.t()) :: t()
  def new(_opts \\ []), do: %__MODULE__{}

  def push_buffer(state, buffer) do
    {ok, bytes_to_buffer} = parse_buffer(state.buffered_bytes <> buffer)

    state = push_packets(%__MODULE__{state | buffered_bytes: <<>>}, ok)
    %__MODULE__{state | buffered_bytes: bytes_to_buffer}
  end

  def push_packets(%__MODULE__{buffered_bytes: buf}, _) when byte_size(buf) > 0 do
    raise "Packets were pushed in the buffer while previous raw bytes are pending"
  end

  def push_packets(state = %__MODULE__{waiting_random_access_indicator: true}, packets) do
    rai = Enum.find_index(packets, fn x -> x.random_access_indicator end)

    if rai == nil do
      pmts = Enum.filter(packets, fn x -> PMT.is_unmarshable?(x.payload, x.pusi) end)

      Logger.warn(
        "Discarding #{length(packets) - length(pmts)}/#{length(packets)} packets while waiting for random access indicator",
        domain: __MODULE__
      )

      do_push_packets(state, pmts)
    else
      Logger.debug("Random access indicator found", domain: __MODULE__)

      pmts =
        packets
        |> Enum.slice(Range.new(0, rai - 1))
        |> Enum.filter(fn x -> PMT.is_unmarshable?(x.payload, x.pusi) end)

      packets = Enum.slice(packets, Range.new(rai, -1))
      push_packets(%{state | waiting_random_access_indicator: false}, pmts ++ packets)
    end
  end

  def push_packets(state, packets) do
    do_push_packets(state, packets)
  end

  defp do_push_packets(state, packets) do
    # filter out packets that are not PES ones, such as PMT tables, and make
    # them available. PES should be assigned to their respective stream.
    {pmt_packets, other_packets} =
      Enum.split_with(packets, fn x ->
        PMT.is_unmarshable?(x.payload, x.pusi)
      end)

    last_pmt =
      pmt_packets
      |> Enum.map(fn x ->
        case PMT.unmarshal(x.payload, x.pusi) do
          {:ok, table} ->
            table

          {:error, reason} ->
            raise ArgumentError,
                  "PMT did not manage to unmarshal packet: #{inspect(x)}, reason: #{inspect(reason)}"
        end
      end)
      |> List.last()

    pmt = if last_pmt != nil, do: last_pmt, else: state.pmt

    streams =
      other_packets
      |> Enum.filter(fn x -> PartialPES.is_unmarshable?(x.payload, x.pusi) end)
      |> Enum.reduce(state.streams, fn x = %Packet{pid: pid}, streams ->
        if Map.has_key?(streams, pid) or map_size(streams) < @max_streams do
          streams
          |> Map.get_lazy(pid, fn -> RingBuffer.new(@max_stream_buffer_size) end)
          |> RingBuffer.push(x)
          |> then(&Map.put(streams, pid, &1))
        else
          Logger.warn(
            "Discarding packet, stream buffer reached its maximum stream capacity",
            domain: __MODULE__,
            packet: inspect(x)
          )

          streams
        end
      end)

    %__MODULE__{state | pmt: pmt, streams: streams}
  end

  def get_pmt(%__MODULE__{pmt: pmt}), do: pmt

  def stream_size(state, stream_id) do
    case Map.get(state.streams, stream_id) do
      nil -> 0
      buf -> RingBuffer.size(buf)
    end
  end

  def take(state, stream_id, size \\ 1) do
    case Map.get(state.streams, stream_id) do
      nil ->
        {[], state}

      buf ->
        {items, buf} = RingBuffer.take(buf, size)
        packets = Enum.map(items, fn x -> unmarshal_partial_pes!(x) end)
        streams = Map.put(state.streams, stream_id, buf)
        {packets, %__MODULE__{state | streams: streams}}
    end
  end

  defp unmarshal_partial_pes!(packet) do
    case PartialPES.unmarshal(packet.payload, packet.pusi) do
      {:ok, pes} ->
        pes

      {:error, reason} ->
        raise ArgumentError,
              "MPEG-TS could not parse Partial PES packet: #{inspect(reason)}"
    end
  end

  defp parse_buffer(buffer) do
    {ok, err} =
      buffer
      |> Packet.parse_many()
      |> Enum.split_with(fn tuple -> elem(tuple, 0) == :ok end)

    # fail fast in case a critical error is encountered. If data becomes
    # mis-aligned this module should be responsible for fixing it.
    critical_err =
      Enum.find(err, fn
        {:error, :invalid_packet, _} -> true
        {:error, :invalid_data, _} -> true
        _ -> false
      end)

    if critical_err != nil do
      raise ArgumentError,
            "MPEG-TS unrecoverable parse error: #{inspect(critical_err, limit: :infinity)}"
    end

    to_buffer =
      err
      |> Enum.filter(fn
        {:error, :not_enough_data, _} -> true
        _ -> false
      end)
      |> Enum.map(fn {_, _, data} -> data end)
      |> Enum.reduce(<<>>, fn x, acc -> acc <> x end)

    ok = Enum.map(ok, fn {:ok, x} -> x end)

    {ok, to_buffer}
  end
end
