defmodule MPEG.TS.Demuxer do
  alias MPEG.TS.Packet
  alias MPEG.TS.Unmarshaler
  alias MPEG.TS.PMT
  alias MPEG.TS.PartialPES
  alias MPEG.TS.StreamBuffer

  require Logger

  # This equals to 75.200MB of buffered data at maximum (@max_streams *
  # @max_stream_buffer_size)
  @max_streams 20

  @type t :: %__MODULE__{
          pmt: PMT.t(),
          streams: %{required(PMT.stream_id_t()) => StreamBuffer.t()}
        }
  defstruct [:pmt, streams: %{}, buffered_bytes: <<>>]

  @type unmarshaler_t :: Unmarshaler.t() | {Unmarshaler.t(), PMT.stream_id_t()}

  @spec new(Keyword.t()) :: t()
  def new(_opts \\ []), do: %__MODULE__{}

  def push_buffer(state, buffer) do
    {ok, bytes_to_buffer} = parse_buffer(state.buffered_bytes <> buffer)

    state = push_packets(%__MODULE__{state | buffered_bytes: <<>>}, ok)
    %__MODULE__{state | buffered_bytes: bytes_to_buffer}
  end

  def push_packets(%__MODULE__{buffered_bytes: buf}, _) when byte_size(buf) > 0 do
    raise ArgumentError, "tried pushing packets when pending raw bytes are buffered"
  end

  def push_packets(state, packets) do
    # filter out packets that are not PES ones, such as PMT tables, and make
    # them available. PES should be assigned to their respective stream.
    {pmt_packets, other_packets} =
      Enum.split_with(packets, fn x ->
        PMT.is_unmarshable?(x.payload, x.is_unit_start)
      end)

    last_pmt =
      pmt_packets
      |> Enum.map(fn x ->
        case PMT.unmarshal(x.payload, x.is_unit_start) do
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
      |> Enum.filter(fn x -> PartialPES.is_unmarshable?(x.payload, x.is_unit_start) end)
      |> Enum.reduce(state.streams, fn x = %Packet{pid: pid}, streams ->
        if Map.has_key?(streams, pid) or map_size(streams) < @max_streams do
          streams
          |> Map.get_lazy(pid, fn -> %StreamBuffer{} end)
          |> StreamBuffer.push(x)
          |> then(&Map.put(streams, pid, &1))
        else
          Logger.warn(
            "discarding packet #{inspect(x)}, stream buffer reached its maximum stream capacity"
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
      stream_buffer -> StreamBuffer.size(stream_buffer)
    end
  end

  def take(state, stream_id, size \\ 1) do
    case Map.get(state.streams, stream_id) do
      nil ->
        {[], state}

      buf ->
        {items, buf} = StreamBuffer.take(buf, size)
        packets = Enum.map(items, fn x -> unmarshal_partial_pes!(x) end)
        streams = Map.put(state.streams, stream_id, buf)
        {packets, %__MODULE__{state | streams: streams}}
    end
  end

  defp unmarshal_partial_pes!(packet) do
    case PartialPES.unmarshal(packet.payload, packet.is_unit_start) do
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
