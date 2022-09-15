defmodule MPEG.TS.Demuxer do
  alias MPEG.TS.Packet
  alias MPEG.TS.Unmarshaler
  alias MPEG.TS.PMT
  alias MPEG.TS.PartialPES

  require Logger

  # This equals to 752KB of buffered data at maximum (@max_streams *
  # @max_stream_buffer_size)
  @max_streams 20
  @max_stream_buffer_size 200
  @max_tables 10

  @type t :: %__MODULE__{
          known_tables: [PMT.t()],
          streams: %{required(PMT.stream_id_t()) => RingBuffer.t()}
        }
  defstruct known_tables: [], streams: %{}, buffered_bytes: <<>>

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

    new_tables =
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
      |> Enum.concat(state.known_tables)

    tables =
      if length(new_tables) > @max_tables do
        discard = @max_tables - length(new_tables)

        Logger.warn(
          "discarding #{inspect(discard)} old tables as the stream buffer reached its maxium table capacity"
        )

        Enum.take(new_tables, @max_tables)
      else
        new_tables
      end

    streams =
      other_packets
      |> Enum.filter(fn x -> PartialPES.is_unmarshable?(x.payload, x.is_unit_start) end)
      |> Enum.reduce(state.streams, fn x = %Packet{pid: pid}, streams ->
        if Map.has_key?(streams, pid) or map_size(streams) < @max_streams do
          streams
          |> Map.get_lazy(pid, fn -> RingBuffer.new(@max_stream_buffer_size) end)
          |> RingBuffer.push({PartialPES, x})
          |> then(&Map.put(streams, pid, &1))
        else
          Logger.warn(
            "discarding packet #{inspect(x)}, stream buffer reached its maximum stream capacity"
          )

          streams
        end
      end)

    %__MODULE__{state | known_tables: tables, streams: streams}
  end

  def has_pmt?(%__MODULE__{known_tables: []}), do: false
  def has_pmt?(_state), do: true

  def take_from_stream(state, stream_id, size \\ 1) do
    case Map.get(state.streams, stream_id) do
      nil ->
        {[], state}

      rb ->
        {items, rb} = RingBuffer.take(rb, size)
        streams = Map.put(state.streams, stream_id, rb)
        {items, %__MODULE__{state | streams: streams}}
    end
  end

  def take_pmts(%__MODULE__{known_tables: tables}) do
    tables
    |> Enum.filter(fn
      %PMT{} -> true
      _ -> true
    end)
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
        {:error, :invalid_packet} -> true
        {:error, :invalid_data} -> true
        _ -> false
      end)

    if critical_err != nil do
      raise ArgumentError, "MPEG-TS unrecoverable parse error: #{inspect(elem(critical_err, 1))}"
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
