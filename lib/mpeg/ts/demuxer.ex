defmodule MPEG.TS.Demuxer do
  @moduledoc """
  Responsible for demultiplexing a stream of MPEG.TS.Packet into the elemetary
  streams listed in the stream's Program Map Table. Does not yet handle PAT.
  """
  alias MPEG.TS.Packet
  alias MPEG.TS.PMT
  alias MPEG.TS.StreamQueue

  require Logger

  @type t :: %__MODULE__{
          pmt: PMT.t(),
          demuxed_queues: %{required(PMT.stream_id_t()) => StreamBuffer.t()},
          packet_filter: (PMT.stream_id_t() -> Boolean.t()),
          buffered_bytes: binary()
        }

  defstruct [
    :pmt,
    :packet_filter,
    demuxed_queues: %{},
    buffered_bytes: <<>>,
    waiting_random_access_indicator: true
  ]

  @spec new(Keyword.t()) :: t()
  def new(_opts \\ []), do: %__MODULE__{packet_filter: fn _ -> true end}

  def push_buffer(state, buffer) do
    {ok, bytes_to_buffer} = parse_buffer(state.buffered_bytes <> buffer)

    state = push_packets(%__MODULE__{state | buffered_bytes: <<>>}, ok)
    %__MODULE__{state | buffered_bytes: bytes_to_buffer}
  end

  def push_packets(%__MODULE__{buffered_bytes: buf}, _) when byte_size(buf) > 0 do
    raise "Packets were pushed in the buffer while previous raw bytes are pending"
  end

  def push_packets(state, packets) do
    {pmt_packets, other_packets} =
      Enum.split_with(packets, fn x ->
        PMT.is_unmarshable?(x.payload, x.pusi)
      end)

    state
    |> push_pmt_packets(pmt_packets)
    |> push_es_packets(other_packets)
  end

  def end_of_stream(state = %__MODULE__{demuxed_queues: queues}) do
    queues =
      queues
      |> Enum.map(fn {stream_id, queue} -> {stream_id, StreamQueue.end_of_stream(queue)} end)
      |> Enum.into(%{})

    %__MODULE__{state | demuxed_queues: queues}
  end

  def take(state = %__MODULE__{demuxed_queues: queues}, stream_id, size \\ 1) do
    case Map.get(queues, stream_id) do
      nil ->
        {[], state}

      queue ->
        {packets, queue} = StreamQueue.take(queue, size)
        {packets, %__MODULE__{state | demuxed_queues: Map.put(queues, stream_id, queue)}}
    end
  end

  def size(%__MODULE__{demuxed_queues: queues}, stream_id) do
    case Map.get(queues, stream_id) do
      nil -> 0
      %StreamQueue{ready: ready} -> Enum.count(ready)
    end
  end

  defp push_pmt_packets(state, pmt_packets) do
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
    %__MODULE__{state | pmt: pmt}
  end

  defp push_es_packets(
         state = %__MODULE__{waiting_random_access_indicator: true},
         packets
       ) do
    rai = Enum.find_index(packets, fn x -> x.random_access_indicator end)

    if rai == nil do
      discard_count = length(packets)

      if discard_count > 0 do
        Logger.warn(
          "Discarding #{discard_count} packets while waiting for random access indicator",
          domain: __MODULE__
        )
      end

      state
    else
      Logger.debug("Random access indicator found", domain: __MODULE__)

      packets = Enum.slice(packets, Range.new(rai, -1))
      push_es_packets(%{state | waiting_random_access_indicator: false}, packets)
    end
  end

  defp push_es_packets(
         state = %__MODULE__{demuxed_queues: queues, packet_filter: filter},
         packets
       ) do
    queues =
      packets
      |> Enum.filter(fn %Packet{pid: pid} -> filter.(pid) end)
      |> Enum.group_by(fn %Packet{pid: pid} -> pid end)
      |> Enum.map(fn {pid, packets} ->
        # WARNING: we're not limiting the number of streams we're tracking.
        queue =
          queues
          |> Map.get_lazy(pid, fn -> StreamQueue.new(pid) end)
          |> StreamQueue.push_es_packets(packets)

        {pid, queue}
      end)
      |> Enum.reduce(queues, fn {pid, queue}, queues ->
        Map.put(queues, pid, queue)
      end)

    %__MODULE__{state | demuxed_queues: queues}
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
