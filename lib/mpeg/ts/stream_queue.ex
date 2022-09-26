defmodule MPEG.TS.StreamQueue do
  @moduledoc """
  This module's responsibility is to reduce a stream of TS packets packets into
  an ordered queue of PES ones. Accepts only packets belonging to the same
  elementary stream. This module is in support of the demuxer and acts as a
  PartialPES depayloader.
  """

  alias MPEG.TS.Packet
  alias MPEG.TS.PartialPES
  alias MPEG.TS.PES

  @derive Inspect
  defstruct [:stream_id, :partials, :ready]

  def new(stream_id) do
    %__MODULE__{stream_id: stream_id, partials: Qex.new(), ready: Qex.new()}
  end

  def push_es_packets(state = %__MODULE__{partials: partials, ready: ready}, packets) do
    {complete_chunks, [partials]} =
      partials
      |> Qex.join(Qex.new(packets))
      |> Enum.map(fn x = %Packet{pid: pid} ->
        if pid != state.stream_id do
          raise ArgumentError,
                "#{inspect(state)} received a packet belonging to stream #{inspect(pid)}"
        end

        x
      end)
      |> Enum.reduce([], fn
        x = %Packet{pusi: true}, acc ->
          [[x] | acc]

        x = %Packet{pusi: false}, [head | rest] ->
          [[x | head] | rest]
      end)
      |> Enum.reverse()
      |> Enum.map(&Enum.reverse/1)
      |> Enum.split(-1)

    partials = Qex.new(partials)

    pes_packets =
      complete_chunks
      |> Enum.map(&reduce_pes_packet(&1))
      |> Enum.filter(fn x -> x != nil end)

    %__MODULE__{state | partials: partials, ready: Enum.into(pes_packets, ready)}
  end

  def end_of_stream(state = %__MODULE__{partials: partials, ready: ready}) do
    ready =
      partials
      |> Enum.into([])
      |> reduce_pes_packet()
      |> List.wrap()
      |> Enum.filter(fn x -> x != nil end)
      |> Enum.into(ready)

    %__MODULE__{state | partials: Qex.new(), ready: ready}
  end

  def take(state = %__MODULE__{ready: queue}, amount) do
    {items, queue} = take_from_queue(queue, amount, [])
    {items, %__MODULE__{state | ready: queue}}
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

  defp reduce_pes_packet(packets) do
    packets =
      packets
      |> Enum.filter(fn x -> PartialPES.is_unmarshable?(x.payload, x.pusi) end)
      |> Enum.map(fn x -> unmarshal_partial_pes!(x) end)

    leader = List.first(packets)

    if leader != nil do
      data =
        packets
        |> Enum.map(fn %PartialPES{data: data} -> data end)
        |> Enum.join()

      %PES{
        data: data,
        stream_id: leader.stream_id,
        pts: leader.pts,
        dts: leader.dts
      }
    else
      nil
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
end
