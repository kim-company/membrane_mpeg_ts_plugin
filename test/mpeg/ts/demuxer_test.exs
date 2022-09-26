defmodule MPEG.TS.DemuxerTest do
  use ExUnit.Case

  alias MPEG.TS.Demuxer
  alias MPEG.TS.PES
  alias MPEG.TS.Packet

  @all_packets_path "./test/fixtures/reference/all.ts"

  test "empty behaviour" do
    state = Demuxer.new()
    assert {[], state} = Demuxer.take(state, 2)
    assert {[], _state} = Demuxer.take(state, 2, 1_000)
  end

  test "finds PMT table" do
    packets =
      @all_packets_path
      |> File.read!()
      |> Packet.parse_valid()

    %Demuxer{pmt: pmt} =
      Demuxer.new()
      |> Demuxer.push_packets(packets)

    assert %MPEG.TS.PMT{
             pcr_pid: 256,
             program_info: [],
             streams: %{
               256 => %{stream_type: :H264, stream_type_id: 27},
               257 => %{stream_type: :MPEG1_AUDIO, stream_type_id: 3}
             }
           } == pmt
  end

  test "demuxes PES stream" do
    packets =
      @all_packets_path
      |> File.read!()
      |> Packet.parse_valid()

    state =
      Demuxer.new()
      |> Demuxer.push_packets(packets)

    assert {[%PES{}], _state} = Demuxer.take(state, 256, 1)
  end

  test "accepts raw bytes" do
    bytes =
      @all_packets_path
      |> File.read!()

    packets =
      bytes
      |> Packet.parse_valid()

    state_from_packets =
      Demuxer.new()
      |> Demuxer.push_packets(packets)

    state_from_bytes =
      Demuxer.new()
      |> Demuxer.push_buffer(bytes)

    assert state_from_bytes == state_from_packets
  end

  test "works with partial data" do
    one_shot =
      Demuxer.new()
      |> Demuxer.push_buffer(File.read!(@all_packets_path))
      |> Demuxer.end_of_stream()

    chunked =
      @all_packets_path
      |> File.open!([:binary])
      |> IO.binstream(512)
      |> Enum.reduce(Demuxer.new(), fn buf, d -> Demuxer.push_buffer(d, buf) end)
      |> Demuxer.end_of_stream()

    stream_id = 256
    amount = 20_000
    {packets_chunked, _state} = Demuxer.take(chunked, stream_id, amount)
    {packets_one_shot, _state} = Demuxer.take(one_shot, stream_id, amount)

    assert length(packets_chunked) == length(packets_one_shot)

    packets_chunked
    |> Enum.zip(packets_one_shot)
    |> Enum.with_index()
    |> Enum.each(fn {{chunked, one_shot}, index} ->
      assert chunked == one_shot,
             "packet #{index}/#{length(packets_chunked) - 1}:\n\tone_shot=#{inspect(one_shot, binaries: :as_strings)}\n\tchunked=#{inspect(chunked, binaries: :as_strings)}"
    end)
  end

  test "allows to take one packet at a time" do
    get_packets = fn step ->
      @all_packets_path
      |> File.read!()
      |> then(&Demuxer.push_buffer(Demuxer.new(), &1))
      |> consume_demuxer(256, step, [])
    end

    assert get_packets.(1000) == get_packets.(1)
  end

  defp consume_demuxer(state, stream_id, step, acc) do
    {packets, state} = Demuxer.take(state, stream_id, step)

    if length(packets) == 0 do
      acc
    else
      consume_demuxer(state, stream_id, step, acc ++ packets)
    end
  end
end
