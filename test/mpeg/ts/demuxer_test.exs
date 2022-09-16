defmodule MPEG.TS.DemuxerTest do
  use ExUnit.Case

  alias MPEG.TS.Demuxer
  alias MPEG.TS.PartialPES
  alias MPEG.TS.Packet

  @all_packets_path "./test/fixtures/reference-1/all.ts"

  test "empty behaviour" do
    state = Demuxer.new()
    assert {[], state} = Demuxer.take_from_stream(state, 2)
    assert {[], _state} = Demuxer.take_from_stream(state, 2, 1_000)
  end

  test "finds PMT table" do
    packets =
      @all_packets_path
      |> File.read!()
      |> Packet.parse_valid()

    state =
      Demuxer.new()
      |> Demuxer.push_packets(packets)

    assert Demuxer.has_pmt?(state)

    assert [
             %MPEG.TS.PMT{
               pcr_pid: 256,
               program_info: [],
               streams: %{
                 256 => %{stream_type: :H264, stream_type_id: 27},
                 257 => %{stream_type: :MPEG1_AUDIO, stream_type_id: 3}
               }
             }
           ] == Demuxer.take_pmts(state)
  end

  test "demuxes PES stream" do
    packets =
      @all_packets_path
      |> File.read!()
      |> Packet.parse_valid()

    state =
      Demuxer.new()
      |> Demuxer.push_packets(packets)

    assert {[{unm = PartialPES, packet}], _state} = Demuxer.take_from_stream(state, 256, 1)
    assert {:ok, _pes} = unm.unmarshal(packet.payload, packet.is_unit_start)
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
end
