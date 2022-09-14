defmodule MPEG.TS.DemuxerTest do
  use ExUnit.Case

  alias MPEG.TS.Demuxer
  alias MPEG.TS.{PartialPES, PMT}
  alias MPEG.TS.Packet

  @all_packets_path "./test/fixtures/all_packets.ts"

  test "does not break if empty" do
    state = Demuxer.new([PMT])
    assert {_, :empty} = Demuxer.pop(state)
  end

  test "finds PMT table" do
    packets =
      @all_packets_path
      |> File.read!()
      |> Packet.parse_valid()

    {state, _rejected} =
      [PMT]
      |> Demuxer.new()
      |> Demuxer.push(packets)

    assert {_state,
            {PMT,
             %MPEG.TS.PMT{
               pcr_pid: 256,
               program_info: [],
               streams: %{
                 256 => %{stream_type: :H264, stream_type_id: 27},
                 257 => %{stream_type: :MPEG1_AUDIO, stream_type_id: 3}
               }
             }}} = Demuxer.pop(state)
  end

  test "demuxes PES stream" do
    packets =
      @all_packets_path
      |> File.read!()
      |> Packet.parse_valid()

    {state, _rejected} =
      [{PartialPES, 256}]
      |> Demuxer.new()
      |> Demuxer.push(packets)

    assert {_state, {{PartialPES, 256}, _pes}} = Demuxer.pop(state)
  end
end
