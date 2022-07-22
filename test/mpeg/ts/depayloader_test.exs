defmodule MPEG.TS.DepayloaderTest do
  use ExUnit.Case

  alias MPEG.TS.Depayloader
  alias MPEG.TS.PMT
  alias MPEG.TS.Packet

  @all_packets_path "./test/fixtures/all_packets.ts"

  test "finds PMT table" do
    packets =
      @all_packets_path
      |> File.read!()
      |> Packet.parse_many_ok()

    state =
      Depayloader.new()
      |> Depayloader.load(packets)

    assert [stream_id] = Depayloader.find_unmarshable(state, PMT)
    assert {:ok, _state, [%PMT{}]} = Depayloader.pop(state, stream_id, PMT)
  end
end
