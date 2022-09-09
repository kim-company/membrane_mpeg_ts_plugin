defmodule MPEG.TS.PacketTest do
  use ExUnit.Case

  alias MPEG.TS.Packet
  alias Support.Factory

  describe "MPEG TS Packet parser" do
    test "handles valid PAT packet" do
      raw_data = Factory.pat_packet()
      assert {:ok, %Packet{payload: data}} = Packet.parse(raw_data)
      assert byte_size(data) > 0
    end

    test "handles valid PMT packet" do
      raw_data = Factory.pmt_packet()
      assert {:ok, %Packet{payload: data}} = Packet.parse(raw_data)
      assert byte_size(data) > 0
    end

    test "asks for more data if packet is not complete but valid" do
      <<partial::160-binary, _rest::binary>> = Factory.pat_packet()
      assert {:error, :not_enough_data} = Packet.parse(partial)
    end

    test "successfully parse a valid PartialPES packet" do
      raw_data = Factory.data_packet_video()
      assert byte_size(raw_data) == Packet.packet_size()
      assert {:ok, %Packet{payload: data}} = Packet.parse(raw_data)
      assert byte_size(data) > 0
    end

    test "fails when garbage is provided" do
      data = "garbagio"
      assert {:error, :invalid_data} = Packet.parse(data)
    end
  end
end
