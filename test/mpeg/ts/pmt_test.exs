defmodule MPEG.TS.PMTTest do
  use ExUnit.Case

  alias MPEG.TS.PMT
  alias MPEG.TS.Packet
  alias Support.Factory

  # TODO: add more exhaustive tests
  describe "Program Map Table table unmarshaler" do
    test "parses valid program map table with stream info but without program info" do
      assert {:ok, table} = PMT.unmarshal_table(Factory.pmt())

      assert %PMT{
               pcr_pid: 0x0100,
               program_info: [],
               streams: %{
                 256 => %{stream_type: :H264, stream_type_id: 0x1B},
                 257 => %{stream_type: :MPEG1_AUDIO, stream_type_id: 0x03}
               }
             } = table
    end

    test "returns an error when map table is malformed" do
      valid_pmt = Factory.pmt()
      garbage_size = byte_size(valid_pmt) - 3
      <<garbage::binary-size(garbage_size), _::binary>> = valid_pmt
      assert {:error, :invalid_data} = PMT.unmarshal_table(garbage)
    end
  end

  describe "PMT unmarshaler" do
    test "unmarshals valid packet" do
      {:ok, packet} = Packet.parse(Factory.pmt_packet())
      assert {:ok, %PMT{}} = PMT.unmarshal(packet.payload, packet.pusi)
    end
  end
end
