defmodule MPEG.TS.PMTTest do
  use ExUnit.Case

  alias MPEG.TS.PMT
  alias Support.Factory

  # TODO: add more exhaustive tests
  describe "Program Map Table unmarshaler" do
    test "parses valid program map table with stream info but without program info" do
      assert {:ok, table} = PMT.unmarshal(Factory.pmt())

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
      assert {:error, :invalid_data} = PMT.unmarshal(garbage)
    end
  end
end
