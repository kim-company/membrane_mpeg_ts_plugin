defmodule MPEG.TS.PATTest do
  use ExUnit.Case

  alias MPEG.TS.PAT

  describe "Program association table parser" do
    test "parses valid packet" do
      pat = <<0x00, 0x01, 0xF0, 0x00>>
      assert {:ok, %{1 => 4096}} = PAT.unmarshal(pat)
    end

    test "returns an error when data is not valid" do
      assert {:error, :malformed_data} = PAT.unmarshal(<<123, 32, 22, 121, 33>>)
    end
  end
end
