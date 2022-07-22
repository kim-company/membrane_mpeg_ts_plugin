defmodule MPEG.TS.PATTest do
  use ExUnit.Case

  alias MPEG.TS.PAT
  alias Support.Factory

  describe "Program association table parser" do
    test "parses valid packet" do
      assert {:ok, %{1 => 4096}} = PAT.unmarshal_table(Factory.pat())
    end

    test "returns an error when data is not valid" do
      assert {:error, :invalid_data} = PAT.unmarshal_table(<<123, 32, 22, 121, 33>>)
    end
  end
end
