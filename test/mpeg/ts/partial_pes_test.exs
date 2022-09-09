defmodule MPEG.TS.PartialPESTest do
  use ExUnit.Case

  alias MPEG.TS.PartialPES
  alias Support.Factory

  describe "Packetized Elementary Stream unmarshaler" do
    test "unmarshals valid payload" do
      payload = Factory.pes_payload()
      assert {:ok, %PartialPES{data: _}} = PartialPES.unmarshal(payload, true)
    end
  end
end
