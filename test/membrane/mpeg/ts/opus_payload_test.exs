defmodule Membrane.MPEG.TS.OpusPayloadTest do
  use ExUnit.Case, async: true

  alias Membrane.MPEG.TS.OpusPayload

  test "packetize and depacketize round trip" do
    payload = <<0xF0, 0x01, 0x02, 0x03, 0x04>>
    ts_payload = OpusPayload.packetize(payload)

    assert <<0x7F, _flags, _rest::binary>> = ts_payload
    assert {:ok, ^payload} = OpusPayload.depacketize(ts_payload)
  end

  test "packetize handles 255-byte payloads" do
    payload = :binary.copy(<<0xAA>>, 255)
    ts_payload = OpusPayload.packetize(payload)
    assert {:ok, ^payload} = OpusPayload.depacketize(ts_payload)
  end
end
