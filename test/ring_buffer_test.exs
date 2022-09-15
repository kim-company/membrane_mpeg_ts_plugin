defmodule RingBufferTest do
  use ExUnit.Case

  alias RingBuffer, as: RB

  test "empty behaviour" do
    buf = RB.new(10)
    assert {:empty, _} = RB.pop(buf)
  end

  test "overrides oldest values" do
    buf =
      2
      |> RB.new()
      |> RB.push(:a)
      |> RB.push(:b)
      |> RB.push(:c)

    assert {{:value, :b}, buf} = RB.pop(buf)
    assert {{:value, :c}, buf} = RB.pop(buf)
    assert {:empty, _buf} = RB.pop(buf)
  end

  test "take/2" do
    buf =
      5
      |> RB.new()
      |> RB.push(:a)
      |> RB.push(:b)
      |> RB.push(:c)

    {items, _buf} = RB.take(buf, 4)
    assert length(items) == 3

    {items, _buf} = RB.take(buf, 2)
    assert length(items) == 2
  end
end
