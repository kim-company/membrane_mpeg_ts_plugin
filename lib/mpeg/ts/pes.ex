defmodule MPEG.TS.PES do
  @derive {Inspect, only: [:stream_id, :pts, :dts]}
  defstruct [:data, :stream_id, :pts, :dts]
end
