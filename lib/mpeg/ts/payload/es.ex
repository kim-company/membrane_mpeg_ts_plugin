defmodule MPEG.TS.Payload.ES do
  @moduledoc """
  Elemetary Stream Payload instance.
  """
  @type t :: %__MODULE__{data: binary()}
  defstruct [:data]
end
