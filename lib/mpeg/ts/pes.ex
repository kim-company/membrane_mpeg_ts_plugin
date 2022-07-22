defmodule MPEG.TS.PES do
  @moduledoc """
  Packetized Elemetary Stream. PES packets are much larger in size
  than TS packets are. This means that they have to be unmarshaled from a
  series of payloads.
  """
  @type t :: %__MODULE__{data: binary()}
  defstruct [:data]

  @type unmarshal_error_t :: :invalid_data

  @spec unmarshal(binary()) :: {:ok, t} | {:error, unmarshal_error_t}
  def unmarshal(data) do
    {:ok, %__MODULE__{data: data}}
  end
end
