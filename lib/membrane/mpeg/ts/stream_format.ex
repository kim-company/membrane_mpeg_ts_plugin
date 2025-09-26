defmodule MPEG.TS.StreamFormat do
  @type t :: %__MODULE__{stream_type: atom()}
  defstruct [:stream_type]
end
