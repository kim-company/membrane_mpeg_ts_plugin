defmodule Membrane.MPEG.TS.StreamFormat do
  @type t :: %__MODULE__{stream_type: atom(), descriptors: list()}
  defstruct stream_type: nil, descriptors: []
end
