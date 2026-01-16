defmodule Membrane.MPEG.TS.StreamFormat do
  @moduledoc """
  MPEG-TS stream format derived from PMT entries.

  Use `stream_type` plus `descriptors` to identify streams that share the same
  stream_type (e.g. PES private data with a registration descriptor).
  """

  @type t :: %__MODULE__{stream_type: atom(), descriptors: list()}
  defstruct stream_type: nil, descriptors: []
end
