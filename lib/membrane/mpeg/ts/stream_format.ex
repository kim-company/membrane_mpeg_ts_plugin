defmodule Membrane.MPEG.TS.StreamFormat do
  @moduledoc """
  MPEG-TS stream format derived from PMT entries.

  Use `stream_type` plus `descriptors` to identify streams that share the same
  stream_type (e.g. PES private data with a registration descriptor).
  """

  @type elementary_stream_t :: %{
          optional(:pid) => non_neg_integer(),
          optional(:stream_type) => atom(),
          optional(:descriptors) => list(),
          optional(:upstream_format) => struct() | nil
        }

  @type t :: %__MODULE__{
          stream_type: atom() | nil,
          descriptors: list(),
          elementary_streams: [elementary_stream_t()]
        }

  defstruct stream_type: nil, descriptors: [], elementary_streams: []
end
