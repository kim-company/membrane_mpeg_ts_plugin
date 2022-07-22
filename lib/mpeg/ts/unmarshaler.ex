defmodule MPEG.TS.Unmarshaler do
  @type t :: module()
  @type result_t :: struct() | map()
  @callback unmarshal(bitstring()) :: {:ok, result_t()} | {:error, any() | :not_enough_data}
  @callback is_unmarshable?(bitstring()) :: boolean()
end
