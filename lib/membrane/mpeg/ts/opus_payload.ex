defmodule Membrane.MPEG.TS.OpusPayload do
  @moduledoc """
  Packetizes and depacketizes Opus for MPEG-TS payloads.
  """

  import Bitwise

  @spec packetize(binary(), keyword()) :: binary()
  def packetize(payload, opts \\ []) when is_binary(payload) do
    trim_start = Keyword.get(opts, :trim_start, 0)
    trim_end = Keyword.get(opts, :trim_end, 0)

    flags =
      0xE0
      |> maybe_set_flag(trim_start > 0, 0x10)
      |> maybe_set_flag(trim_end > 0, 0x08)

    header = [
      0x7F,
      flags,
      encode_length(byte_size(payload))
    ]

    header =
      header
      |> maybe_add_trim(trim_start)
      |> maybe_add_trim(trim_end)

    IO.iodata_to_binary([header, payload])
  end

  @spec depacketize(binary()) :: {:ok, binary()} | {:error, atom()}
  def depacketize(<<0x7F, flags, rest::binary>>) do
    with {:ok, len, rest_after_len} <- parse_length(rest),
         {:ok, rest_after_trim} <- parse_trim(flags, rest_after_len),
         true <- byte_size(rest_after_trim) >= len do
      {:ok, binary_part(rest_after_trim, 0, len)}
    else
      {:error, reason} -> {:error, reason}
      false -> {:error, :payload_too_short}
      _ -> {:error, :invalid_header}
    end
  end

  def depacketize(_payload), do: {:error, :missing_header}

  defp maybe_set_flag(flags, true, mask), do: flags ||| mask
  defp maybe_set_flag(flags, false, _mask), do: flags

  defp maybe_add_trim(header, 0), do: header

  defp maybe_add_trim(header, value) when value > 0 and value <= 0xFFFF do
    [header, <<value::unsigned-big-16>>]
  end

  defp encode_length(size) do
    encode_length_bytes(size, [])
  end

  defp encode_length_bytes(size, acc) when size >= 0 do
    len = min(size, 255)
    next_size = size - 255
    acc = [acc, len]

    if next_size >= 0 do
      encode_length_bytes(next_size, acc)
    else
      acc
    end
  end

  defp parse_length(data) do
    parse_length_bytes(data, 0)
  end

  defp parse_length_bytes(<<>>, _acc), do: {:error, :length_eof}

  defp parse_length_bytes(<<len, rest::binary>>, acc) do
    acc = acc + len

    if len < 255 do
      {:ok, acc, rest}
    else
      parse_length_bytes(rest, acc)
    end
  end

  defp parse_trim(flags, rest) do
    with {:ok, rest} <- maybe_drop_trim(rest, flags, 0x10),
         {:ok, rest} <- maybe_drop_trim(rest, flags, 0x08) do
      {:ok, rest}
    end
  end

  defp maybe_drop_trim(rest, flags, mask) do
    if (flags &&& mask) != 0 do
      drop_bytes(rest, 2)
    else
      {:ok, rest}
    end
  end

  defp drop_bytes(rest, count) do
    if byte_size(rest) < count do
      {:error, :trim_eof}
    else
      {:ok, binary_part(rest, count, byte_size(rest) - count)}
    end
  end
end
