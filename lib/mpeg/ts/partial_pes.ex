defmodule MPEG.TS.PartialPES do
  @behaviour MPEG.TS.Unmarshaler

  @moduledoc """
  Partial Packetized Elemetary Stream. PES packets are much larger in size than
  TS packets are. This means that they have to be unmarshaled from a series of
  payloads, hence each packet here will only contain a partial PES packet.
  """
  @type t :: %__MODULE__{data: binary(), stream_id: pos_integer(), packet_length: pos_integer()}
  defstruct [:data, :stream_id, :packet_length]

  @impl true
  def is_unmarshable?(_data, false), do: false

  def is_unmarshable?(
        <<
          1::24,
          _stream_id::8,
          _packet_length::16,
          _optional_fields::bitstring
        >>,
        true
      ),
      do: true

  def is_unmarshable?(_data, _true), do: false

  @impl true
  def unmarshal(
        <<
          1::24,
          stream_id::8,
          packet_length::16,
          optional_fields::bitstring
        >>,
        true
      ) do
    # Packet length is ignored as the field is also allowed to be zero in case
    # the payload is a video elementary stream. If the PES packet length is set
    # to zero, the PES packet can be of any length.
    case parse_optional(optional_fields, stream_id) do
      {:ok, data} ->
        {:ok, %__MODULE__{data: data, stream_id: stream_id, packet_length: packet_length}}

      err = {:error, _reason} ->
        err
    end
  end

  def unmarshal(_, true), do: {:error, :invalid_packet}

  def unmarshal(data, false), do: {:ok, data}

  defp parse_optional(<<0b10::2, optional::bitstring>>, stream_id) do
    stream_ids_with_no_optional_fields = [
      # padding_stream
      0b10111110,
      # private_stream_2
      0b10111111
    ]

    case optional do
      <<
        _scrambling_control::2,
        _priority::1,
        _data_alignment_indicator::1,
        _copyright::1,
        _original_or_copy::1,
        _pts_dts_indicator::2,
        _escr_flag::1,
        _es_rate_flag::1,
        _dsm_trick_mode_flag::1,
        _additional_copy_info_flag::1,
        _crc_flag::1,
        _extension_flag::1,
        pes_header_length::8,
        rest::binary
      >> ->
        if stream_id in stream_ids_with_no_optional_fields do
          {:ok, rest}
        else
          case rest do
            <<_optional_fields::binary-size(pes_header_length), data::binary>> -> {:ok, data}
            _ -> {:error, :invalid_payload}
          end
        end

      _ ->
        {:error, :invalid_optional_field}
    end
  end

  defp parse_optional(optional, _stream_id) do
    {:ok, optional}
  end
end
