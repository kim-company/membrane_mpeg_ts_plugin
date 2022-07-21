defmodule MPEG.TS.Packet do
  alias MPEG.TS.Payload.{PSI, ES, Empty}

  @ts_packet_size 188
  @ts_header_size 4
  @ts_payload_size @ts_packet_size - @ts_header_size

  @type adaptation_t :: :payload | :adaptation | :adaptation_and_payload | :reserved

  @type payload_id_t :: :psi | :null_packet | :unsupported

  @type payload_t :: PSI.t() | ES.t() | Empty.t()
  @type t :: %__MODULE__{
    payload: payload_t()
  }
  defstruct [:payload]

  @type unmarshal_error_t :: :invalid_data | :not_enough_data | :invalid_packet | :unsupported_packet

  @spec unmarshal(binary()) :: {:ok, t, binary()} | {:error, unmarshal_error_t, binary()}
  def unmarshal(data = <<
           0x47::8,
           _transport_error_indicator::1,
           payload_unit_start_indicator::1,
           _transport_priority::1,
           pid::13,
           _transport_scrambling_control::2,
           adaptation_field_control::2,
           _continuity_counter::4,
           # 184 = 188 - header length
    optional_fields::@ts_payload_size-binary,
    rest::binary,
     >>) do

   with adaptation_field_id = parse_adaptation_field(adaptation_field_control),
        payload_id = parse_payload_id(pid),
        pusi = parse_pusi(payload_unit_start_indicator),
            {:ok, data} <- parse_payload(optional_fields, adaptation_field_id, payload_id, pusi),
         {:ok, payload} <- unmarshal_payload(data) do
     {:ok, %__MODULE__{payload: payload}, rest}
    else
      {:error, reason} -> {:error, reason, data}
    end
  end

  def unmarshal(data = <<0x47::8, _rest::binary>>), do: {:error, :not_enough_data, data}
  def unmarshal(data) when byte_size(data) == @ts_packet_size, do: {:error, :invalid_packet, data}
  def unmarshal(data) , do: {:error, :invalid_data, data}

  defp parse_adaptation_field(0b01), do: :payload
  defp parse_adaptation_field(0b10), do: :adaptation
  defp parse_adaptation_field(0b11), do: :adaptation_and_payload
  defp parse_adaptation_field(0b00), do: :reserved

  defp parse_payload_id(0x0000), do: :psi
  defp parse_payload_id(id) when id in 0x0020..0x1FFA or id in 0x1FFC..0x1FFE, do: :psi
  defp parse_payload_id(0x1FFF), do: :null_packet
  defp parse_payload_id(_), do: :unsupported

  defp parse_pusi(0b1), do: true
  defp parse_pusi(0b0), do: false

  @spec parse_payload(binary(), adaptation_t(), payload_id_t(), boolean()) :: {:ok, bitstring()} | {:error, unmarshal_error_t()}
  defp parse_payload(_, :adaptation, _, _), do: {:ok, <<>>}
  defp parse_payload(_, :reserved, _, _), do: {:error, :unsupported_packet}
  defp parse_payload(_, :payload, :null_packet, _), do: {:ok, <<>>}

  defp parse_payload(<<
        adaptation_field_length::8,
        _adaptation_field::binary-size(adaptation_field_length),
        payload::bitstring
    >>, :adaptation_and_payload, payload_id, pusi), do: parse_payload(payload, :payload, payload_id, pusi)

  # TODO: we have to use the pointer to find the proper payload starting point.
  defp parse_payload(<<_pointer::8, payload::binary>>, :payload, :psi, true), do: {:ok, payload}
  defp parse_payload(payload, :payload, :psi, false), do: {:ok, payload}

  defp parse_payload(_, :payload, _, _), do: {:error, :unsupported_packet}

  defp unmarshal_payload(<<>>), do: {:ok, %Empty{}}
  defp unmarshal_payload(data), do: PSI.unmarshal(data)
end
