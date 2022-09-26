defmodule MPEG.TS.Packet do
  @ts_packet_size 188
  @ts_header_size 4
  @ts_payload_size @ts_packet_size - @ts_header_size

  @type adaptation_control_t :: :payload | :adaptation | :adaptation_and_payload | :reserved

  @type scrambling_t :: :no | :even_key | :odd_key | :reserved

  @type pid_class_t :: :pat | :psi | :null_packet | :unsupported
  @type pid_t :: pos_integer()

  @type adaptation_t :: %{}

  @type payload_t :: bitstring()
  @type t :: %__MODULE__{
          payload: payload_t(),
          # payload unit start indicator
          pusi: boolean(),
          pid: pid_t(),
          continuity_counter: binary(),
          scrambling: scrambling_t(),
          discontinuity_indicator: boolean(),
          random_access_indicator: boolean(),
          pcr: pos_integer()
        }
  @derive {Inspect,
           only: [
             :pid,
             :pusi,
             :continuity_counter,
             :discontinuity_indicator,
             :random_access_indicator,
             :payload
           ]}
  defstruct [
    :payload,
    :pusi,
    :pid,
    :continuity_counter,
    :scrambling,
    :discontinuity_indicator,
    :random_access_indicator,
    :pcr
  ]

  @type parse_error_t ::
          :invalid_data | :invalid_packet | :unsupported_packet

  @spec parse(binary()) ::
          {:ok, t} | {:error, parse_error_t, binary()}
  def parse(
        data = <<
          0x47::8,
          _transport_error_indicator::1,
          payload_unit_start_indicator::1,
          _transport_priority::1,
          pid::13,
          transport_scrambling_control::2,
          adaptation_field_control::2,
          continuity_counter::4,
          optional_fields::@ts_payload_size-binary
        >>
      ) do
    with adaptation_field_id = parse_adaptation_field_control(adaptation_field_control),
         pid_class = parse_pid_class(pid),
         pusi = parse_flag(payload_unit_start_indicator),
         scrambling = parse_scrambling_control(transport_scrambling_control),
         {:ok, adaptation, data} <- parse_payload(optional_fields, adaptation_field_id, pid_class) do
      packet = %__MODULE__{
        pusi: pusi,
        pid: pid,
        payload: data,
        scrambling: scrambling,
        continuity_counter: continuity_counter,
        discontinuity_indicator: Map.get(adaptation, :discontinuity_indicator, false),
        random_access_indicator: Map.get(adaptation, :random_access_indicator, false),
        pcr: Map.get(adaptation, :pcr, nil)
      }

      {:ok, packet}
    else
      {:error, reason} -> {:error, reason, data}
    end
  end

  def parse(data = <<0x47::8, _::binary>>) when byte_size(data) < @ts_packet_size,
    do: {:error, :not_enough_data, data}

  def parse(data), do: {:error, :invalid_data, data}

  def packet_size(), do: @ts_packet_size

  @spec parse_many(binary()) :: [{:error, parse_error_t(), binary()} | {:ok, t}]
  def parse_many(data), do: parse_many(data, [])

  @spec parse_many!(binary()) :: [t]
  def parse_many!(data) do
    data
    |> parse_many()
    |> Enum.map(fn {:ok, x} -> x end)
  end

  @spec parse_valid(binary()) :: [t]
  def parse_valid(data) do
    data
    |> parse_many()
    |> Enum.filter(fn
      {:ok, _} -> true
      {:error, _, _} -> false
    end)
    |> Enum.map(fn {:ok, x} -> x end)
  end

  defp parse_many(<<>>, acc), do: Enum.reverse(acc)

  defp parse_many(<<packet::binary-@ts_packet_size, rest::binary>>, acc) do
    parse_many(rest, [parse(packet) | acc])
  end

  defp parse_many(data, acc) when byte_size(data) < @ts_packet_size do
    parse_many(<<>>, [parse(data) | acc])
  end

  defp parse_adaptation_field_control(0b01), do: :payload
  defp parse_adaptation_field_control(0b10), do: :adaptation
  defp parse_adaptation_field_control(0b11), do: :adaptation_and_payload
  defp parse_adaptation_field_control(0b00), do: :reserved

  defp parse_scrambling_control(0b00), do: :no
  defp parse_scrambling_control(0b01), do: :reserved
  defp parse_scrambling_control(0b10), do: :even_key
  defp parse_scrambling_control(0b11), do: :odd_key

  defp parse_pid_class(0x0000), do: :pat
  defp parse_pid_class(id) when id in 0x0020..0x1FFA or id in 0x1FFC..0x1FFE, do: :psi
  defp parse_pid_class(0x1FFF), do: :null_packet
  defp parse_pid_class(_), do: :unsupported

  defp parse_flag(0b1), do: true
  defp parse_flag(0b0), do: false

  @spec parse_payload(binary(), adaptation_control_t(), pid_class_t()) ::
          {:ok, Map.t(), bitstring()} | {:error, parse_error_t()}
  defp parse_payload(data, :adaptation, _) do
    with {:ok, adaptation} <- parse_adaptation_field(data) do
      {:ok, adaptation, <<>>}
    end
  end

  defp parse_payload(_, :reserved, _), do: {:error, :unsupported_packet}
  defp parse_payload(_, :payload, :null_packet), do: {:ok, %{}, <<>>}

  defp parse_payload(
         <<
           adaptation_field_length::8,
           adaptation_field::binary-size(adaptation_field_length),
           payload::bitstring
         >>,
         :adaptation_and_payload,
         pid
       ) do
    with {:ok, %{}, payload} <- parse_payload(payload, :payload, pid),
         {:ok, adaptation} <- parse_adaptation_field(adaptation_field) do
      {:ok, adaptation, payload}
    end
  end

  defp parse_payload(payload, :payload, :psi), do: {:ok, %{}, payload}
  defp parse_payload(payload, :payload, :pat), do: {:ok, %{}, payload}
  defp parse_payload(_, _, _), do: {:error, :unsupported_packet}

  defp parse_adaptation_field(<<
         discontinuity_indicator::1,
         random_access_indicator::1,
         _elementary_stream_priority_indicator::1,
         has_pcr::1,
         _has_opcr::1,
         _has_splicing_point::1,
         _is_transport_private_data::1,
         _has_adaptation_field_extension::1,
         rest::binary
       >>) do
    discontinuity_indicator = parse_flag(discontinuity_indicator)
    random_access_indicator = parse_flag(random_access_indicator)
    has_pcr = parse_flag(has_pcr)

    adaptation =
      if has_pcr do
        {pcr, _} = parse_pcr(rest)
        %{pcr: pcr}
      else
        %{}
      end

    adaptation =
      Map.merge(adaptation, %{
        discontinuity_indicator: discontinuity_indicator,
        random_access_indicator: random_access_indicator
      })

    {:ok, adaptation}
  end

  defp parse_pcr(<<
         base::33,
         _reserved::6,
         extension::9,
         rest::binary
       >>) do
    pcr = base * 300 + extension
    {pcr, rest}
  end
end
