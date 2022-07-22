defmodule MPEG.TS.PSI do
  @behaviour MPEG.TS.Unmarshaler

  @moduledoc """
  Program Specific Information payload. Supported tables are PMT and PAT.
  """

  @type header_t :: %{
          table_id: 0..3 | 16..31,
          section_syntax_indicator: boolean,
          section_length: 0..1021,
          transport_stream_id: 0..65_535,
          version_number: 0..31,
          current_next_indicator: boolean,
          section_number: 0..255,
          last_section_number: 0..255
        }
  @type t :: %__MODULE__{header: header_t(), table: bitstring()}
  defstruct [:header, :table]

  @crc_length 4
  @remaining_header_length 5

  @impl true
  def is_unmarshable?(data, is_start_unit) do
    case unmarshal_header(data, is_start_unit) do
      {:ok, _} -> true
      _ -> false
    end
  end

  @impl true
  def unmarshal(data, is_start_unit) do
    with {:ok, {header, data}} <- unmarshal_header(data, is_start_unit) do
      content_length = header.section_length - @crc_length - @remaining_header_length

      case data do
        <<raw_data::binary-size(content_length), _crc::@crc_length-binary, _::binary>> ->
          {:ok, %__MODULE__{header: header, table: raw_data}}

        _ ->
          {:error, :invalid_data}
      end
    end
  end

  def unmarshal_header(<<0::8, data::bitstring>>, true), do: unmarshal_header(data)
  def unmarshal_header(data, false), do: unmarshal_header(data)

  def unmarshal_header(<<
        table_id::8,
        section_syntax_indicator::1,
        0::1,
        _r1::2,
        # section length starts with 00
        0::2,
        section_length::10,
        transport_stream_id::16,
        _r2::2,
        version_number::5,
        current_next_indicator::1,
        section_number::8,
        last_section_number::8,
        rest::binary
      >>) do
    header = %{
      table_id: table_id,
      section_syntax_indicator: section_syntax_indicator == 1,
      section_length: section_length,
      transport_stream_id: transport_stream_id,
      version_number: version_number,
      current_next_indicator: current_next_indicator == 1,
      section_number: section_number,
      last_section_number: last_section_number
    }

    {:ok, {header, rest}}
  end

  def unmarshal_header(_), do: {:error, :invalid_header}
end
