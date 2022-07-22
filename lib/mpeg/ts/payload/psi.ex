defmodule MPEG.TS.Payload.PSI do
  @moduledoc """
  Program Specific Information payload. Supported tables are PMT and PAT.
  """

  alias MPEG.TS.{PAT, PMT}

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
  @type t :: %__MODULE__{header: header_t(), table: PAT.t() | PMT.t()}
  defstruct [:header, :table]

  @crc_length 4
  @remaining_header_length 5

  @type unmarshal_error_t :: :invalid_data | :unsupported_table

  @spec unmarshal(binary()) :: {:ok, t()} | {:error, unmarshal_error_t()}
  def unmarshal(data) do
    with {:ok, {header, data}} <- unmarshal_header(data) do
      content_length = header.section_length - @crc_length - @remaining_header_length

      case data do
        <<raw_data::binary-size(content_length), _crc::@crc_length-binary, _::binary>> ->
          case unmarshal_table(header.table_id, raw_data) do
            {:ok, table} -> {:ok, %__MODULE__{header: header, table: table}}
            err = {:error, _reason} -> err
          end

        _ ->
          {:error, :invalid_data}
      end
    end
  end

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

  def unmarshal_header(_), do: {:error, :invalid_data}

  defp unmarshal_table(0x00, data), do: PAT.unmarshal(data)
  defp unmarshal_table(0x02, data), do: PMT.unmarshal(data)
  defp unmarshal_table(_, _data), do: {:error, :unsupported_table}
end
