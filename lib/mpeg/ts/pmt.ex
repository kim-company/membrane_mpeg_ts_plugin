defmodule MPEG.TS.PMT do
  @moduledoc """
  Program Map Table.
  """

  @type stream_type_id_t :: 0..255
  @type stream_id_t :: 0..8191

  @type stream_t :: %{
          stream_type: atom,
          stream_type_id: stream_type_id_t
        }

  @type streams_t :: %{
          required(stream_id_t) => stream_t
        }

  defstruct [:pcr_pid, program_info: [], streams: %{}]

  @type t :: %__MODULE__{
          streams: streams_t(),
          program_info: list(),
          pcr_pid: 0..8191
        }

  @spec unmarshal(binary()) :: {:ok, t()} | {:error, :invalid_data}
  def unmarshal(<<
        _reserved::3,
        pcr_pid::13,
        _reserved2::4,
        program_info_length::12,
        rest::binary
      >>) do
    with {:ok, {program_info, rest}} <- parse_program_info(program_info_length, rest),
         {:ok, streams} <- parse_streams(rest) do
      result = %__MODULE__{
        program_info: program_info,
        streams: streams,
        pcr_pid: pcr_pid
      }

      {:ok, result}
    end
  end

  defp parse_program_info(descriptors, data)
  defp parse_program_info(0, date), do: {:ok, {[], date}}

  defp parse_streams(data, acc \\ %{})
  defp parse_streams(<<>>, acc), do: {:ok, acc}

  # TODO handle es_info (Page 54, Rec. ITU-T H.222.0 (03/2017))
  defp parse_streams(
         <<
           stream_type_id::8,
           _reserved::3,
           elementary_pid::13,
           _reserved1::4,
           program_info_length::12,
           _program_info::binary-size(program_info_length),
           rest::binary
         >>,
         acc
       ) do
    stream = %{
      stream_type_id: stream_type_id,
      stream_type: parse_stream_assigment(stream_type_id)
    }

    result = Map.put(acc, elementary_pid, stream)
    parse_streams(rest, result)
  end

  defp parse_streams(_, _) do
    {:error, :invalid_data}
  end

  # Based on https://en.wikipedia.org/wiki/Program-specific_information#Elementary_stream_types
  defp parse_stream_assigment(0x01), do: :MPEG1_VIDEO
  defp parse_stream_assigment(0x02), do: :MPEG2_VIDEO
  defp parse_stream_assigment(0x03), do: :MPEG1_AUDIO
  defp parse_stream_assigment(0x04), do: :MPEG2_AUDIO
  defp parse_stream_assigment(0x0F), do: :AAC
  defp parse_stream_assigment(0x1B), do: :H264
  defp parse_stream_assigment(0x1F), do: :MPEG4_SVC
  defp parse_stream_assigment(0x20), do: :MPEG4_MVC
  defp parse_stream_assigment(0x24), do: :H265
end
