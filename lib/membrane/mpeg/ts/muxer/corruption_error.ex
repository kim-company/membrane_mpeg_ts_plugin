defmodule Membrane.MPEG.TS.Muxer.CorruptionError do
  @moduledoc """
  Error raised when the muxer produces corrupted output.

  This indicates a critical bug in the underlying mpeg_ts library where
  packets are being generated with PIDs that were not registered in the PMT,
  or with invalid/uninitialized memory values.
  """

  defexception [:message, :invalid_pids, :valid_pids, :packet_count]

  @impl true
  def exception(opts) do
    invalid_pids = Keyword.fetch!(opts, :invalid_pids)
    valid_pids = Keyword.fetch!(opts, :valid_pids)
    packet_count = Keyword.fetch!(opts, :packet_count)

    invalid_pids_hex =
      invalid_pids
      |> Enum.map(&"0x#{Integer.to_string(&1, 16)}")
      |> Enum.join(", ")

    valid_pids_hex =
      valid_pids
      |> Enum.map(&"0x#{Integer.to_string(&1, 16)}")
      |> Enum.join(", ")

    message = """
    MPEG-TS Muxer Corruption Detected!

    Found #{packet_count} packet(s) with PIDs that were never registered in the PMT.
    This indicates a critical bug in the mpeg_ts library (uninitialized memory or buffer corruption).

    Invalid PIDs: #{inspect(invalid_pids)} (#{invalid_pids_hex})
    Valid PIDs:   #{inspect(valid_pids)} (#{valid_pids_hex})

    These invalid PIDs will produce an MPEG-TS stream that:
    - Violates the MPEG-TS specification
    - Contains unreferenced PIDs not in the PMT
    - May appear as scrambled/encrypted to analyzers
    - Will fail validation with tools like tsanalyze

    Action Required:
    1. Report this to: https://github.com/kim-company/kim_mpeg_ts/issues
    2. Include your muxer configuration and input stream details
    3. This is likely a bug in mpeg_ts library version 3.3.x

    Technical Details:
    - The PID field in MPEG-TS packets is 13 bits
    - Random PIDs suggest uninitialized memory in packet construction
    - The mpeg_ts library may have a buffer overflow or memory corruption issue
    """

    %__MODULE__{
      message: message,
      invalid_pids: invalid_pids,
      valid_pids: valid_pids,
      packet_count: packet_count
    }
  end
end
