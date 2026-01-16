defmodule Membrane.MPEG.TS.Profiles do
  @moduledoc """
  Well-known stream profiles that combine stream_type, descriptors, and payload adapters.

  Profiles provide a higher-level way to select or configure streams without
  hiding the underlying PMT details. For example, `:opus_mpeg_ts` expands to
  PES private data with an `Opus` registration descriptor and uses the Opus
  MPEG-TS packetizer/depacketizer.
  """

  @profiles %{
    opus_mpeg_ts: %{
      stream_type: :PES_PRIVATE_DATA,
      descriptors: [%{tag: 0x05, data: "Opus"}],
      stream_category: :audio,
      packetizer: Membrane.MPEG.TS.OpusPayload
    },
    scte35: %{
      stream_type: :PES_PRIVATE_DATA,
      descriptors: [%{tag: 0x05, data: "CUEI"}],
      stream_category: :cues,
      packetizer: nil
    }
  }

  @spec fetch(atom()) :: {:ok, map()} | :error
  def fetch(profile) do
    Map.fetch(@profiles, profile)
  end

  @spec fetch!(atom()) :: map()
  def fetch!(profile) do
    Map.fetch!(@profiles, profile)
  end

  @spec list() :: [atom()]
  def list do
    Map.keys(@profiles)
  end
end
