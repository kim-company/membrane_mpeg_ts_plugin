# Membrane MPEG.TS Plugin
![Hex.pm Version](https://img.shields.io/hexpm/v/membrane_mpeg_ts_plugin)

MPEG-TS Demuxer and Muxer for the Membrane Framework.

This element is used in production.

## Installation

Add to your `mix.exs`:

```elixir
def deps do
  [
    {:membrane_mpeg_ts_plugin, "~> 2.1"}
  ]
end
```

## Usage

### Demuxer

Extract streams by PID:
```elixir
spec = [
  child(:source, %Membrane.File.Source{location: "input.ts"})
  |> child(:demuxer, Membrane.MPEG.TS.Demuxer),
  get_child(:demuxer)
  |> via_out(:output, options: [pid: 0x100])
  |> child(:video_sink, %Membrane.File.Sink{location: "video.h264"}),
  get_child(:demuxer)
  |> via_out(:output, options: [pid: 0x101])
  |> child(:audio_sink, %Membrane.File.Sink{location: "audio.aac"})
]
```

Extract streams by type:
```elixir
get_child(:demuxer)
|> via_out(:output, options: [stream_type: :H264_AVC])
|> child(:video_sink, sink)

get_child(:demuxer)
|> via_out(:output, options: [stream_category: :video])
|> child(:video_sink, sink)
```

Extract streams by profile (stream_type + descriptors + depacketizer):
```elixir
get_child(:demuxer)
|> via_out(:output, options: [profile: :opus_mpeg_ts])
|> child(:audio_sink, sink)
```

PMT notification:
```elixir
assert_pipeline_notified(pid, :demuxer, {:pmt, %MPEG.TS.PMT{}})
```

### Muxer

```elixir
spec = [
  child(:h264_source, source)
  |> via_in(:input, options: [stream_type: :H264_AVC])
  |> get_child(:muxer),
  child(:aac_source, source)
  |> via_in(:input, options: [stream_type: :AAC_ADTS])
  |> get_child(:muxer),
  child(:muxer, Membrane.MPEG.TS.Muxer)
  |> child(:sink, %Membrane.File.Sink{location: "output.ts"})
]
```

Mux with a profile (adds descriptors and packetizes payloads when needed):
```elixir
child(:opus_source, source)
|> via_in(:input, options: [profile: :opus_mpeg_ts])
|> get_child(:muxer)
```

Custom payloads via descriptors (e.g. JSON in PES private data):
```elixir
child(:json_source, source)
|> via_in(:input, options: [
  stream_type: :PES_PRIVATE_DATA,
  descriptors: [%{tag: 0x05, data: "JSON"}]
])
|> get_child(:muxer)
```

Available profiles:
- `:opus_mpeg_ts`
- `:scte35`


## Copyright and License
Copyright 2022, [KIM Keep In Mind GmbH](https://www.keepinmind.info/)
Licensed under the [Apache License, Version 2.0](LICENSE)
