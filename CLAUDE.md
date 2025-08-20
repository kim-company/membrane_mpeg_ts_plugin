# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Building and Testing
- `mix deps.get` - Install dependencies
- `mix compile` - Compile the project
- `mix test` - Run all tests
- `mix docs` - Generate documentation (outputs to `doc/` directory)

### Elixir Environment
- Uses Elixir 1.18.3 with OTP 27 (managed via mise.toml)
- Project uses Mix build tool with standard Elixir project structure

## Architecture Overview

This is a Membrane Framework plugin for MPEG Transport Stream (MPEG-TS) processing. The core components are:

### Main Modules
- **`Membrane.MPEG.TS.Demuxer`** (`lib/membrane/mpeg/ts/demuxer.ex`): Primary demuxer that implements Membrane.Filter behavior. Extracts individual streams from MPEG-TS input, waits for PAT/PMT tables, and creates output pads dynamically based on discovered streams.

- **`Membrane.MPEG.TS.Muxer`** (`lib/membrane/mpeg/ts/muxer.ex`): Experimental muxer supporting AAC audio and H264 video. Handles PES packetization, PCR insertion, and PAT/PMT table generation.

- **`Membrane.MPEG.TS.Aggregator`** (`lib/membrane/mpeg/ts/aggregator.ex`): Aggregates TS packets into segments based on duration and PUSI (Payload Unit Start Indicator) boundaries.

- **`Membrane.MPEG.TS.AVDemuxer`** (`lib/membrane/mpeg/ts/av_demuxer.ex`): Convenience wrapper that automatically extracts first audio (AAC) and video (H264) streams from TS input.

### Key Dependencies
- `mpeg_ts` ~> 2.0 - Core MPEG-TS parsing library
- `membrane_core` ~> 1.0 - Membrane Framework core
- `crc` ~> 0.10 - CRC calculation for TS packets

### Architecture Pattern
- Uses Membrane Framework's Filter/Bin pattern
- Dynamic pad creation based on PMT stream discovery
- State machines for handling PMT parsing and stream setup
- Buffer-based processing with demand-driven flow control

## Development Notes

- Tests use ExUnit with fixtures in `test/fixtures/`
- Documentation generated with ExDoc
- Production-ready (noted in README)
- Focused on MPEG-TS demuxing primarily, with experimental muxing capabilities