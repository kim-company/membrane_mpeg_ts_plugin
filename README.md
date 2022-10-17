# Membrane MPEG.TS Plugin
MPEG-TS Demuxer that implements the Membrane.Filter behaviour.

This element is used in production.

## Installation
```elixir
def deps do
  [
    {:membrane_mpeg_ts_plugin, github: "kim-company/membrane_mpeg_ts_plugin"}
  ]
end
```

## Usage
1. Attach the filter to a TS source
2. The pipeline will receive a notification with the Program Map Table (PMT) found
3. Extract the streams you're interested in, use them as pad identifiers when allocating the new pad.

## Gotchas
### On LFS (if tests are failing
Beware that fixtures are stored using the git LFS protocol. On debian, set it up
with
```
% sudo apt install git-lfs
# Within the repo
% git lfs install
% git lfs pull
```

If you add more fixture files, track them on LFS with `git lfs track <the
files>`.

## Copyright and License
Copyright 2022, [KIM Keep In Mind GmbH](https://www.keepinmind.info/)
Licensed under the [Apache License, Version 2.0](LICENSE)



