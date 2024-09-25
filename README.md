# Membrane MPEG.TS Plugin
MPEG-TS Demuxer that implements the Membrane.Filter behaviour.

This element is used in production.

## Usage
1. Attach the filter to a TS source
2. The pipeline will receive a notification with the Program Map Table (PMT) found
3. Extract the streams you're interested in, use them as pad identifiers when allocating the new pad.


## Copyright and License
Copyright 2022, [KIM Keep In Mind GmbH](https://www.keepinmind.info/)
Licensed under the [Apache License, Version 2.0](LICENSE)
