# Membrane.MPEG.TS
MPEG-TS Demuxer that implements the Membrane.Filter behaviour. Takes
table/packet parsing code from `membraneframework/membrane_mpegts_plugin`.

## TODOS
* [x] Filter out streams we're not following to avoid an overflow. Consider
  setting a maximum limit on the partial packets the StreamQueue may collect.
* [x] Get rid of Agent in Demuxer
