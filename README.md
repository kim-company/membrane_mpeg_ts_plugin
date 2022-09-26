# Membrane.MPEG.TS
MPEG-TS Demuxer that implements the Membrane.Filter behaviour. Takes
table/packet parsing code from `membraneframework/membrane_mpegts_plugin`.

## TODOS
* [ ] Filter out streams we're not following to avoid an overflow. Consider
  setting a maximum limit on the partial packets the StreamQueue may collect.


