# Membrane.MPEG.TS
MPEG-TS Demuxer that implements the Membrane.Filter behaviour. Takes
table/packet parsing code from `membraneframework/membrane_mpegts_plugin`.

## NOTES
* the MPEG demuxer is extracting LESS data than available in the original
  stream. I compared the output extracted with our tool and with ffmpeg.
