package main

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/dgduncan/mqtt/v3/packets"
	"github.com/stretchr/testify/require"
)

func TestReadAcknowledgementReasons(t *testing.T) {
	for _, header := range []byte{0x40, 0x50, 0x62, 0x70} {
		for _, reason := range []byte{0, packets.ErrNotAuthorized.Code} {
			wire := []byte{header, 4, 0, 7, reason, 0}
			c := &client{r: bufio.NewReader(bytes.NewReader(wire))}
			pk, err := c.read()
			require.NoError(t, err)
			require.Equal(t, header>>4, pk.FixedHeader.Type)
			require.Equal(t, uint16(7), pk.PacketID)
			require.Equal(t, reason, pk.ReasonCode)
		}
	}
}
