// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package main

import (
	"bufio"
	"bytes"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	mqtt "github.com/dgduncan/mqtt/v3"
	"github.com/dgduncan/mqtt/v3/hooks/auth"
	"github.com/dgduncan/mqtt/v3/packets"
	"github.com/stretchr/testify/require"
)

// Exercise the cluster's auth-file loading through MQTT, including the fallback
// allow rule used in the original credential bypass and ACLs without a final deny.
func TestAddAuth(t *testing.T) {
	ledger := auth.Ledger{
		Users: auth.Users{
			"device":    {Password: "secret", ACL: auth.Filters{"tenant/a/#": auth.ReadWrite}},
			"suspended": {Password: "secret", Disallow: true},
			"acl-only":  {ACL: auth.Filters{"tenant/a/#": auth.ReadOnly}},
		},
		Auth: auth.AuthRules{{Client: "audit-*", Allow: true}},
		ACL: auth.ACLRules{
			{Username: "rule-user", Filters: auth.Filters{"tenant/a/#": auth.ReadWrite}},
			{Username: "explicit-all", Filters: auth.Filters{"#": auth.ReadWrite}},
		},
	}
	for format, encode := range map[string]func() ([]byte, error){"yaml": ledger.ToYAML, "json": ledger.ToJSON} {
		t.Run(format, func(t *testing.T) {
			data, err := encode()
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "auth."+format)
			require.NoError(t, os.WriteFile(path, data, 0600))
			server := mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			t.Cleanup(func() { require.NoError(t, server.Close()) })
			require.NoError(t, addAuth(server, &config{AuthFile: path}))

			for _, tt := range []struct{ name, username, password string }{
				{"wrong password", "device", "wrong"},
				{"missing password", "device", ""},
				{"disabled correct password", "suspended", "secret"},
				{"disabled wrong password", "suspended", "wrong"},
			} {
				t.Run(tt.name, func(t *testing.T) {
					_, _, ack := authTestConnect(t, server, tt.username, tt.password)
					require.Equal(t, packets.ErrBadUsernameOrPassword.Code, ack.ReasonCode)
				})
			}

			for _, username := range []string{"device", "rule-user", "acl-only", "no-grants", "explicit-all"} {
				t.Run(username, func(t *testing.T) {
					conn, reader, ack := authTestConnect(t, server, username, "secret")
					require.Zero(t, ack.ReasonCode)
					for i, topic := range []string{"tenant/a/allowed", "tenant/b/denied"} {
						readAllowed := username == "explicit-all" || (i == 0 && username != "no-grants")
						writeAllowed := readAllowed && username != "acl-only"
						for _, qos := range []byte{1, 2} {
							authTestWrite(t, conn, packets.Packet{
								FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos},
								TopicName:   topic, PacketID: uint16(1 + i*2 + int(qos)), Payload: []byte("probe"),
							})
							ack := authTestRead(t, reader)
							wantType := packets.Puback
							if qos == 2 {
								wantType = packets.Pubrec
							}
							require.Equal(t, wantType, ack.FixedHeader.Type)
							if writeAllowed {
								require.Less(t, ack.ReasonCode, byte(0x80))
								if qos == 2 {
									authTestWrite(t, conn, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrel, Qos: 1}, PacketID: ack.PacketID})
									ack = authTestRead(t, reader)
									require.Equal(t, packets.Pubcomp, ack.FixedHeader.Type)
									require.Zero(t, ack.ReasonCode)
								}
							} else {
								require.Equal(t, packets.ErrNotAuthorized.Code, ack.ReasonCode)
							}
						}
						// Subscribe after publishing so deliveries cannot hide the acknowledgements.
						authTestWrite(t, conn, packets.Packet{
							FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1}, PacketID: uint16(20 + i),
							Filters: packets.Subscriptions{{Filter: topic, Qos: 2}},
						})
						ack := authTestRead(t, reader)
						require.Equal(t, packets.Suback, ack.FixedHeader.Type)
						want := packets.ErrNotAuthorized.Code
						if readAllowed {
							want = packets.CodeGrantedQos2.Code
						}
						require.Equal(t, []byte{want}, ack.ReasonCodes)
					}
					for _, filter := range []string{"#", "$SYS/#"} {
						authTestWrite(t, conn, packets.Packet{
							FixedHeader: packets.FixedHeader{Type: packets.Subscribe, Qos: 1}, PacketID: 30,
							Filters: packets.Subscriptions{{Filter: filter, Qos: 2}},
						})
						ack := authTestRead(t, reader)
						require.Equal(t, packets.Suback, ack.FixedHeader.Type)
						want := packets.ErrNotAuthorized.Code
						if username == "explicit-all" {
							want = packets.CodeGrantedQos2.Code
						}
						require.Equal(t, []byte{want}, ack.ReasonCodes)
					}
				})
			}
		})
	}
}

func authTestConnect(t *testing.T, server *mqtt.Server, username, password string) (net.Conn, *bufio.Reader, packets.Packet) {
	t.Helper()
	client, broker := net.Pipe()
	done := make(chan struct{})
	t.Cleanup(func() {
		_ = client.Close()
		_ = broker.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("broker connection did not stop")
		}
	})
	go func() { defer close(done); _ = server.EstablishConnection("audit", broker) }()
	require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
	authTestWrite(t, client, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Connect},
		Connect: packets.ConnectParams{
			ProtocolName: []byte("MQTT"), ClientIdentifier: "audit-client", Clean: true, Keepalive: 30,
			UsernameFlag: true, Username: []byte(username), PasswordFlag: password != "", Password: []byte(password),
		},
	})
	reader := bufio.NewReader(client)
	ack := authTestRead(t, reader)
	require.Equal(t, packets.Connack, ack.FixedHeader.Type)
	return client, reader, ack
}

func authTestWrite(t *testing.T, conn net.Conn, pk packets.Packet) {
	t.Helper()
	pk.ProtocolVersion = 5
	var buf bytes.Buffer
	var err error
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(&buf)
	case packets.Publish:
		err = pk.PublishEncode(&buf)
	case packets.Pubrel:
		err = pk.PubrelEncode(&buf)
	case packets.Subscribe:
		err = pk.SubscribeEncode(&buf)
	default:
		t.Fatalf("unexpected request type: %d", pk.FixedHeader.Type)
	}
	require.NoError(t, err)
	_, err = conn.Write(buf.Bytes())
	require.NoError(t, err)
}

func authTestRead(t *testing.T, reader *bufio.Reader) packets.Packet {
	t.Helper()
	pk := packets.Packet{ProtocolVersion: 5}
	header, err := reader.ReadByte()
	require.NoError(t, err)
	require.NoError(t, pk.FixedHeader.Decode(header))
	n, _, err := packets.DecodeLength(reader)
	require.NoError(t, err)
	pk.FixedHeader.Remaining = n
	body := make([]byte, n)
	_, err = io.ReadFull(reader, body)
	require.NoError(t, err)
	switch pk.FixedHeader.Type {
	case packets.Connack:
		err = pk.ConnackDecode(body)
	case packets.Puback, packets.Pubrec, packets.Pubcomp:
		err = pk.PubackDecode(body)
	case packets.Suback:
		err = pk.SubackDecode(body)
	default:
		t.Fatalf("unexpected response type: %d", pk.FixedHeader.Type)
	}
	require.NoError(t, err)
	return pk
}
