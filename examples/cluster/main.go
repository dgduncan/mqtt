// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-co
// SPDX-FileContributor: mochi-co

package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/hooks/auth"
	"github.com/mochi-co/mqtt/v2/hooks/cluster"
	"github.com/mochi-co/mqtt/v2/listeners"
	"github.com/rs/zerolog"

	rv8 "github.com/go-redis/redis/v8"
)

func main() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	server := mqtt.New(nil)
	l := server.Log.Level(zerolog.DebugLevel)
	server.Log = &l
	_ = server.AddHook(new(auth.AllowHook), nil)
	tcp := listeners.NewTCP("t1", ":1883", nil)
	err := server.AddListener(tcp)
	if err != nil {
		log.Fatal(err)
	}

	err = server.AddHook(new(cluster.Hook), &cluster.Options{
		Options: &rv8.Options{
			Addr:     "localhost:6379", // default redis address
			Password: "",               // your password
			DB:       0,                // your redis db
		}},
	)
	if err != nil {
		log.Fatal(err)
	}

	// Start the server
	go func() {
		err := server.Serve()
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Demonstration of directly publishing messages to a topic via the
	// `server.Publish` method. Subscribe to `direct/publish` using your
	// MQTT client to see the messages.
	// go func() {
	// 	cl := server.NewClient(nil, "local", "inline", true)
	// 	for range time.Tick(time.Second * 1) {
	// 		err := server.InjectPacket(cl, packets.Packet{
	// 			FixedHeader: packets.FixedHeader{
	// 				Type: packets.Publish,
	// 			},
	// 			TopicName: "direct/publish",
	// 			Payload:   []byte("injected scheduled message"),
	// 		})
	// 		if err != nil {
	// 			server.Log.Error().Err(err).Msg("server.InjectPacket")
	// 		}
	// 		server.Log.Info().Msgf("main.go injected packet to direct/publish")
	// 	}
	// }()

	// There is also a shorthand convenience function, Publish, for easily sending
	// publish packets if you are not concerned with creating your own packets.
	// go func() {
	// 	for range time.Tick(time.Second * 5) {
	// 		err := server.Publish("direct/publish", []byte("packet scheduled message"), false, 0)
	// 		if err != nil {
	// 			server.Log.Error().Err(err).Msg("server.Publish")
	// 		}
	// 		server.Log.Info().Msgf("main.go issued direct message to direct/publish")
	// 	}
	// }()

	<-done
	server.Log.Warn().Msg("caught signal, stopping...")
	server.Close()
	server.Log.Info().Msg("main.go finished")
}
