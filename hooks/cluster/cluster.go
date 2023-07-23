package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/hooks/storage"
	"github.com/mochi-co/mqtt/v2/packets"
	"github.com/mochi-co/mqtt/v2/system"
)

type Storage interface {
	Connect()
	Stop()
	GetClient()
}

type Node interface {
	GetLocalIP()
}

// type Node struct {
// }

// defaultAddr is the default address to the redis service.
const defaultAddr = "localhost:6379"

// defaultHPrefix is a prefix to better identify hsets created by mochi mqtt.
const defaultHPrefix = "mochi-"

// clientKey returns a primary key for a client.
func clientKey(cl *mqtt.Client) string {
	return cl.ID
}

// subscriptionKey returns a primary key for a subscription.
func subscriptionKey(cl *mqtt.Client, filter string) string {
	return cl.ID + ":" + filter
}

// retainedKey returns a primary key for a retained message.
func retainedKey(topic string) string {
	return topic
}

// inflightKey returns a primary key for an inflight message.
func inflightKey(cl *mqtt.Client, pk packets.Packet) string {
	return cl.ID + ":" + pk.FormatID()
}

// sysInfoKey returns a primary key for system info.
func sysInfoKey() string {
	return storage.SysInfoKey
}

// func nodeKey(uuid, ip string) string {
// 	return uuid + ":" + ip
// }

// Options contains configuration settings for the bolt instance.
type Options struct {
	HPrefix string
	Options *redis.Options
}

// Hook is a persistent storage hook based using Redis as a backend.
type Hook struct {
	mqtt.HookBase
	config *Options        // options for connecting to the Redis instance.
	db     *redis.Client   // the Redis instance
	ctx    context.Context // a context for the connection
	id     string
}

// ID returns the id of the hook.
func (h *Hook) ID() string {
	return "cluster"
}

// Provides indicates which hook methods this hook provides.
func (h *Hook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnStarted,
		mqtt.OnStopped,
		mqtt.OnSessionEstablish,
		mqtt.OnSessionEstablished,
		mqtt.OnDisconnect,
		mqtt.OnSubscribed,
		mqtt.OnUnsubscribed,
		mqtt.OnRetainMessage,
		mqtt.OnQosPublish,
		mqtt.OnQosComplete,
		mqtt.OnQosDropped,
		mqtt.OnWillSent,
		mqtt.OnSysInfoTick,
		mqtt.OnClientExpired,
		mqtt.OnRetainedExpired,
		// mqtt.StoredClients,
		// mqtt.StoredInflightMessages,
		// mqtt.StoredRetainedMessages,
		// mqtt.StoredSubscriptions,
		// mqtt.StoredSysInfo,
		mqtt.FetchStoredClient,
		mqtt.FetchStoredInflightMessages,
		mqtt.FetchStoredRetainedMessages,
		mqtt.FetchStoredSubscriptions,
	}, []byte{b})
}

// hKey returns a hash set key with a unique prefix.
func (h *Hook) hKey(s string) string {
	return h.config.HPrefix + s
}

// Init initializes and connects to the redis service.
func (h *Hook) Init(config any) error {
	if _, ok := config.(*Options); !ok && config != nil {
		return mqtt.ErrInvalidConfigType
	}

	h.ctx = context.Background()

	if config == nil {
		config = &Options{
			Options: &redis.Options{
				Addr: defaultAddr,
			},
		}
	}

	h.config = config.(*Options)
	if h.config.HPrefix == "" {
		h.config.HPrefix = defaultHPrefix
	}

	h.Log.Info().
		Str("address", h.config.Options.Addr).
		Str("username", h.config.Options.Username).
		Int("password-len", len(h.config.Options.Password)).
		Int("db", h.config.Options.DB).
		Msg("connecting to redis service")

	h.db = redis.NewClient(h.config.Options)
	_, err := h.db.Ping(context.Background()).Result()
	if err != nil {
		return fmt.Errorf("failed to ping service: %w", err)
	}

	h.Log.Info().Msg("connected to redis service")

	return nil
}

func (h *Hook) OnStarted() {
	// generate node uuid
	// add to node table

	uuid := uuid.New().String()
	h.id = uuid
	// fetch IP from interface
	ip, err := h.getLocalIP()
	if err != nil {
		h.Log.Error().Err(err).Msg("failed to fetch local IP")
		return
	}

	err = h.db.HSet(h.ctx, h.hKey(storage.NodeKey), uuid, ip).Err()
	if err != nil {
		h.Log.Error().Err(err).Msg("failed to hset subscription data")
	}
}

func (h *Hook) OnStopped() {
	// remove from table
	err := h.db.HDel(h.ctx, h.hKey(storage.NodeKey), h.id)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func (h *Hook) OnSessionEstablish(cl *mqtt.Client, pk packets.Packet) {
	// fetch client info from remote
	// check if it locally owned
	// if it is not, make RPC to remote to tell it to disconnect
	// if it is, pull in the data

}

// Close closes the redis connection.
func (h *Hook) Stop() error {
	h.Log.Info().Msg("disconnecting from redis service")
	return h.db.Close()
}

// OnSessionEstablished adds a client to the store when their session is established.
func (h *Hook) OnSessionEstablished(cl *mqtt.Client, pk packets.Packet) {
	h.updateClient(cl)
}

// OnWillSent is called when a client sends a will message and the will message is removed
// from the client record.
func (h *Hook) OnWillSent(cl *mqtt.Client, pk packets.Packet) {
	h.updateClient(cl)
}

// updateClient writes the client data to the store.
func (h *Hook) updateClient(cl *mqtt.Client) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	props := cl.Properties.Props.Copy(false)
	in := &storage.Client{
		ID:              clientKey(cl),
		T:               storage.ClientKey,
		Remote:          cl.Net.Remote,
		Listener:        cl.Net.Listener,
		Username:        cl.Properties.Username,
		Clean:           cl.Properties.Clean,
		ProtocolVersion: cl.Properties.ProtocolVersion,
		Properties: storage.ClientProperties{
			SessionExpiryInterval: props.SessionExpiryInterval,
			AuthenticationMethod:  props.AuthenticationMethod,
			AuthenticationData:    props.AuthenticationData,
			RequestProblemInfo:    props.RequestProblemInfo,
			RequestResponseInfo:   props.RequestResponseInfo,
			ReceiveMaximum:        props.ReceiveMaximum,
			TopicAliasMaximum:     props.TopicAliasMaximum,
			User:                  props.User,
			MaximumPacketSize:     props.MaximumPacketSize,
		},
		Will:      storage.ClientWill(cl.Properties.Will),
		OwnerNode: h.id,
	}

	err := h.db.HSet(h.ctx, h.hKey(storage.ClientKey), clientKey(cl), in).Err()
	if err != nil {
		h.Log.Error().Err(err).Interface("data", in).Msg("failed to hset client data")
	}
}

// OnDisconnect removes a client from the store if they were using a clean session.
func (h *Hook) OnDisconnect(cl *mqtt.Client, _ error, expire bool) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	if !expire {
		return
	}

	if cl.StopCause() == packets.ErrSessionTakenOver {
		return
	}

	err := h.db.HDel(h.ctx, h.hKey(storage.ClientKey), clientKey(cl)).Err()
	if err != nil {
		h.Log.Error().Err(err).Str("id", clientKey(cl)).Msg("failed to delete client")
	}
}

// OnSubscribed adds one or more client subscriptions to the store.
func (h *Hook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	var in *storage.Subscription
	for i := 0; i < len(pk.Filters); i++ {
		in = &storage.Subscription{
			ID:                subscriptionKey(cl, pk.Filters[i].Filter),
			T:                 storage.SubscriptionKey,
			Client:            cl.ID,
			Qos:               reasonCodes[i],
			Filter:            pk.Filters[i].Filter,
			Identifier:        pk.Filters[i].Identifier,
			NoLocal:           pk.Filters[i].NoLocal,
			RetainHandling:    pk.Filters[i].RetainHandling,
			RetainAsPublished: pk.Filters[i].RetainAsPublished,
		}

		err := h.db.HSet(h.ctx, h.hKey(storage.SubscriptionKey), subscriptionKey(cl, pk.Filters[i].Filter), in).Err()
		if err != nil {
			h.Log.Error().Err(err).Interface("data", in).Msg("failed to hset subscription data")
		}
	}
}

// OnUnsubscribed removes one or more client subscriptions from the store.
func (h *Hook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	for i := 0; i < len(pk.Filters); i++ {
		err := h.db.HDel(h.ctx, h.hKey(storage.SubscriptionKey), subscriptionKey(cl, pk.Filters[i].Filter)).Err()
		if err != nil {
			h.Log.Error().Err(err).Str("id", clientKey(cl)).Msg("failed to delete subscription data")
		}
	}
}

// OnRetainMessage adds a retained message for a topic to the store.
func (h *Hook) OnRetainMessage(cl *mqtt.Client, pk packets.Packet, r int64) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	if r == -1 {
		err := h.db.HDel(h.ctx, h.hKey(storage.RetainedKey), retainedKey(pk.TopicName)).Err()
		if err != nil {
			h.Log.Error().Err(err).Str("id", clientKey(cl)).Msg("failed to delete retained message data")
		}

		return
	}

	props := pk.Properties.Copy(false)
	in := &storage.Message{
		ID:          retainedKey(pk.TopicName),
		T:           storage.RetainedKey,
		FixedHeader: pk.FixedHeader,
		TopicName:   pk.TopicName,
		Payload:     pk.Payload,
		Created:     pk.Created,
		Origin:      pk.Origin,
		Properties: storage.MessageProperties{
			PayloadFormat:          props.PayloadFormat,
			MessageExpiryInterval:  props.MessageExpiryInterval,
			ContentType:            props.ContentType,
			ResponseTopic:          props.ResponseTopic,
			CorrelationData:        props.CorrelationData,
			SubscriptionIdentifier: props.SubscriptionIdentifier,
			TopicAlias:             props.TopicAlias,
			User:                   props.User,
		},
	}

	err := h.db.HSet(h.ctx, h.hKey(storage.RetainedKey), retainedKey(pk.TopicName), in).Err()
	if err != nil {
		h.Log.Error().Err(err).Interface("data", in).Msg("failed to hset retained message data")
	}
}

// OnQosPublish adds or updates an inflight message in the store.
func (h *Hook) OnQosPublish(cl *mqtt.Client, pk packets.Packet, sent int64, resends int) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	props := pk.Properties.Copy(false)
	in := &storage.Message{
		ID:          inflightKey(cl, pk),
		T:           storage.InflightKey,
		Origin:      pk.Origin,
		FixedHeader: pk.FixedHeader,
		TopicName:   pk.TopicName,
		Payload:     pk.Payload,
		Sent:        sent,
		Created:     pk.Created,
		Properties: storage.MessageProperties{
			PayloadFormat:          props.PayloadFormat,
			MessageExpiryInterval:  props.MessageExpiryInterval,
			ContentType:            props.ContentType,
			ResponseTopic:          props.ResponseTopic,
			CorrelationData:        props.CorrelationData,
			SubscriptionIdentifier: props.SubscriptionIdentifier,
			TopicAlias:             props.TopicAlias,
			User:                   props.User,
		},
	}

	err := h.db.HSet(h.ctx, h.hKey(storage.InflightKey), inflightKey(cl, pk), in).Err()
	if err != nil {
		h.Log.Error().Err(err).Interface("data", in).Msg("failed to hset qos inflight message data")
	}
}

// OnQosComplete removes a resolved inflight message from the store.
func (h *Hook) OnQosComplete(cl *mqtt.Client, pk packets.Packet) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	err := h.db.HDel(h.ctx, h.hKey(storage.InflightKey), inflightKey(cl, pk)).Err()
	if err != nil {
		h.Log.Error().Err(err).Str("id", clientKey(cl)).Msg("failed to delete inflight message data")
	}
}

// OnQosDropped removes a dropped inflight message from the store.
func (h *Hook) OnQosDropped(cl *mqtt.Client, pk packets.Packet) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
	}

	h.OnQosComplete(cl, pk)
}

// OnSysInfoTick stores the latest system info in the store.
func (h *Hook) OnSysInfoTick(sys *system.Info) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	in := &storage.SystemInfo{
		ID:   sysInfoKey(),
		T:    storage.SysInfoKey,
		Info: *sys,
	}

	err := h.db.HSet(h.ctx, h.hKey(storage.SysInfoKey), sysInfoKey(), in).Err()
	if err != nil {
		h.Log.Error().Err(err).Interface("data", in).Msg("failed to hset server info data")
	}
}

// OnRetainedExpired deletes expired retained messages from the store.
func (h *Hook) OnRetainedExpired(filter string) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	err := h.db.HDel(h.ctx, h.hKey(storage.RetainedKey), retainedKey(filter)).Err()
	if err != nil {
		h.Log.Error().Err(err).Str("id", retainedKey(filter)).Msg("failed to delete retained message data")
	}
}

// OnClientExpired deleted expired clients from the store.
func (h *Hook) OnClientExpired(cl *mqtt.Client) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	err := h.db.HDel(h.ctx, h.hKey(storage.ClientKey), clientKey(cl)).Err()
	if err != nil {
		h.Log.Error().Err(err).Str("id", clientKey(cl)).Msg("failed to delete expired client")
	}
}

// StoredClients returns all stored clients from the store.
func (h *Hook) StoredClients() (v []storage.Client, err error) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	rows, err := h.db.HGetAll(h.ctx, h.hKey(storage.ClientKey)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		h.Log.Error().Err(err).Msg("failed to HGetAll client data")
		return
	}

	for _, row := range rows {
		var d storage.Client
		if err = d.UnmarshalBinary([]byte(row)); err != nil {
			h.Log.Error().Err(err).Str("data", row).Msg("failed to unmarshal client data")
		}

		v = append(v, d)
	}

	return v, nil
}

// StoredSubscriptions returns all stored subscriptions from the store.
func (h *Hook) StoredSubscriptions() (v []storage.Subscription, err error) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	rows, err := h.db.HGetAll(h.ctx, h.hKey(storage.SubscriptionKey)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		h.Log.Error().Err(err).Msg("failed to HGetAll subscription data")
		return
	}

	for _, row := range rows {
		var d storage.Subscription
		if err = d.UnmarshalBinary([]byte(row)); err != nil {
			h.Log.Error().Err(err).Str("data", row).Msg("failed to unmarshal subscription data")
		}

		v = append(v, d)
	}

	return v, nil
}

// StoredRetainedMessages returns all stored retained messages from the store.
func (h *Hook) StoredRetainedMessages() (v []storage.Message, err error) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	rows, err := h.db.HGetAll(h.ctx, h.hKey(storage.RetainedKey)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		h.Log.Error().Err(err).Msg("failed to HGetAll retained message data")
		return
	}

	for _, row := range rows {
		var d storage.Message
		if err = d.UnmarshalBinary([]byte(row)); err != nil {
			h.Log.Error().Err(err).Str("data", row).Msg("failed to unmarshal retained message data")
		}

		v = append(v, d)
	}

	return v, nil
}

// StoredInflightMessages returns all stored inflight messages from the store.
func (h *Hook) StoredInflightMessages() (v []storage.Message, err error) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	rows, err := h.db.HGetAll(h.ctx, h.hKey(storage.InflightKey)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		h.Log.Error().Err(err).Msg("failed to HGetAll inflight message data")
		return
	}

	for _, row := range rows {
		var d storage.Message
		if err = d.UnmarshalBinary([]byte(row)); err != nil {
			h.Log.Error().Err(err).Str("data", row).Msg("failed to unmarshal inflight message data")
		}

		v = append(v, d)
	}

	return v, nil
}

// StoredSysInfo returns the system info from the store.
func (h *Hook) StoredSysInfo() (v storage.SystemInfo, err error) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return
	}

	row, err := h.db.HGet(h.ctx, h.hKey(storage.SysInfoKey), storage.SysInfoKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return
	}

	if err = v.UnmarshalBinary([]byte(row)); err != nil {
		h.Log.Error().Err(err).Str("data", row).Msg("failed to unmarshal sys info data")
	}

	return v, nil
}

func (h *Hook) FetchStoredClient(cl *mqtt.Client) ([]storage.Client, error) {
	if h.db == nil {
		h.Log.Error().Err(storage.ErrDBFileNotOpen)
		return []storage.Client{}, nil
	}

	row, err := h.db.HGet(h.ctx, h.hKey(storage.ClientKey), cl.ID).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		h.Log.Error().Err(err).Msg("failed to HGetAll client data")
		return []storage.Client{}, nil
	}

	if row == "" {
		println("not found")
		return []storage.Client{}, nil
	}

	var d storage.Client
	if err = d.UnmarshalBinary([]byte(row)); err != nil {
		h.Log.Error().Err(err).Str("data", row).Msg("failed to unmarshal client data")
	}

	fmt.Println("found!")
	ds := make([]storage.Client, 0)
	ds = append(ds, d)

	if d.OwnerNode == h.id {
		println("client belongs to this node")
	} else {
		println("client belongs to remote ... sending request to disconnect")
		row, err := h.db.HGet(h.ctx, h.hKey(storage.NodeKey), h.id).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			h.Log.Error().Err(err).Msg("failed to HGetAll client data")
			return []storage.Client{}, nil
		}

		if row == "" {
			fmt.Println("node not found ... continuing client connection")
			return []storage.Client{}, nil
		}

		ctx, cancel := context.WithTimeout(h.ctx, 100*time.Millisecond)
		defer cancel()

		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+row+"/client/"+cl.ID+"/disconnect", http.NoBody)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Println(err)
			fmt.Println(len(ds))
			return ds, nil
		}

		if resp.StatusCode != http.StatusOK {
			fmt.Println("something went wrong")
			return ds, nil
		}
	}

	return ds, nil
}

func (h *Hook) FetchStoredSubscriptions(cl *mqtt.Client) ([]storage.Subscription, error) {
	fmt.Println("called FetchStoredSubscriptions")
	subscriptions := make([]storage.Subscription, 0)
	return subscriptions, nil

}
func (h *Hook) FetchStoredInflightMessages(cl *mqtt.Client) ([]storage.Message, error) {
	fmt.Println("called FetchStoredInflightMessages")
	inflights := make([]storage.Message, 0)
	return inflights, nil

}
func (h *Hook) FetchStoredRetainedMessages(cl *mqtt.Client) ([]storage.Message, error) {
	fmt.Println("called FetchStoredRetainedMessages")
	retainedmessaged := make([]storage.Message, 0)
	return retainedmessaged, nil

}

func (h *Hook) getLocalIP() (string, error) {
	req, err := http.NewRequestWithContext(h.ctx, http.MethodGet, "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip", http.NoBody)
	if err != nil {
		return "", err
	}
	req.Header.Add("Metadata-Flavor", "Google")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "1.2.3.4", nil
	}

	if resp.StatusCode != http.StatusOK {
		return "", errors.New("something went wrong")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
	}

	return string(body), nil

}
