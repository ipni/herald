package herald

import (
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type (
	Option  func(*options) error
	options struct {
		httpPublisherListenAddr string
		topic                   string
		id                      peer.ID
		identity                crypto.PrivKey
		providerAddrs           []string
		localPublisherDir       string
		adEntriesChunkSize      int
		ds                      datastore.Datastore
		metadata                []byte
	}
)

// TODO add options setters
