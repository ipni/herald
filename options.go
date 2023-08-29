package herald

import (
	"crypto/rand"
	"errors"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
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

func newOptions(o ...Option) (*options, error) {
	opts := options{
		httpPublisherListenAddr: "0.0.0.0:40080",
		topic:                   "/indexer/ingest/mainnet",
		providerAddrs:           nil,
		adEntriesChunkSize:      16 << 10,
	}
	for _, apply := range o {
		if err := apply(&opts); err != nil {
			return nil, err
		}
	}
	if opts.metadata == nil {
		return nil, errors.New("metadata must be set")
	}
	if opts.providerAddrs == nil {
		return nil, errors.New("at least one provider address must be set")
	}
	if opts.identity == nil {
		logger.Warnw("no identity is specified; generating one at random...")
		var err error
		opts.identity, _, err = crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, err
		}
		opts.id, err = peer.IDFromPrivateKey(opts.identity)
		if err != nil {
			return nil, err
		}
		logger.Infow("using randomly generated identity", "peerID", opts.id)
	}
	if opts.ds == nil {
		logger.Warnw("using in-memory datastore")
		opts.ds = sync.MutexWrap(datastore.NewMapDatastore())
	}
	return &opts, nil
}

func WithHttpPublisherListenAddr(v string) Option {
	return func(o *options) error {
		o.httpPublisherListenAddr = v
		return nil
	}
}

func WithTopic(v string) Option {
	return func(o *options) error {
		o.topic = v
		return nil
	}
}

func WithIdentity(v crypto.PrivKey) Option {
	return func(o *options) error {
		var err error
		if o.id, err = peer.IDFromPrivateKey(v); err != nil {
			return err
		}
		o.identity = v
		return nil
	}
}

func WithProviderAddress(a ...multiaddr.Multiaddr) Option {
	return func(o *options) error {
		o.providerAddrs = make([]string, 0, len(a))
		for _, ma := range a {
			o.providerAddrs = append(o.providerAddrs, ma.String())
		}
		return nil
	}
}

func WithLocalPublisherDir(v string) Option {
	return func(o *options) error {
		o.localPublisherDir = v
		return nil
	}
}

func WithAdEntriesChunkSize(v int) Option {
	return func(o *options) error {
		o.adEntriesChunkSize = v
		return nil
	}
}

func WithDatastore(v datastore.Datastore) Option {
	return func(o *options) error {
		o.ds = v
		return nil
	}
}

func WithMetadata(v metadata.Metadata) Option {
	return func(o *options) error {
		var err error
		o.metadata, err = v.MarshalBinary()
		return err
	}
}
