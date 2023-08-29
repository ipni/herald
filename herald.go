package herald

import (
	"context"
	"errors"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multihash"
)

var (
	logger = log.Logger("herald")

	ErrCatalogIteratorDone = errors.New("no more items")
)

type (
	CatalogID       []byte
	CatalogIterator interface {
		Next() (multihash.Multihash, error)
		Done() bool
	}
	Catalog interface {
		ID() []byte
		Iterator() CatalogIterator
		Transport() interface {
			Providers() any
		}
	}
	Publisher interface {
		Publish(context.Context, Catalog) (cid.Cid, error)
		Retract(context.Context, CatalogID) (cid.Cid, error)
		GetContent(context.Context, cid.Cid) (io.ReadCloser, error)
		GetHead(context.Context) (cid.Cid, error)
		// TODO:
		//  - Update address
		//  - AddProvider
		//  - RemoveProvider
		//  - UpdateProvider
		//  - Transport et. al.
	}
	Herald struct {
		*options
		publisher *httpPublisher
	}
)

func New(o ...Option) (*Herald, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	h := &Herald{options: opts}
	dspub, err := newDsPublisher(h)
	if err != nil {
		return nil, err
	}
	h.publisher, err = newHttpPublisher(h, dspub)
	if err != nil {
		return nil, err
	}
	return h, err
}

func (h *Herald) Start(ctx context.Context) error {
	return h.publisher.Start(ctx)
}

func (h *Herald) Shutdown(ctx context.Context) error {
	return h.publisher.Shutdown(ctx)
}
