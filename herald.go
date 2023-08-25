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
	}
)

// TODO add constructor and wrapping publisher interface
