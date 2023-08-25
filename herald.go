package herald

import (
	"errors"

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
	Herald struct {
		*options
	}
)

// TODO add constructor and wrapping publisher interface
