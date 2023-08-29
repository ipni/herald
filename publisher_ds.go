package herald

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/multiformats/go-multihash"
)

var (
	_ Publisher     = (*dsPublisher)(nil)
	_ io.ReadCloser = (*pooledBytesBufferCloser)(nil)

	bytesBuffers = sync.Pool{
		New: func() any { return new(bytes.Buffer) },
	}

	headKey = datastore.NewKey("head")
)

type (
	dsPublisher struct {
		h      *Herald
		locker sync.RWMutex
		ls     ipld.LinkSystem
	}
	pooledBytesBufferCloser struct {
		buf *bytes.Buffer
	}
)

func newDsPublisher(h *Herald) (*dsPublisher, error) {
	var ds dsPublisher
	ds.h = h
	ds.ls = cidlink.DefaultLinkSystem()
	ds.ls.StorageReadOpener = ds.storageReadOpener
	ds.ls.StorageWriteOpener = ds.storageWriteOpener
	return &ds, nil
}

func (l *dsPublisher) storageWriteOpener(ctx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
	buf := bytesBuffers.Get().(*bytes.Buffer)
	buf.Reset()
	return buf, func(lnk ipld.Link) error {
		defer bytesBuffers.Put(buf)
		return l.h.ds.Put(ctx.Ctx, dsKey(lnk), buf.Bytes())
	}, nil
}

func (l *dsPublisher) storageReadOpener(ctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
	val, err := l.h.ds.Get(ctx.Ctx, dsKey(lnk))
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(val), nil
}

func dsKey(l ipld.Link) datastore.Key {
	return datastore.NewKey(l.(cidlink.Link).Cid.String())
}

func (l *dsPublisher) Publish(ctx context.Context, catalog Catalog) (cid.Cid, error) {
	entries, err := l.generateEntries(ctx, catalog)
	if err != nil {
		return cid.Undef, err
	}
	return l.generateAdvertisement(ctx, catalog.ID(), entries, false)
}

func (l *dsPublisher) generateEntriesChunk(ctx context.Context, next ipld.Link, mhs []multihash.Multihash) (ipld.Link, error) {
	chunk, err := schema.EntryChunk{
		Entries: mhs,
		Next:    next,
	}.ToNode()
	if err != nil {
		return nil, err
	}
	return l.ls.Store(ipld.LinkContext{Ctx: ctx}, schema.Linkproto, chunk)
}

func (l *dsPublisher) generateEntries(ctx context.Context, catalog Catalog) (ipld.Link, error) {
	mhs := make([]multihash.Multihash, 0, l.h.adEntriesChunkSize)
	var next ipld.Link
	var mhCount, chunkCount int
	for iter := catalog.Iterator(); !iter.Done(); {
		mh, err := iter.Next()
		if err != nil {
			return nil, err
		}
		mhs = append(mhs, mh)
		mhCount++
		if len(mhs) >= l.h.adEntriesChunkSize {
			next, err = l.generateEntriesChunk(ctx, next, mhs)
			if err != nil {
				return nil, err
			}
			chunkCount++
			mhs = mhs[:0]
		}
	}
	if len(mhs) != 0 {
		var err error
		next, err = l.generateEntriesChunk(ctx, next, mhs)
		if err != nil {
			return nil, err
		}
		chunkCount++
	}
	logger.Infow("Generated linked chunks of multihashes", "link", next, "totalMhCount", mhCount, "chunkCount", chunkCount)
	return next, nil
}

func (l *dsPublisher) Retract(ctx context.Context, id CatalogID) (cid.Cid, error) {
	// TODO: find removed entries and remove from the datastore
	return l.generateAdvertisement(ctx, id, schema.NoEntries, true)
}

func (l *dsPublisher) generateAdvertisement(ctx context.Context, id CatalogID, entries ipld.Link, isRm bool) (cid.Cid, error) {
	l.locker.Lock()
	defer l.locker.Unlock()

	var previousID ipld.Link
	if head, err := l.GetHead(ctx); err != nil {
		return cid.Undef, err
	} else if !cid.Undef.Equals(head) {
		previousID = cidlink.Link{Cid: head}
	}
	ad := schema.Advertisement{
		PreviousID: previousID,
		Provider:   l.h.id.String(),
		Addresses:  l.h.providerAddrs,
		Entries:    entries,
		ContextID:  id,
		Metadata:   l.h.metadata,
		IsRm:       isRm,
	}
	if err := ad.Sign(l.h.identity); err != nil {
		logger.Errorw("failed to sign advertisement", "err", err)
		return cid.Undef, err
	}
	adNode, err := ad.ToNode()
	if err != nil {
		logger.Errorw("failed to generate IPLD node from advertisement", "err", err)
		return cid.Undef, err
	}
	adLink, err := l.ls.Store(ipld.LinkContext{Ctx: ctx}, schema.Linkproto, adNode)
	if err != nil {
		logger.Errorw("failed to store advertisement", "err", err)
		return cid.Undef, err
	}

	newHead := adLink.(cidlink.Link).Cid
	if err := l.h.ds.Put(ctx, headKey, newHead.Bytes()); err != nil {
		logger.Errorw("failed to set new head", "newHead", newHead, "err", err)
		return cid.Undef, err
	}
	return newHead, nil
}

func (l *dsPublisher) GetContent(ctx context.Context, cid cid.Cid) (io.ReadCloser, error) {
	key := dsKey(cidlink.Link{Cid: cid})
	switch value, err := l.h.ds.Get(ctx, key); {
	case errors.Is(err, datastore.ErrNotFound):
		return nil, ErrContentNotFound
	case err != nil:
		return nil, err
	default:
		buf := bytesBuffers.Get().(*bytes.Buffer)
		buf.Reset()
		buf.Grow(len(value))
		_, _ = buf.Write(value)
		return &pooledBytesBufferCloser{buf: buf}, err
	}
}

func (l *dsPublisher) GetHead(ctx context.Context) (cid.Cid, error) {
	switch value, err := l.h.ds.Get(ctx, headKey); {
	case errors.Is(err, datastore.ErrNotFound):
		return cid.Undef, nil
	case err != nil:
		return cid.Undef, err
	default:
		_, head, err := cid.CidFromBytes(value)
		if err != nil {
			logger.Errorw("failed to decode stored head as CID", "err", err)
		}
		return head, nil
	}
}

func (c *pooledBytesBufferCloser) Read(b []byte) (n int, err error) { return c.buf.Read(b) }

func (c *pooledBytesBufferCloser) Close() error {
	bytesBuffers.Put(c.buf)
	return nil
}
