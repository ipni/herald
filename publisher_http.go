package herald

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	headschema "github.com/ipni/go-libipni/dagsync/ipnisync/head"
)

var (
	_ Publisher = (*httpPublisher)(nil)

	ErrContentNotFound = errors.New("content is not found")

	contentBuffers = sync.Pool{
		New: func() any { return new([1024]byte) },
	}
)

type (
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
	httpPublisher struct {
		h           *Herald
		server      http.Server
		dsPublisher *dsPublisher
	}
)

func newHttpPublisher(h *Herald) (*httpPublisher, error) {
	var pub httpPublisher
	pub.h = h
	pub.server.Handler = pub.serveMux()
	return &pub, nil
}

func (p *httpPublisher) serveMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/head", p.handleGetHead)
	mux.HandleFunc("/*", p.handleGetContent)
	return mux
}

func (p *httpPublisher) handleGetHead(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	head, err := p.GetHead(r.Context())
	if err != nil {
		logger.Errorw("failed to get head CID", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	if cid.Undef.Equals(head) {
		http.Error(w, "", http.StatusNoContent)
		return
	}
	signedHead, err := headschema.NewSignedHead(head, p.h.topic, p.h.identity)
	if err != nil {
		logger.Errorw("failed to generate signed head message", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	resp, err := signedHead.Encode()
	if err != nil {
		logger.Errorw("failed to encode signed head message", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if written, err := w.Write(resp); err != nil {
		logger.Errorw("failed to write encoded head response", "written", written, "err", err)
	} else {
		logger.Debugw("successfully responded with head message", "head", head, "written", written)
	}
}

func (p *httpPublisher) handleGetContent(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	pathParam := strings.TrimPrefix("/", r.URL.RawPath)
	id, err := cid.Decode(pathParam)
	if err != nil {
		logger.Debugw("invalid CID as path parameter while getting content", "pathParam", pathParam, "err", err)
		http.Error(w, "invalid CID: "+pathParam, http.StatusBadRequest)
		return
	}
	switch body, err := p.GetContent(r.Context(), id); {
	case errors.Is(err, ErrContentNotFound):
		http.Error(w, "", http.StatusNotFound)
		return
	case err != nil:
		logger.Errorw("failed to get content from store", "id", id, "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	default:
		defer func() {
			if err := body.Close(); err != nil {
				logger.Debugw("failed to close reader for content", "id", id, "err", err)
			}
		}()
		switch id.Prefix().Codec {
		case cid.DagJSON:
			w.Header().Set("Content-Type", "application/json")
		case cid.DagCBOR:
			w.Header().Set("Content-Type", "application/cbor")
		}
		buf := contentBuffers.Get().(*[1024]byte)
		defer contentBuffers.Put(buf)
		if written, err := io.CopyBuffer(w, body, buf[:]); err != nil {
			logger.Errorw("failed to write content response", "written", written, "err", err)
		} else {
			logger.Debugw("successfully responded with content", "id", id, "written", written)
		}
	}
}

func (p *httpPublisher) Publish(ctx context.Context, catalog Catalog) (cid.Cid, error) {
	return p.dsPublisher.Publish(ctx, catalog)
}

func (p *httpPublisher) Retract(ctx context.Context, id CatalogID) (cid.Cid, error) {
	return p.dsPublisher.Retract(ctx, id)
}

func (p *httpPublisher) GetContent(ctx context.Context, id cid.Cid) (io.ReadCloser, error) {
	return p.dsPublisher.GetContent(ctx, id)
}

func (p *httpPublisher) GetHead(ctx context.Context) (cid.Cid, error) {
	return p.dsPublisher.GetHead(ctx)
}
