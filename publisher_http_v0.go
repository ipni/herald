package herald

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/dagsync/ipnisync/head"
)

var (
	_ Publisher = (*httpPublisher)(nil)

	ErrContentNotFound = errors.New("content is not found")

	contentBuffers = sync.Pool{
		New: func() any { return new([1024]byte) },
	}
)

type (
	httpPublisher struct {
		h           *Herald
		server      http.Server
		dsPublisher *dsPublisher
	}
)

func newHttpPublisher(h *Herald, dspub *dsPublisher) (*httpPublisher, error) {
	var pub httpPublisher
	pub.h = h
	pub.server.Handler = pub.serveMux()
	pub.dsPublisher = dspub
	return &pub, nil
}

func (p *httpPublisher) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", p.h.httpPublisherListenAddr)
	if err != nil {
		return err
	}
	go func() {
		if err := p.server.Serve(listener); errors.Is(err, http.ErrServerClosed) {
			logger.Info("HTTP publisher stopped successfully.")
		} else {
			logger.Errorw("HTTP publisher stopped erroneously.", "err", err)
		}
	}()
	logger.Infow("HTTP publisher started successfully.", "address", listener.Addr())
	return nil
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
	h, err := p.GetHead(r.Context())
	if err != nil {
		logger.Errorw("failed to get head CID", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	if cid.Undef.Equals(h) {
		http.Error(w, "", http.StatusNoContent)
		return
	}
	signedHead, err := head.NewSignedHead(h, p.h.topic, p.h.identity)
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
		logger.Debugw("successfully responded with head message", "head", h, "written", written)
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

func (p *httpPublisher) Shutdown(ctx context.Context) error {
	return p.server.Shutdown(ctx)
}
