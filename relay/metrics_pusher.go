package relay

import (
	"bytes"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	DefaultHTTPInterval   = 10 * time.Second
	DefaultMaxBatchPoints = 5000
)

type MetricsPusherConfig struct {
	Outputs        []HTTPOutputConfig
	Interval       int
	MaxBatchPoints int
}

type MetricsPusher struct {
	backends  []*httpBackend
	batchLock sync.RWMutex
	done      chan interface{}
	wg        sync.WaitGroup
	interval  time.Duration
}

func NewMetricsPusher(cfg OutputConfig) (*MetricsPusher, error) {

	interval := DefaultHTTPInterval
	if cfg.Interval != "" {
		i, err := time.ParseDuration(cfg.Interval)
		if err != nil {
			return nil, fmt.Errorf("error parsing HTTP interval '%v'", err)
		}
		interval = i
	}

	backends := []*httpBackend{}
	for i := range cfg.Outputs {
		backend, err := newHTTPBackend(&cfg.Outputs[i])
		if err != nil {
			return nil, err
		}

		backends = append(backends, backend)
	}

	return &MetricsPusher{
		backends:       backends,
		interval:       interval,
		done:           make(chan interface{}),
	}, nil

}

func (p *MetricsPusher) Run() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.SendBatches()
			case <-p.done:
				p.SendBatches()
				return
			}
		}
	}()
}

func (p *MetricsPusher) Stop() {
	close(p.done)
	p.wg.Wait()
}

func (p *MetricsPusher) Add(queryParams url.Values, authHeader string, outBuf *bytes.Buffer) error {
	p.batchLock.Lock()
	
	h, err := p.GetBackend(queryParams)
	if err != nil {
		log.Println("Error : ", err)
		return err
	}

	b := h.GetBatchList(queryParams, authHeader)

	b.AddBytes(outBuf)
	p.batchLock.Unlock()
	return nil
}

func (p *MetricsPusher) GetBackend(queryParams url.Values) (*httpBackend, error) {
	db := queryParams.Get("db")
	for _, h := range p.backends {
		for _, hdb := range h.databases {
			if db == hdb {
				return h, nil
			}
		}
	}
	return nil, fmt.Errorf("Database %s is not configured", db)
}

func (h *httpBackend) GetBatchList(queryParams url.Values, authHeader string) *BatchList {
	for _, b := range h.batchLists {
		if b.queryParams.Encode() == queryParams.Encode() && b.authHeader == authHeader {
			return b
		}
	}
	newBatchList := &BatchList{
		queryParams: queryParams,
		authHeader:  authHeader,
		batches: []*Batch{
			&Batch{
				buf:      new(bytes.Buffer),
				nbPoints: 0,
			},
		},
		maxBatchPoints: h.maxBatchPoints,
	}
	h.batchLists = append(h.batchLists, newBatchList)
	return newBatchList
}

func (p *MetricsPusher) SendBatches() {

	var wg sync.WaitGroup
	wg.Add(len(p.backends))

	for _, h := range p.backends {
		h := h
		go func() {
			defer wg.Done()
			for _, list := range h.batchLists {
				query := list.queryParams.Encode()
				authHeader := list.authHeader
				for _, batch := range list.batches {
					log.Printf("Writing %d points to %q in database %q", batch.nbPoints, h.name, list.queryParams.Get("db"))
					resp, err := h.post(batch.buf.Bytes(), query, authHeader)
					if err != nil {
						log.Printf("Problem posting to relay backend %q: %v", h.name, err)
					} else {
						if resp.StatusCode/100 == 5 {
							log.Printf("5xx response for relay backend %q: %v", h.name, resp.StatusCode)
							log.Printf("5xx response for relay backend %q: %q", h.name, resp.ContentType)
						}
					}
				}
			}
			h.batchLists = h.batchLists[:0]
		}()
	}

}

func newHTTPBackend(cfg *HTTPOutputConfig) (*httpBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	maxBatchPoints := DefaultMaxBatchPoints
	if cfg.MaxBatchPoints != 0 {
		maxBatchPoints = cfg.MaxBatchPoints
	}

	timeout := DefaultHTTPTimeout
	if cfg.Timeout != "" {
		t, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("error parsing HTTP timeout '%v'", err)
		}
		timeout = t
	}

	var p poster = newSimplePoster(cfg.Location, timeout, cfg.SkipTLSVerification)

	// If configured, create a retryBuffer per backend.
	// This way we serialize retries against each backend.
	if cfg.BufferSizeMB > 0 {
		max := DefaultMaxDelayInterval
		if cfg.MaxDelayInterval != "" {
			m, err := time.ParseDuration(cfg.MaxDelayInterval)
			if err != nil {
				return nil, fmt.Errorf("error parsing max retry time %v", err)
			}
			max = m
		}

		batch := DefaultBatchSizeKB * KB
		if cfg.MaxBatchKB > 0 {
			batch = cfg.MaxBatchKB * KB
		}

		p = newRetryBuffer(cfg.BufferSizeMB*MB, batch, max, p)
	}

	return &httpBackend{
		poster:         p,
		name:           cfg.Name,
		databases:      cfg.Databases,
		batchLists:     []*BatchList{},
		maxBatchPoints: maxBatchPoints,
	}, nil
}

type httpBackend struct {
	poster
	name           string
	databases      []string
	batchLists     []*BatchList
	maxBatchPoints int
}

type BatchList struct {
	authHeader     string
	queryParams    url.Values
	batches        []*Batch
	maxBatchPoints int
}

type Batch struct {
	buf      *bytes.Buffer
	nbPoints int
}

func (b *BatchList) AddBytes(outBuf *bytes.Buffer) {
	points := strings.Split(outBuf.String(), "\n")
	points = points[:len(points)-1]
	nbPoints := len(points)

	lastBatch := b.batches[len(b.batches)-1]

	nbPointsToAdd := 0
	if b.maxBatchPoints-lastBatch.nbPoints < nbPoints {
		nbPointsToAdd = b.maxBatchPoints - lastBatch.nbPoints
	} else {
		nbPointsToAdd = nbPoints
	}

	lastBatch.buf.WriteString(strings.Join(points[:nbPointsToAdd], "\n") + "\n")
	lastBatch.nbPoints += nbPointsToAdd
	points = points[nbPointsToAdd:]

	for len(points) > 0 {
		if len(points) < b.maxBatchPoints {
			nbPointsToAdd = len(points)
		} else {
			nbPointsToAdd = b.maxBatchPoints
		}
		newBatch := &Batch{
			buf:      bytes.NewBufferString(strings.Join(points[:nbPointsToAdd], "\n") + "\n"),
			nbPoints: nbPointsToAdd,
		}
		b.batches = append(b.batches, newBatch)
		points = points[nbPointsToAdd:]
	}
}
