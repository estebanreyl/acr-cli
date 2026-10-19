// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

// Package requestcount provides opt-in HTTP attempt counters for purge experiments.
package requestcount

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

type contextKey struct{}

type Counter struct {
	mu     sync.Mutex
	counts map[string]uint64
}

func WithCounter(ctx context.Context) (context.Context, *Counter) {
	counter := &Counter{counts: make(map[string]uint64)}
	return context.WithValue(ctx, contextKey{}, counter), counter
}

func FromContext(ctx context.Context) *Counter {
	counter, _ := ctx.Value(contextKey{}).(*Counter)
	return counter
}

func (c *Counter) Add(category string) {
	c.mu.Lock()
	c.counts[category]++
	c.mu.Unlock()
}

func (c *Counter) Summary() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var total uint64
	for _, count := range c.counts {
		total += count
	}
	return fmt.Sprintf("HTTP request attempts: total=%d registry_auth=%d catalog=%d tags=%d manifest_metadata=%d manifest_get=%d registry_other=%d arg_queries=%d (includes retries; excludes ARM credential acquisition, external processes, and redirects)",
		total, c.counts["registry_auth"], c.counts["catalog"], c.counts["tags"],
		c.counts["manifest_metadata"], c.counts["manifest_get"], c.counts["registry_other"], c.counts["arg_queries"])
}

type Sender interface {
	Do(*http.Request) (*http.Response, error)
}

type registrySender struct {
	next    Sender
	counter *Counter
}

func WrapRegistry(ctx context.Context, next Sender) Sender {
	if counter := FromContext(ctx); counter != nil {
		return &registrySender{next: next, counter: counter}
	}
	return next
}

func (s *registrySender) Do(req *http.Request) (*http.Response, error) {
	category := "registry_other"
	path := req.URL.Path
	switch {
	case strings.HasPrefix(path, "/oauth2/"):
		category = "registry_auth"
	case req.Method == http.MethodGet && (path == "/acr/v1/_catalog" || path == "/v2/_catalog"):
		category = "catalog"
	case req.Method == http.MethodGet && strings.HasPrefix(path, "/acr/v1/") && strings.HasSuffix(path, "/_tags"):
		category = "tags"
	case req.Method == http.MethodGet && strings.HasPrefix(path, "/acr/v1/") && strings.HasSuffix(path, "/_manifests"):
		category = "manifest_metadata"
	case req.Method == http.MethodGet && strings.HasPrefix(path, "/v2/") && strings.Contains(path, "/manifests/"):
		category = "manifest_get"
	}
	s.counter.Add(category)
	return s.next.Do(req)
}
