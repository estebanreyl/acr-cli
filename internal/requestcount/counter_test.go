// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package requestcount

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/go-autorest/autorest"
)

type senderFunc func(*http.Request) (*http.Response, error)

func (f senderFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

func TestRegistryCategoriesAndErrors(t *testing.T) {
	ctx, counter := WithCounter(context.Background())
	expected := errors.New("network unavailable")
	for _, tc := range []struct {
		method, path, category string
	}{
		{"POST", "/oauth2/token", "registry_auth"},
		{"GET", "/acr/v1/_catalog", "catalog"},
		{"GET", "/acr/v1/team/repo/_tags", "tags"},
		{"GET", "/acr/v1/team/repo/_manifests", "manifest_metadata"},
		{"GET", "/v2/team/repo/manifests/sha256:123", "manifest_get"},
		{"DELETE", "/v2/team/repo/manifests/sha256:123", "registry_other"},
	} {
		t.Run(tc.category, func(t *testing.T) {
			req, err := http.NewRequestWithContext(ctx, tc.method, "https://registry.test"+tc.path+"?secret=not-recorded", nil)
			if err != nil {
				t.Fatal(err)
			}
			wrapped := WrapRegistry(ctx, senderFunc(func(actual *http.Request) (*http.Response, error) {
				if actual != req {
					t.Fatal("instrumentation replaced the request")
				}
				return nil, expected
			}))
			if _, err := wrapped.Do(req); !errors.Is(err, expected) {
				t.Fatalf("instrumentation changed the transport error: %v", err)
			}
			if !strings.Contains(counter.Summary(), tc.category+"=1") {
				t.Fatal(counter.Summary())
			}
		})
	}
	if !strings.Contains(counter.Summary(), "total=6 ") || strings.Contains(counter.Summary(), "not-recorded") {
		t.Fatal(counter.Summary())
	}
}

func TestRegistryRetriesCountEveryAttempt(t *testing.T) {
	ctx, counter := WithCounter(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://registry.test/v2/repo/manifests/sha256:123", nil)
	if err != nil {
		t.Fatal(err)
	}
	attempts := 0
	sender := WrapRegistry(ctx, senderFunc(func(*http.Request) (*http.Response, error) {
		attempts++
		code := http.StatusOK
		if attempts == 1 {
			code = http.StatusServiceUnavailable
		}
		return &http.Response{StatusCode: code, Header: make(http.Header), Body: http.NoBody}, nil
	}))
	resp, err := autorest.SendWithSender(sender, req,
		autorest.DoRetryForStatusCodes(1, time.Millisecond, http.StatusServiceUnavailable))
	if err != nil || resp.StatusCode != http.StatusOK || attempts != 2 ||
		!strings.Contains(counter.Summary(), "manifest_get=2 ") || !strings.Contains(counter.Summary(), "total=2 ") {
		t.Fatalf("retry counts incorrect: %s, attempts=%d, err=%v", counter.Summary(), attempts, err)
	}
}

func TestCountersAreConcurrentAndPerRun(t *testing.T) {
	ctx, counter := WithCounter(context.Background())
	_, other := WithCounter(context.Background())
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			FromContext(ctx).Add("arg_queries")
		}()
	}
	group.Wait()
	if !strings.Contains(counter.Summary(), "total=32 ") || !strings.Contains(other.Summary(), "total=0 ") {
		t.Fatalf("per-run counter mismatch: %s, %s", counter.Summary(), other.Summary())
	}
	next := &http.Client{}
	if WrapRegistry(context.Background(), next) != next {
		t.Fatal("disabled instrumentation changed the sender")
	}
}
