// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package arg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const cacheImageBody = `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`

func testManifestCache(t *testing.T, rows ...json.RawMessage) *ManifestCache {
	t.Helper()
	cache, err := NewManifestCache(Discovery{RegistryID: testRegistry, Data: rows})
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func TestManifestCachePreservesChildrenAndSubjectsWithoutTagHistory(t *testing.T) {
	parent, child, subject := testArtifact(1), testArtifact(2), testArtifact(3)
	body := fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"digest":%q}],"subject":{"digest":%q}}`,
		child.Digest, subject.Digest)
	cache := testManifestCache(t, nativeArtifact(parent, "Manifest", body))
	fallback := func(context.Context, string, string) ([]byte, error) {
		t.Fatal("cached manifest invoked registry GET")
		return nil, nil
	}
	result, err := cache.GetManifest(context.Background(), parent.Repository, parent.Digest, fallback)
	if err != nil || string(result) != body {
		t.Fatalf("indexed body was changed: %s, %v", result, err)
	}
	result[0] = '!'
	result, err = cache.GetManifest(context.Background(), parent.Repository, parent.Digest, fallback)
	if err != nil || string(result) != body || cache.Stats().Hits != 2 {
		t.Fatalf("caller mutation changed the cached body: %s, %v", result, err)
	}
}

func TestManifestCacheMissingAndInvalidBodiesUseCountedFallback(t *testing.T) {
	for name, body := range map[string]string{
		"malformed":   `"not a manifest object"`,
		"unsupported": `{"schemaVersion":2,"mediaType":"unknown"}`,
		"truncated":   `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json"}`,
		"bad child":   `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"digest":"bad"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			row := testArtifact(1)
			cache := testManifestCache(t, nativeArtifact(row, "Manifest", body))
			expected := errors.New("registry unavailable")
			result, err := cache.GetManifest(context.Background(), row.Repository, row.Digest,
				func(_ context.Context, repository, digest string) ([]byte, error) {
					if repository != row.Repository || digest != row.Digest {
						t.Fatalf("incorrect fallback identity: %s@%s", repository, digest)
					}
					return nil, expected
				})
			stats := cache.Stats()
			if result != nil || !errors.Is(err, expected) || stats.InvalidFallbacks != 1 ||
				stats.CachedBodies != 0 || stats.RejectedBodies != 1 || len(cache.Warnings()) != 1 {
				t.Fatalf("invalid body did not use an explicit fallback: %+v, %v", stats, err)
			}
		})
	}
	t.Run("empty ARG inventory", func(t *testing.T) {
		cache := testManifestCache(t)
		ctx := context.WithValue(context.Background(), struct{}{}, "test")
		result, err := cache.GetManifest(ctx, "repo", testArtifact(1).Digest,
			func(actual context.Context, _, _ string) ([]byte, error) {
				if actual != ctx {
					t.Fatal("fallback context was not propagated")
				}
				return []byte(cacheImageBody), nil
			})
		if err != nil || string(result) != cacheImageBody || cache.Stats().MissingFallbacks != 1 {
			t.Fatalf("empty cache did not fall back: %s, %v", result, err)
		}
	})
}

func TestManifestCacheRepositoryScopeAndDuplicateBodies(t *testing.T) {
	row := testArtifact(1)
	duplicate := row
	duplicate.ID += "-duplicate"
	cache := testManifestCache(t, nativeArtifact(row, "Manifest", cacheImageBody),
		nativeArtifact(duplicate, "Manifest", cacheImageBody))
	if cache.Stats().CachedBodies != 0 || !strings.Contains(cache.Warnings()[0], "duplicate") {
		t.Fatal("ambiguous bodies must not be served from ARG")
	}
	cache = testManifestCache(t, nativeArtifact(row, "Manifest", cacheImageBody))
	calls := 0
	_, err := cache.GetManifest(context.Background(), "other", row.Digest,
		func(context.Context, string, string) ([]byte, error) {
			calls++
			return []byte(cacheImageBody), nil
		})
	if err != nil || calls != 1 || cache.Stats().Hits != 0 {
		t.Fatal("same digest in another repository incorrectly hit the cache")
	}
}

func TestManifestCacheRejectsHistoryAndUnscopedRecords(t *testing.T) {
	row := testArtifact(1)
	tag := row
	tag.Tags = []string{"old"}
	outside := row
	outside.ID = "/another/registry/artifacts/1"
	for name, raw := range map[string]json.RawMessage{
		"historical tag": nativeArtifact(tag, "Tag", "null"),
		"outside scope":  nativeArtifact(outside, "Manifest", cacheImageBody),
		"invalid JSON":   json.RawMessage(`{`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewManifestCache(Discovery{RegistryID: testRegistry, Data: []json.RawMessage{raw}}); err == nil {
				t.Fatal("invalid manifest inventory accepted")
			}
		})
	}
}

func TestManifestCacheConcurrentReadsAndCancellation(t *testing.T) {
	row := testArtifact(1)
	cache := testManifestCache(t, nativeArtifact(row, "Manifest", cacheImageBody))
	var fallbackCalls atomic.Uint64
	fallback := func(context.Context, string, string) ([]byte, error) {
		fallbackCalls.Add(1)
		return []byte(cacheImageBody), nil
	}
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := cache.GetManifest(context.Background(), row.Repository, row.Digest, fallback); err != nil {
				t.Error(err)
			}
			if _, err := cache.GetManifest(context.Background(), row.Repository, testArtifact(2).Digest, fallback); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cache.GetManifest(ctx, row.Repository, row.Digest, fallback); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached GET ignored cancellation: %v", err)
	}
	stats := cache.Stats()
	if stats.Hits != 32 || stats.MissingFallbacks != 32 || fallbackCalls.Load() != 32 {
		t.Fatalf("concurrent request counts are incorrect: %+v", stats)
	}
}
