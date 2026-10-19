// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package arg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
)

type ManifestCache struct {
	RegistryID string
	bodies     map[identity][]byte
	rejected   map[identity]string
	queryPages int
	hits       atomic.Uint64
	missing    atomic.Uint64
	invalid    atomic.Uint64
}

type ManifestCacheStats struct {
	CachedBodies     int
	RejectedBodies   int
	QueryPages       int
	Hits             uint64
	MissingFallbacks uint64
	InvalidFallbacks uint64
}

func manifestQuery(registryID string, repositories []string) (string, error) {
	if len(repositories) == 0 {
		return "", errors.New("ARG manifest discovery requires at least one repository")
	}
	names := make([]string, 0, len(repositories))
	for _, name := range repositories {
		if name == "" {
			return "", errors.New("ARG repository names must not be empty")
		}
		quoted, err := json.Marshal(name)
		if err != nil {
			return "", err
		}
		names = append(names, string(quoted))
	}
	return scopedArtifactQuery(registryID, " | where kind =~ 'Manifest'"+
		" | where tostring(properties.registryArtifact.repository) in ("+strings.Join(names, ",")+")")
}

func LoadManifestCache(ctx context.Context, options Options, repositories []string) (*ManifestCache, error) {
	if len(repositories) == 0 {
		return nil, errors.New("ARG manifest discovery requires at least one repository")
	}
	c, err := defaultClient(ctx, options)
	if err != nil {
		return nil, err
	}
	captured, err := c.discoverArtifacts(ctx, options, repositories)
	if err != nil {
		return nil, err
	}
	return NewManifestCache(captured)
}

func NewManifestCache(captured Discovery) (*ManifestCache, error) {
	if _, err := registrySubscription(captured.RegistryID); err != nil {
		return nil, err
	}
	cache := &ManifestCache{
		RegistryID: captured.RegistryID, queryPages: captured.QueryPages,
		bodies: make(map[identity][]byte), rejected: make(map[identity]string),
	}
	ids := make(map[string]bool)
	seen := make(map[identity]bool)
	for _, raw := range captured.Data {
		var row struct {
			ID         string `json:"id"`
			Kind       string `json:"kind"`
			Properties struct {
				RegistryArtifact struct {
					Repository string          `json:"repository"`
					Digest     string          `json:"digest"`
					Manifest   json.RawMessage `json:"manifest"`
				} `json:"registryArtifact"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, fmt.Errorf("decode ARG manifest cache record: %w", err)
		}
		if row.Kind != "Manifest" {
			return nil, fmt.Errorf("ARG manifest cache rejects kind %q; historical Tag entities are not supported", row.Kind)
		}
		if !strings.HasPrefix(strings.ToLower(row.ID), strings.ToLower(captured.RegistryID)+"/") || ids[row.ID] {
			return nil, errors.New("ARG manifest cache record ID is duplicated or outside the requested registry")
		}
		ids[row.ID] = true
		item := row.Properties.RegistryArtifact
		if item.Repository == "" || !digestPattern.MatchString(item.Digest) {
			return nil, errors.New("ARG manifest cache record is missing repository/digest")
		}
		key := identity{item.Repository, item.Digest}
		if seen[key] {
			delete(cache.bodies, key)
			cache.rejected[key] = "duplicate indexed manifest identity"
			continue
		}
		seen[key] = true
		normalized, err := normalizeArtifacts([]json.RawMessage{raw}, captured.RegistryID)
		if err == nil {
			for _, child := range normalized[0].Children {
				if !digestPattern.MatchString(child) {
					err = errors.New("indexed manifest has an invalid child digest")
					break
				}
			}
		}
		if err != nil {
			cache.rejected[key] = err.Error()
			continue
		}
		cache.bodies[key] = item.Manifest
	}
	return cache, nil
}

func (c *ManifestCache) Warnings() []string {
	warnings := make([]string, 0, len(c.rejected))
	for key, reason := range c.rejected {
		warnings = append(warnings, fmt.Sprintf("%s@%s: %s", key.repository, key.digest, reason))
	}
	sort.Strings(warnings)
	return warnings
}

func (c *ManifestCache) Stats() ManifestCacheStats {
	return ManifestCacheStats{
		CachedBodies: len(c.bodies), RejectedBodies: len(c.rejected), QueryPages: c.queryPages,
		Hits: c.hits.Load(), MissingFallbacks: c.missing.Load(), InvalidFallbacks: c.invalid.Load(),
	}
}

func (c *ManifestCache) GetManifest(ctx context.Context, repository, digest string,
	fallback func(context.Context, string, string) ([]byte, error)) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := identity{repository, digest}
	if body, ok := c.bodies[key]; ok {
		c.hits.Add(1)
		return bytes.Clone(body), nil
	}
	if _, rejected := c.rejected[key]; rejected {
		c.invalid.Add(1)
	} else {
		c.missing.Add(1)
	}
	body, err := fallback(ctx, repository, digest)
	if err != nil {
		return nil, fmt.Errorf("registry manifest GET fallback for %s@%s: %w", repository, digest, err)
	}
	return body, nil
}
