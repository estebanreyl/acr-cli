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

// ManifestCache serves indexed manifest bodies with registry fallback.
type ManifestCache struct {
	RegistryID string
	bodies     map[identity][]byte
	rejected   map[identity]string
	queryPages int
	hits       atomic.Uint64
	missing    atomic.Uint64
	invalid    atomic.Uint64
}

// ManifestCacheStats reports inventory size, cache hits, and fallback counts.
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

// LoadManifestCache queries ARG for manifest bodies in the selected repositories.
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

// NewManifestCache validates captured inventory and indexes usable manifest bodies.
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
		if err := validateManifestBody(item.Manifest); err != nil {
			cache.rejected[key] = err.Error()
			continue
		}
		cache.bodies[key] = item.Manifest
	}
	return cache, nil
}

func validateManifestBody(raw json.RawMessage) error {
	type descriptor struct {
		Digest    string `json:"digest"`
		MediaType string `json:"mediaType"`
	}
	var body struct {
		SchemaVersion int             `json:"schemaVersion"`
		MediaType     string          `json:"mediaType"`
		Manifests     *[]descriptor   `json:"manifests"`
		Layers        *[]descriptor   `json:"layers"`
		Blobs         *[]descriptor   `json:"blobs"`
		Subject       json.RawMessage `json:"subject"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return fmt.Errorf("decode indexed manifest: %w", err)
	}
	// Schema checks cannot detect omitted children or subjects in otherwise valid indexed bodies.
	switch body.MediaType {
	case "application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json":
		if body.SchemaVersion != 2 || body.Manifests == nil {
			return errors.New("indexed image index/list is missing schemaVersion/manifests")
		}
		for _, child := range *body.Manifests {
			if !digestPattern.MatchString(child.Digest) {
				return errors.New("indexed manifest has an invalid child digest")
			}
			if strings.TrimSpace(child.MediaType) == "" {
				return errors.New("indexed manifest has a child descriptor missing mediaType")
			}
		}
	case "application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json":
		if body.SchemaVersion != 2 || body.Layers == nil {
			return errors.New("indexed image manifest is missing schemaVersion/layers")
		}
	case "application/vnd.oci.artifact.manifest.v1+json":
		if body.Blobs == nil {
			return errors.New("indexed artifact manifest is missing blobs")
		}
	default:
		return fmt.Errorf("unsupported indexed manifest media type %q", body.MediaType)
	}
	if len(body.Subject) > 0 && !bytes.Equal(bytes.TrimSpace(body.Subject), []byte("null")) {
		var subject descriptor
		if err := json.Unmarshal(body.Subject, &subject); err != nil || !digestPattern.MatchString(subject.Digest) {
			return errors.New("indexed manifest has an invalid subject descriptor")
		}
	}
	return nil
}

// Warnings returns sorted rejection reasons for bodies requiring registry fallback.
func (c *ManifestCache) Warnings() []string {
	warnings := make([]string, 0, len(c.rejected))
	for key, reason := range c.rejected {
		warnings = append(warnings, fmt.Sprintf("%s@%s: %s", key.repository, key.digest, reason))
	}
	sort.Strings(warnings)
	return warnings
}

// Stats returns the current cache and fallback counters.
func (c *ManifestCache) Stats() ManifestCacheStats {
	return ManifestCacheStats{
		CachedBodies: len(c.bodies), RejectedBodies: len(c.rejected), QueryPages: c.queryPages,
		Hits: c.hits.Load(), MissingFallbacks: c.missing.Load(), InvalidFallbacks: c.invalid.Load(),
	}
}

// GetManifest returns a cloned cached body or invokes the counted registry fallback.
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
