// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"fmt"
	"sort"

	"github.com/Azure/acr-cli/internal/api"
	"github.com/Azure/acr-cli/internal/arg"
	"github.com/spf13/cobra"
)

type argManifestLoader func(context.Context, arg.Options, []string) (*arg.ManifestCache, error)

type argManifestClient struct {
	api.AcrCLIClientInterface
	cache *arg.ManifestCache
}

func (c *argManifestClient) GetManifest(ctx context.Context, repository, digest string) ([]byte, error) {
	return c.cache.GetManifest(ctx, repository, digest, c.AcrCLIClientInterface.GetManifest)
}

func validateARGFlags(cmd *cobra.Command, params *purgeParameters) error {
	if !params.useARG {
		for _, name := range []string{"arg-registry-id", "arg-subscription", "arg-region", "arg-api-version"} {
			if cmd.Flags().Changed(name) {
				return fmt.Errorf("--%s requires --arg", name)
			}
		}
		return nil
	}
	if !params.dryRun {
		return fmt.Errorf("--arg requires --dry-run; ARG-assisted deletion is not supported")
	}
	if !params.untaggedOnly && !params.untagged {
		return fmt.Errorf("--arg requires --untagged-only or --untagged")
	}
	registryName, err := params.GetRegistryName()
	if err != nil {
		return err
	}
	params.argOptions.RegistryName = registryName
	if err := params.argOptions.Validate(); err != nil {
		return fmt.Errorf("invalid ARG options: %w", err)
	}
	return nil
}

func prepareARGPurgeClient(ctx context.Context, client api.AcrCLIClientInterface, options arg.Options,
	filters map[string]string, load argManifestLoader) (api.AcrCLIClientInterface, *arg.ManifestCache, error) {
	if len(filters) == 0 {
		return client, nil, nil
	}
	repositories := make([]string, 0, len(filters))
	for repository := range filters {
		repositories = append(repositories, repository)
	}
	sort.Strings(repositories)
	cache, err := load(ctx, options, repositories)
	if err != nil {
		return nil, nil, fmt.Errorf("ARG manifest cache initialization failed: %w", err)
	}
	if cache == nil {
		return nil, nil, fmt.Errorf("ARG manifest cache loader returned no cache")
	}
	return &argManifestClient{AcrCLIClientInterface: client, cache: cache}, cache, nil
}

func printARGCacheStats(cmd *cobra.Command, cache *arg.ManifestCache) {
	stats := cache.Stats()
	cmd.Printf("ARG body cache: indexed=%d rejected=%d pages=%d hits=%d registry GET fallbacks=%d (missing=%d invalid=%d)\n",
		stats.CachedBodies, stats.RejectedBodies, stats.QueryPages, stats.Hits,
		stats.MissingFallbacks+stats.InvalidFallbacks, stats.MissingFallbacks, stats.InvalidFallbacks)
}
