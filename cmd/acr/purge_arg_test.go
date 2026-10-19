// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Azure/acr-cli/acr"
	"github.com/Azure/acr-cli/cmd/mocks"
	"github.com/Azure/acr-cli/cmd/repository"
	"github.com/Azure/acr-cli/internal/arg"
	"github.com/Azure/go-autorest/autorest"
	"github.com/stretchr/testify/mock"
)

const argTestRegistry = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/test/providers/Microsoft.ContainerRegistry/registries/testregistry"

func TestPurgeRequestCountsAreOptInAndPrintOnAuthenticationFailure(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%v", enabled), func(t *testing.T) {
			params := &purgeParameters{rootParameters: &rootParameters{
				registryName: "testregistry", username: "user",
			}}
			cmd := newPurgeCmdWithARGLoader(params, nil)
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			args := []string{"--dry-run", "--untagged-only"}
			if enabled {
				args = append(args, "--debug-request-counts")
			}
			cmd.SetArgs(args)
			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), "missing identity token or password") {
				t.Fatalf("expected local credential failure: %v", err)
			}
			if strings.Contains(output.String(), "HTTP request attempts: total=0 ") != enabled {
				t.Fatalf("counter opt-in/error output incorrect: %s", output.String())
			}
		})
	}
}

func TestARGHybridRejectsInvalidOptionsBeforeClients(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--arg", "--untagged-only"}, "--dry-run"},
		{[]string{"--arg", "--dry-run", "--filter", "repo:.*", "--ago", "1d"}, "--untagged"},
		{[]string{"--arg", "--dry-run", "--untagged-only", "--arg-registry-id", "invalid"}, "invalid ARG options"},
		{[]string{"--arg", "--dry-run", "--untagged-only", "--arg-region", "https://invalid"}, "invalid ARG options"},
		{[]string{"--arg", "--dry-run", "--untagged-only", "--arg-subscription", "invalid"}, "invalid ARG options"},
		{[]string{"--untagged-only", "--arg-registry-id", argTestRegistry}, "requires --arg"},
		{[]string{"--untagged-only", "--arg-api-version", "2024-04-01"}, "requires --arg"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			called := false
			params := &purgeParameters{rootParameters: &rootParameters{registryName: "testregistry"}}
			cmd := newPurgeCmdWithARGLoader(params, func(context.Context, arg.Options, []string) (*arg.ManifestCache, error) {
				called = true
				return nil, errors.New("unexpected ARG request")
			})
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			cmd.SetArgs(tc.args)
			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), tc.want) || called {
				t.Fatalf("expected %q before client use, got %v (called=%v)", tc.want, err, called)
			}
		})
	}
}

func TestARGHybridAcceptsOrdinaryPurgeFlagsAndRegistryCredentials(t *testing.T) {
	for _, args := range [][]string{
		{"--arg", "--dry-run", "--untagged-only"},
		{"--arg", "--dry-run", "--untagged", "--filter", "repo:.*", "--ago", "1d"},
		{"--arg", "--dry-run", "--untagged-only", "--keep", "2", "--include-locked",
			"--concurrency", "2", "--repository-page-size", "50", "--verbose", "--config", "test.json"},
	} {
		params := &purgeParameters{rootParameters: &rootParameters{
			registryName: "testregistry", username: "user", password: "password",
		}}
		cmd := newPurgeCmdWithARGLoader(params, nil)
		if err := cmd.ParseFlags(args); err != nil {
			t.Fatal(err)
		}
		if err := validateARGFlags(cmd, params); err != nil {
			t.Fatalf("hybrid rejected existing purge options: %v", err)
		}
		if params.argOptions.RegistryName != "testregistry" || params.argOptions.RegistryID != "" {
			t.Fatalf("automatic ARG scope resolution was lost: %+v", params.argOptions)
		}
	}
}

func TestARGHybridLoaderScopeContextAndErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &mocks.AcrCLIClientInterface{}
	cache := argFixtureCache(t, nil)
	options := arg.Options{RegistryName: "testregistry"}
	wrapped, loaded, err := prepareARGPurgeClient(ctx, client, options,
		map[string]string{"team/b": "ignored", "team/a": ".*"},
		func(actual context.Context, actualOptions arg.Options, repositories []string) (*arg.ManifestCache, error) {
			if actual != ctx || actualOptions != options || !reflect.DeepEqual(repositories, []string{"team/a", "team/b"}) {
				t.Fatalf("loader scope/context mismatch: %+v, %v", actualOptions, repositories)
			}
			return cache, nil
		})
	if err != nil || loaded != cache || wrapped == client {
		t.Fatalf("cache client was not installed: %v", err)
	}
	expected := errors.New("ARG authorization failed")
	if _, _, err := prepareARGPurgeClient(ctx, client, options, map[string]string{"repo": ""},
		func(context.Context, arg.Options, []string) (*arg.ManifestCache, error) {
			return nil, expected
		}); !errors.Is(err, expected) {
		t.Fatalf("ARG request error was hidden by a registry fallback: %v", err)
	}
	called := false
	wrapped, loaded, err = prepareARGPurgeClient(ctx, client, options, nil,
		func(context.Context, arg.Options, []string) (*arg.ManifestCache, error) {
			called = true
			return cache, nil
		})
	if err != nil || loaded != nil || wrapped != client || called {
		t.Fatal("empty repository selection performed an ARG request")
	}
}

func TestARGHybridBodyMissPreservesRegistryFailure(t *testing.T) {
	client := &mocks.AcrCLIClientInterface{}
	digest := argFixtureDigest(1)
	expected := errors.New("registry manifest unavailable")
	client.On("GetManifest", mock.Anything, "repo", digest).Return(nil, expected).Once()
	cache := argFixtureCache(t, nil)
	wrapped := &argManifestClient{AcrCLIClientInterface: client, cache: cache}
	if _, err := wrapped.GetManifest(context.Background(), "repo", digest); !errors.Is(err, expected) {
		t.Fatalf("registry fallback failure was lost: %v", err)
	}
	if cache.Stats().MissingFallbacks != 1 {
		t.Fatalf("registry GET fallback was not counted: %+v", cache.Stats())
	}
	client.AssertExpectations(t)
}

func argFixtureDigest(number int) string {
	return fmt.Sprintf("sha256:%064x", number)
}

func argFixtureCache(t *testing.T, bodies map[string]string) *arg.ManifestCache {
	t.Helper()
	captured := arg.Discovery{RegistryID: argTestRegistry}
	for digest, body := range bodies {
		raw, err := json.Marshal(map[string]interface{}{
			"id": argTestRegistry + "/artifacts/" + digest, "kind": "Manifest",
			"properties": map[string]interface{}{"registryArtifact": map[string]interface{}{
				"repository": "repo", "digest": digest, "manifest": json.RawMessage(body),
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		captured.Data = append(captured.Data, raw)
	}
	cache, err := arg.NewManifestCache(captured)
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func TestARGHybridPreservesTaggedNestedChildrenReferrersAndPlannedUntags(t *testing.T) {
	imageType := "application/vnd.oci.image.manifest.v1+json"
	indexType := "application/vnd.oci.image.index.v1+json"
	imageBody := fmt.Sprintf(`{"schemaVersion":2,"mediaType":%q,"layers":[]}`, imageType)
	bodies := map[string]string{
		argFixtureDigest(1): fmt.Sprintf(`{"schemaVersion":2,"mediaType":%q,"manifests":[{"mediaType":%q,"digest":%q}]}`,
			indexType, indexType, argFixtureDigest(2)),
		argFixtureDigest(2): fmt.Sprintf(`{"schemaVersion":2,"mediaType":%q,"manifests":[{"mediaType":%q,"digest":%q}]}`,
			indexType, imageType, argFixtureDigest(3)),
		argFixtureDigest(3): imageBody,
		argFixtureDigest(4): imageBody,
		argFixtureDigest(5): fmt.Sprintf(`{"schemaVersion":2,"mediaType":%q,"layers":[],"subject":{"digest":%q}}`,
			imageType, argFixtureDigest(4)),
		argFixtureDigest(6): imageBody, // Indexed but absent from the live registry listing.
	}
	for _, untag := range []bool{false, true} {
		t.Run(fmt.Sprintf("untag=%v", untag), func(t *testing.T) {
			var baseline []string
			for _, hybrid := range []bool{false, true} {
				metadata := make([]acr.ManifestAttributesBase, 0, 5)
				old := "2000-01-01T00:00:00Z"
				enabled := true
				for number := 1; number <= 5; number++ {
					digest, mediaType := argFixtureDigest(number), imageType
					tags := []string{}
					if number <= 2 {
						mediaType = indexType
					}
					if number == 1 {
						tags = []string{"keep"}
					}
					metadata = append(metadata, acr.ManifestAttributesBase{
						Digest: &digest, MediaType: &mediaType, Tags: &tags, LastUpdateTime: &old,
						ChangeableAttributes: &acr.ChangeableAttributes{DeleteEnabled: &enabled, WriteEnabled: &enabled},
					})
				}
				client := &mocks.AcrCLIClientInterface{}
				client.On("GetAcrManifests", mock.Anything, "repo", "", "").
					Return(&acr.Manifests{ManifestsAttributes: &metadata}, nil).Once()
				empty := []acr.ManifestAttributesBase{}
				client.On("GetAcrManifests", mock.Anything, "repo", "", argFixtureDigest(5)).
					Return(&acr.Manifests{ManifestsAttributes: &empty}, nil).Once()
				cache := argFixtureCache(t, bodies)
				planned := make(map[string]int)
				if untag {
					name, digest := "keep", argFixtureDigest(1)
					tags := []acr.TagAttributesBase{{
						Name: &name, Digest: &digest, LastUpdateTime: &old,
						ChangeableAttributes: &acr.ChangeableAttributes{DeleteEnabled: &enabled, WriteEnabled: &enabled},
					}}
					client.On("GetAcrTags", mock.Anything, "repo", "timedesc", "").
						Return(&acr.RepositoryTagsType{
							Response:       autorest.Response{Response: &http.Response{StatusCode: http.StatusOK}},
							TagsAttributes: &tags,
						}, nil).Once()
					count, removed, err := purgeTags(context.Background(), client, 4, "testregistry.azurecr.io",
						"repo", 0, ".*", 0, 60, true, false)
					if err != nil || count != 1 {
						t.Fatalf("failed to simulate the tag-deletion phase: %v", err)
					}
					planned = removed
				}
				if !hybrid {
					for digest, body := range bodies {
						client.On("GetManifest", mock.Anything, "repo", digest).Return([]byte(body), nil).Maybe()
					}
				}
				var manifests []acr.ManifestAttributesBase
				var err error
				cutoff := time.Now().UTC()
				if hybrid {
					manifests, err = repository.GetUntaggedManifests(context.Background(), 4,
						&argManifestClient{AcrCLIClientInterface: client, cache: cache}, "repo", false, planned, true, false, &cutoff)
				} else {
					manifests, err = repository.GetUntaggedManifests(context.Background(), 4,
						client, "repo", false, planned, true, false, &cutoff)
				}
				if err != nil {
					t.Fatal(err)
				}
				var digests []string
				for _, manifest := range manifests {
					digests = append(digests, *manifest.Digest)
				}
				sort.Strings(digests)
				if !hybrid {
					baseline = digests
					want := []string{argFixtureDigest(4)}
					if untag {
						want = []string{argFixtureDigest(1), argFixtureDigest(2), argFixtureDigest(3), argFixtureDigest(4)}
					}
					if !reflect.DeepEqual(digests, want) {
						t.Fatalf("unexpected baseline: %v", digests)
					}
				} else if !reflect.DeepEqual(digests, baseline) ||
					cache.Stats().Hits == 0 || cache.Stats().MissingFallbacks != 0 || cache.Stats().InvalidFallbacks != 0 {
					t.Fatalf("hybrid changed current-state protection: %v vs %v, %+v", digests, baseline, cache.Stats())
				}
				client.AssertExpectations(t)
			}
		})
	}
}
