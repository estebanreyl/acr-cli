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
	"github.com/Azure/acr-cli/internal/api"
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
		{[]string{"--arg", "--dry-run", "--untagged-only", "--min-untagged-manifests", "1"}, "--min-untagged-manifests requires --ago"},
		{[]string{"--arg", "--dry-run", "--untagged-only", "--max-untagged-manifests", "-1"}, "--max-untagged-manifests must be nonnegative"},
		{[]string{"--arg", "--dry-run", "--untagged-only", "--ago", "1d", "--min-untagged-manifests", "2", "--max-untagged-manifests", "1"}, "--min-untagged-manifests must not exceed"},
		{[]string{"--arg", "--dry-run", "--untagged-only", "--max-tags", "1"}, "--max-tags cannot be combined with --untagged-only"},
		{[]string{"--arg", "--dry-run", "--untagged-only", "--keep", "0", "--max-untagged-manifests", "0"}, "none of the others"},
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
		{"--arg", "--dry-run", "--untagged-only", "--ago", "1d", "--min-untagged-manifests", "2", "--max-untagged-manifests", "5"},
		{"--arg", "--dry-run", "--untagged", "--filter", "repo:.*", "--max-tags", "0", "--max-untagged-manifests", "0"},
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
	for _, tc := range []struct {
		untag          bool
		incompleteBody bool
	}{
		{},
		{untag: true},
		{incompleteBody: true},
		{untag: true, incompleteBody: true},
	} {
		t.Run(fmt.Sprintf("untag=%v/incomplete=%v", tc.untag, tc.incompleteBody), func(t *testing.T) {
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
				cachedBodies := make(map[string]string, len(bodies))
				for digest, body := range bodies {
					cachedBodies[digest] = body
				}
				if tc.incompleteBody {
					cachedBodies[argFixtureDigest(1)] = fmt.Sprintf(
						`{"schemaVersion":2,"mediaType":%q,"manifests":[{"digest":%q}]}`,
						indexType, argFixtureDigest(2))
				}
				cache := argFixtureCache(t, cachedBodies)
				planned := make(map[string]int)
				if tc.untag {
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
					opts := retentionOptions{minTags: -1, maxTags: -1, minManifests: -1, maxManifests: -1, dryRun: true}
					count, removed, err := purgeTags(context.Background(), client, 4, "testregistry.azurecr.io",
						"repo", opts, ".*", 60, false)
					if err != nil || count != 1 {
						t.Fatalf("failed to simulate the tag-deletion phase: %v", err)
					}
					planned = removed
				}
				if !hybrid {
					for digest, body := range bodies {
						client.On("GetManifest", mock.Anything, "repo", digest).Return([]byte(body), nil).Maybe()
					}
				} else if tc.incompleteBody {
					client.On("GetManifest", mock.Anything, "repo", argFixtureDigest(1)).
						Return([]byte(bodies[argFixtureDigest(1)]), nil).Once()
				}
				var manifests []repository.ManifestToDelete
				var err error
				cutoff := time.Now().UTC()
				opts := repository.UntaggedManifestsOptions{
					DryRun: true, DeleteCutoff: &cutoff, MinManifests: -1, MaxManifests: -1,
				}
				if hybrid {
					manifests, err = repository.GetUntaggedManifests(context.Background(), 4,
						&argManifestClient{AcrCLIClientInterface: client, cache: cache}, "repo", opts, planned)
				} else {
					manifests, err = repository.GetUntaggedManifests(context.Background(), 4,
						client, "repo", opts, planned)
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
					if tc.untag {
						want = []string{argFixtureDigest(1), argFixtureDigest(2), argFixtureDigest(3), argFixtureDigest(4)}
					}
					if !reflect.DeepEqual(digests, want) {
						t.Fatalf("unexpected baseline: %v", digests)
					}
				} else {
					var invalidFallbacks uint64
					if tc.incompleteBody {
						invalidFallbacks = 1
					}
					if !reflect.DeepEqual(digests, baseline) || cache.Stats().Hits == 0 ||
						cache.Stats().MissingFallbacks != 0 || cache.Stats().InvalidFallbacks != invalidFallbacks {
						t.Fatalf("hybrid changed current-state protection: %v vs %v, %+v", digests, baseline, cache.Stats())
					}
				}

				client.AssertExpectations(t)
			}
		})
	}
}

func TestARGHybridRetentionParity(t *testing.T) {
	for _, tc := range []struct {
		name          string
		args          []string
		wantTags      []string
		wantManifests []int
	}{
		{"keep untagged", []string{"--untagged-only", "--ago", "0d", "--keep", "1"}, nil, []int{1, 2, 6}},
		{"keep both phases", []string{"--untagged", "--ago", "0d", "--keep", "1"}, []string{"ci-old"}, []int{1, 2, 5, 6}},
		{"minimum protects index children", []string{"--untagged-only", "--ago", "0d", "--min-untagged-manifests", "2"}, nil, []int{6}},
		{"maximum without age", []string{"--untagged-only", "--max-untagged-manifests", "2"}, nil, []int{6}},
		{"age and maximum", []string{"--untagged-only", "--ago", "7d", "--max-untagged-manifests", "2"}, nil, []int{6}},
		{"minimum and maximum both phases", []string{"--untagged", "--ago", "1d", "--min-tags", "1", "--max-tags", "1",
			"--min-untagged-manifests", "2", "--max-untagged-manifests", "3"}, []string{"ci-old"}, []int{5, 6}},
		{"zero maximum both phases", []string{"--untagged", "--max-tags", "0", "--max-untagged-manifests", "0"},
			[]string{"ci-new", "ci-old"}, []int{1, 2, 3, 4, 5, 6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := &purgeParameters{rootParameters: &rootParameters{registryName: "testregistry"}}
			cmd := newPurgeCmdWithARGLoader(params, nil)
			args := append([]string{"--arg", "--dry-run", "--filter", "repo:^ci-"}, tc.args...)
			if err := cmd.ParseFlags(args); err != nil {
				t.Fatal(err)
			}
			if err := validateARGFlags(cmd, params); err != nil {
				t.Fatal(err)
			}
			opts := retentionOptions{
				keep: params.keep, minTags: params.minTags, maxTags: params.maxTags,
				minManifests: params.minUntaggedManifests, maxManifests: params.maxUntaggedManifests, dryRun: true,
			}
			if params.ago != "" {
				duration := mustParseDuration(params.ago)
				opts.agoDuration = &duration
			}
			want := make([]string, 0, len(tc.wantTags)+len(tc.wantManifests))
			for _, tag := range tc.wantTags {
				want = append(want, "testregistry.azurecr.io/repo:"+tag)
			}
			for _, number := range tc.wantManifests {
				want = append(want, "testregistry.azurecr.io/repo@"+argFixtureDigest(number))
			}
			sort.Strings(want)

			var baseline []string
			for _, hybrid := range []bool{false, true} {
				client := &mocks.AcrCLIClientInterface{}
				client.On("IsAbac").Return(false)
				imageType := "application/vnd.oci.image.manifest.v1+json"
				indexType := "application/vnd.oci.image.index.v1+json"
				bodies := make(map[string]string)
				metadata := make([]acr.ManifestAttributesBase, 0, 6)
				now := time.Now().UTC()
				ages := []time.Duration{48 * time.Hour, 96 * time.Hour, time.Hour, 30 * time.Minute, 120 * time.Hour, 240 * time.Hour}
				for i, age := range ages {
					digest, mediaType := argFixtureDigest(i+1), imageType
					body := fmt.Sprintf(`{"schemaVersion":2,"mediaType":%q,"layers":[]}`, imageType)
					if i == 0 {
						mediaType = indexType
						body = fmt.Sprintf(`{"schemaVersion":2,"mediaType":%q,"manifests":[{"mediaType":%q,"digest":%q}]}`,
							indexType, imageType, argFixtureDigest(2))
					}
					tags := []string{}
					if i == 3 {
						tags = []string{"ci-new"}
					} else if i == 4 {
						tags = []string{"ci-old"}
					}
					updated := now.Add(-age).Format(time.RFC3339Nano)
					metadata = append(metadata, acr.ManifestAttributesBase{
						Digest: &digest, MediaType: &mediaType, LastUpdateTime: &updated, Tags: &tags,
					})
					bodies[digest] = body
					if !hybrid {
						client.On("GetManifest", mock.Anything, "repo", digest).Return([]byte(body), nil).Maybe()
					}
				}
				client.On("GetAcrManifests", mock.Anything, "repo", "", "").
					Return(&acr.Manifests{ManifestsAttributes: &metadata}, nil).Once()
				client.On("GetAcrManifests", mock.Anything, "repo", "", argFixtureDigest(6)).
					Return(EmptyListManifestsResult, nil).Once()
				if !params.untaggedOnly {
					tags := []acr.TagAttributesBase{
						{Name: &(*metadata[3].Tags)[0], Digest: metadata[3].Digest, LastUpdateTime: metadata[3].LastUpdateTime},
						{Name: &(*metadata[4].Tags)[0], Digest: metadata[4].Digest, LastUpdateTime: metadata[4].LastUpdateTime},
					}
					client.On("GetAcrTags", mock.Anything, "repo", "timedesc", "").
						Return(&acr.RepositoryTagsType{
							Response:       autorest.Response{Response: &http.Response{StatusCode: http.StatusOK}},
							TagsAttributes: &tags,
						}, nil).Once()
				}
				cache := argFixtureCache(t, bodies)
				var purgeClient api.AcrCLIClientInterface = client
				if hybrid {
					purgeClient = &argManifestClient{AcrCLIClientInterface: client, cache: cache}
				}
				output := capturePurgeOutput(t, func() {
					tags, manifests, err := purge(context.Background(), purgeClient, "testregistry.azurecr.io", 4,
						opts, 60, true, params.untaggedOnly, map[string]string{"repo": "^ci-"}, false)
					if err != nil || tags != len(tc.wantTags) || manifests != len(tc.wantManifests) {
						t.Fatalf("hybrid=%v: tags=%d manifests=%d err=%v", hybrid, tags, manifests, err)
					}
				})
				selected := []string{}
				for _, line := range strings.Split(output, "\n") {
					if strings.HasPrefix(line, "Would delete: ") {
						selected = append(selected, strings.Fields(line)[2])
					}
				}
				sort.Strings(selected)
				if !reflect.DeepEqual(selected, want) {
					t.Fatalf("hybrid=%v: selected %v, want %v", hybrid, selected, want)
				}
				if !hybrid {
					baseline = selected
				} else if !reflect.DeepEqual(selected, baseline) || cache.Stats().Hits == 0 ||
					cache.Stats().MissingFallbacks != 0 || cache.Stats().InvalidFallbacks != 0 {
					t.Fatalf("ARG retention parity failed: %+v", cache.Stats())
				}
				client.AssertExpectations(t)
			}
		})
	}
}
