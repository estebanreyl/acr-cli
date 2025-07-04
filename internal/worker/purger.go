// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package worker

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/Azure/acr-cli/acr"
	"github.com/Azure/acr-cli/cmd/common"
	"github.com/Azure/acr-cli/internal/api"
	"github.com/alitto/pond/v2"
)

// Purger purges tags or manifests concurrently.
type Purger struct {
	Executer
	acrClient api.AcrCLIClientInterface
}

// NewPurger creates a new Purger. Purgers are currently repository specific
func NewPurger(repoParallelism int, acrClient api.AcrCLIClientInterface, loginURL string, repoName string) *Purger {
	executeBase := Executer{
		pool:     pond.NewPool(repoParallelism, pond.WithQueueSize(repoParallelism*3), pond.WithNonBlocking(false)),
		loginURL: loginURL,
		repoName: repoName,
	}
	return &Purger{
		Executer:  executeBase,
		acrClient: acrClient,
	}
}

// PurgeTags purges a list of tags concurrently, and returns a count of deleted tags and the first error occurred.
func (p *Purger) PurgeTags(ctx context.Context, tags []acr.TagAttributesBase) (int, error) {
	var deletedTags atomic.Int64 // Count of successfully deleted tags
	group := p.pool.NewGroup()
	for _, tag := range tags {
		group.SubmitErr(func() error {
			resp, err := p.acrClient.DeleteAcrTag(ctx, p.repoName, *tag.Name)
			if err == nil {
				fmt.Printf("Deleted %s/%s:%s\n", p.loginURL, p.repoName, *tag.Name)
				// Increment the count of successfully deleted tags atomically
				deletedTags.Add(1)
				return nil
			}

			if resp != nil && resp.Response != nil && resp.StatusCode == http.StatusNotFound {
				// If the tag is not found it can be assumed to have been deleted.
				deletedTags.Add(1)
				fmt.Printf("Skipped %s/%s:%s, HTTP status: %d\n", p.loginURL, p.repoName, *tag.Name, resp.StatusCode)
				return nil
			}

			fmt.Printf("Failed to delete %s/%s:%s, error: %v\n", p.loginURL, p.repoName, *tag.Name, err)
			return err
		})
	}
	err := group.Wait() // Error should be nil
	return int(deletedTags.Load()), err
}

// deleteManifest handles the deletion of a single manifest with proper error handling and logging
func (p *Purger) deleteManifest(ctx context.Context, manifestDigest string, deletedCount *atomic.Int64) error {
	resp, err := p.acrClient.DeleteManifest(ctx, p.repoName, manifestDigest)
	if err == nil {
		fmt.Printf("Deleted %s/%s@%s\n", p.loginURL, p.repoName, manifestDigest)
		deletedCount.Add(1)
		return nil
	}

	if resp != nil && resp.Response != nil && resp.StatusCode == http.StatusNotFound {
		// If the manifest is not found it can be assumed to have been deleted.
		deletedCount.Add(1)
		fmt.Printf("Skipped %s/%s@%s, HTTP status: %d\n", p.loginURL, p.repoName, manifestDigest, resp.StatusCode)
		return nil
	}

	fmt.Printf("Failed to delete %s/%s@%s, error: %v\n", p.loginURL, p.repoName, manifestDigest, err)
	return err
}

// PurgeManifests purges a list of manifests concurrently, and returns a count of deleted manifests and the first error occurred.
func (p *Purger) PurgeManifests(ctx context.Context, manifests []string) (int, error) {
	var deletedManifests atomic.Int64 // Count of successfully deleted tags
	group := p.pool.NewGroup()
	for _, manifest := range manifests {
		group.SubmitErr(func() error {
			return p.deleteManifest(ctx, manifest, &deletedManifests)
		})
	}
	err := group.Wait()
	return int(deletedManifests.Load()), err
}

// PurgeUntaggedManifests gets all manifests from the repository, excludes tag-related ones, and purges the remaining manifests.
// The tagRelatedManifests map contains manifest digests that should be skipped (e.g., tagged manifests and their dependencies).
func (p *Purger) PurgeUntaggedManifests(ctx context.Context, toSkipManifests map[string]struct{}) (int, error) {
	var deletedManifests atomic.Int64
	group := p.pool.NewGroup()
	lastManifestDigest := ""

	// Get all manifests from the repository and delete untagged ones directly
	for {
		resultManifests, err := p.acrClient.GetAcrManifests(ctx, p.repoName, "", lastManifestDigest)
		if err != nil {
			if resultManifests != nil && resultManifests.Response.Response != nil && resultManifests.StatusCode == http.StatusNotFound {
				// Repository not found, return 0 deleted
				return 0, nil
			}
			return 0, err
		}

		if resultManifests == nil || resultManifests.ManifestsAttributes == nil || len(*resultManifests.ManifestsAttributes) == 0 {
			break
		}

		manifests := *resultManifests.ManifestsAttributes
		for _, manifest := range manifests {
			if manifest.Digest == nil || !common.IsManifestDeletable(manifest) {
				continue
			}

			// Skip manifests that are in the tag-related set
			if _, ok := toSkipManifests[*manifest.Digest]; ok {
				continue
			}

			// Delete the manifest directly using goroutines
			manifestDigest := *manifest.Digest // Capture for closure
			group.SubmitErr(func() error {
				return p.deleteManifest(ctx, manifestDigest, &deletedManifests)
			})
		}

		// Get the last manifest digest for pagination
		if len(manifests) == 0 {
			break
		}
		lastManifestDigest = *manifests[len(manifests)-1].Digest
	}

	// Wait for all deletion tasks to complete
	err := group.Wait()
	return int(deletedManifests.Load()), err
}
