// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package worker

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/Azure/acr-cli/acr"
	"github.com/Azure/acr-cli/internal/api"
	"github.com/Azure/acr-cli/internal/logger"
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
		// Use a queue size 3x the pool size to buffer enough tasks and keep workers busy and avoiding
		// slowdown due to task scheduling blocking.
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
	log := logger.Get().With().
		Str(logger.FieldRepository, p.repoName).
		Int(logger.FieldTagCount, len(tags)).
		Logger()

	log.Info().Msg("Starting concurrent tag deletion")

	var deletedTags atomic.Int64 // Count of successfully deleted tags
	group := p.pool.NewGroup()
	for _, tag := range tags {
		group.SubmitErr(func() error {
			resp, err := p.acrClient.DeleteAcrTag(ctx, p.repoName, *tag.Name)
			if err == nil {
				log.Info().Str(logger.FieldTag, *tag.Name).Msg("Deleted tag")
				// Increment the count of successfully deleted tags atomically
				deletedTags.Add(1)
				return nil
			}

			if resp != nil && resp.Response != nil && resp.StatusCode == http.StatusNotFound {
				// If the tag is not found it can be assumed to have been deleted.
				deletedTags.Add(1)
				log.Warn().Str(logger.FieldTag, *tag.Name).Int(logger.FieldStatusCode, resp.StatusCode).Msg("Skipped tag, HTTP status")
				return nil
			}

			log.Error().Str(logger.FieldTag, *tag.Name).Err(err).Msg("Failed to delete tag")
			return err
		})
	}
	err := group.Wait() // Error should be nil
	return int(deletedTags.Load()), err
}

// PurgeManifests purges a list of manifests concurrently, and returns a count of deleted manifests and the first error occurred.
func (p *Purger) PurgeManifests(ctx context.Context, manifests []string) (int, error) {
	log := logger.Get().With().
		Str(logger.FieldRepository, p.repoName).
		Int(logger.FieldManifestCount, len(manifests)).
		Logger()

	log.Info().Msg("Starting concurrent manifest deletion")

	var deletedManifests atomic.Int64 // Count of successfully deleted manifests
	group := p.pool.NewGroup()
	for _, manifest := range manifests {
		group.SubmitErr(func() error {
			resp, err := p.acrClient.DeleteManifest(ctx, p.repoName, manifest)
			if err == nil {
				log.Info().Str(logger.FieldLoginURL, p.loginURL).Str(logger.FieldManifest, manifest).Msg("Deleted manifest")

				// Increment the count of successfully deleted manifests atomically
				deletedManifests.Add(1)
				return nil
			}

			if resp != nil && resp.Response != nil && resp.StatusCode == http.StatusNotFound {
				// If the manifest is not found it can be assumed to have been deleted.
				deletedManifests.Add(1)

				log.Warn().Str(logger.FieldManifest, manifest).Int(logger.FieldStatusCode, resp.StatusCode).Msg("Skipped manifest, HTTP status")
				return nil
			}

			log.Error().Str(logger.FieldManifest, manifest).Err(err).Msg("Failed to delete manifest")
			return err

		})
	}
	err := group.Wait()

	finalCount := int(deletedManifests.Load())
	log.Info().
		Int(logger.FieldDeletedCount, finalCount).
		Int(logger.FieldAttemptedCount, len(manifests)).
		Msg("Completed manifest deletion batch")

	return finalCount, err
}
