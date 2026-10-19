// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

// Package arg provides experimental, read-only ARG discovery and manifest-body caching.
package arg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	DefaultRegion     = "southeastasia"
	DefaultAPIVersion = "2024-04-01"
)

type Options struct {
	RegistryName   string
	RegistryID     string
	SubscriptionID string
	Region         string
	APIVersion     string
}

func (o Options) endpoint() (string, error) {
	if o.Region == "" {
		o.Region = DefaultRegion
	}
	if o.APIVersion == "" {
		o.APIVersion = DefaultAPIVersion
	}
	if !regionPattern.MatchString(o.Region) || !versionPattern.MatchString(o.APIVersion) {
		return "", errors.New("invalid ARG ARM region or API version")
	}
	host := o.Region + ".management.azure.com"
	if o.Region == "global" {
		host = "management.azure.com"
	}
	return "https://" + host + "/providers/Microsoft.ResourceGraph/resources?api-version=" + o.APIVersion, nil
}

func (o Options) Validate() error {
	if o.RegistryName == "" && o.RegistryID == "" {
		return errors.New("a registry name or ARM resource ID is required")
	}
	if o.SubscriptionID != "" && !subscriptionPattern.MatchString(o.SubscriptionID) {
		return errors.New("ARG subscription must be a subscription UUID")
	}
	var name string
	if o.RegistryName != "" {
		var err error
		name, err = registryName(o.RegistryName)
		if err != nil {
			return err
		}
	}
	if o.RegistryID != "" {
		subscription, err := registrySubscription(o.RegistryID)
		if err != nil {
			return err
		}
		if o.SubscriptionID != "" && !strings.EqualFold(subscription, o.SubscriptionID) {
			return errors.New("ARG subscription does not match the registry resource ID")
		}
		resourceName := o.RegistryID[strings.LastIndex(o.RegistryID, "/")+1:]
		if name != "" && !strings.EqualFold(name, resourceName) {
			return errors.New("registry name does not match the ARG registry resource ID")
		}
	}
	_, err := o.endpoint()
	return err
}

var (
	registryPattern     = regexp.MustCompile(`(?i)^/subscriptions/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/resourceGroups/[^/'\r\n]+/providers/Microsoft.ContainerRegistry/registries/[a-z0-9]+$`)
	subscriptionPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	registryNamePattern = regexp.MustCompile(`^[a-z0-9]{5,50}$`)
	digestPattern       = regexp.MustCompile(`^[a-zA-Z0-9_+.-]+:[0-9a-fA-F]+$`)
	regionPattern       = regexp.MustCompile(`^[a-z0-9]+$`)
	versionPattern      = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}(-preview)?$`)
)

func registryName(value string) (string, error) {
	name := strings.TrimSuffix(strings.ToLower(value), ".azurecr.io")
	if !registryNamePattern.MatchString(name) {
		return "", errors.New("ARG registry must be a public-cloud registry name or .azurecr.io login server")
	}
	return name, nil
}

type queryOptions struct {
	ResultFormat string `json:"resultFormat"`
	Top          int    `json:"$top"`
	SkipToken    string `json:"$skipToken,omitempty"`
}

type queryRequest struct {
	Subscriptions []string     `json:"subscriptions,omitempty"`
	Query         string       `json:"query"`
	Options       queryOptions `json:"options"`
}

type queryResponse struct {
	Data            []json.RawMessage `json:"data"`
	Count           *int              `json:"count"`
	TotalRecords    *int              `json:"totalRecords"`
	SkipToken       string            `json:"$skipToken"`
	ResultTruncated json.RawMessage   `json:"resultTruncated"`
}

type Discovery struct {
	Mode       string            `json:"mode"`
	RegistryID string            `json:"registryId"`
	Data       []json.RawMessage `json:"data"`
	QueryPages int               `json:"queryPages,omitempty"`
}

type artifact struct {
	ID           string          `json:"id"`
	Repository   string          `json:"repository"`
	Digest       string          `json:"digest"`
	Tags         []string        `json:"tags"`
	AgeTimestamp string          `json:"ageTimestamp"`
	Children     []string        `json:"children"`
	Subject      json.RawMessage `json:"subject"`
}

type identity struct {
	repository string
	digest     string
}

type entry struct {
	artifact artifact
	updated  time.Time
	subject  string
}

type Candidate struct {
	Repository   string `json:"repository"`
	Digest       string `json:"digest"`
	AgeTimestamp string `json:"ageTimestamp"`
	Reason       string `json:"reason"`
}

type Selection struct {
	Mode           string      `json:"mode"`
	RegistryID     string      `json:"registryId"`
	Cutoff         time.Time   `json:"cutoff"`
	InventoryCount int         `json:"inventoryCount"`
	ProtectedCount int         `json:"protectedCount"`
	CandidateCount int         `json:"candidateCount"`
	AgeBasis       string      `json:"ageBasis"`
	Candidates     []Candidate `json:"candidates"`
	Warning        string      `json:"warning"`
}

func registrySubscription(id string) (string, error) {
	match := registryPattern.FindStringSubmatch(id)
	if match == nil {
		return "", errors.New("registry ID must be a complete registry ARM resource ID")
	}
	return match[1], nil
}

func artifactQuery(registryID string) (string, error) {
	return scopedArtifactQuery(registryID, "")
}

func scopedArtifactQuery(registryID, filters string) (string, error) {
	if _, err := registrySubscription(registryID); err != nil {
		return "", err
	}
	query := "ContainerRegistryResources" +
		" | where type =~ 'microsoft.containerregistry/registries/artifacts'" +
		fmt.Sprintf(" | where id startswith '%s/'", registryID)
	return query + filters + "\n| order by id asc", nil
}

func normalizeArtifacts(rawRows []json.RawMessage, registryID string) ([]artifact, error) {
	type descriptor struct {
		Digest string `json:"digest"`
	}
	type nativeRow struct {
		ID         string `json:"id"`
		Kind       string `json:"kind"`
		Properties struct {
			RegistryArtifact struct {
				Repository string          `json:"repository"`
				Digest     string          `json:"digest"`
				Tag        string          `json:"tag"`
				PushedAt   string          `json:"pushedAt"`
				Manifest   json.RawMessage `json:"manifest"`
			} `json:"registryArtifact"`
		} `json:"properties"`
	}
	manifests := make(map[identity]artifact)
	tags := make(map[identity][]string)
	tagTargets := make(map[identity]string)
	ids := make(map[string]bool)
	for _, raw := range rawRows {
		var row nativeRow
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, fmt.Errorf("decode native ARG artifact: %w", err)
		}
		if !strings.HasPrefix(strings.ToLower(row.ID), strings.ToLower(registryID)+"/") || ids[row.ID] {
			return nil, errors.New("native artifact ID is duplicated or outside the requested registry")
		}
		ids[row.ID] = true
		item := row.Properties.RegistryArtifact
		if item.Repository == "" || !digestPattern.MatchString(item.Digest) {
			return nil, errors.New("native artifact is missing repository/digest")
		}
		key := identity{item.Repository, item.Digest}
		switch row.Kind {
		case "Tag":
			if item.Tag == "" {
				return nil, errors.New("native Tag artifact has no tag name")
			}
			tagKey := identity{item.Repository, item.Tag}
			if previous, exists := tagTargets[tagKey]; exists {
				return nil, fmt.Errorf("ARG has ambiguous tag %s:%s targeting %s and %s; rerun after inventory converges",
					item.Repository, item.Tag, previous, item.Digest)
			}
			tagTargets[tagKey] = item.Digest
			tags[key] = append(tags[key], item.Tag)
		case "Manifest":
			if _, exists := manifests[key]; exists {
				return nil, errors.New("native inventory contains duplicate repository/digest manifests")
			}
			var body struct {
				SchemaVersion int             `json:"schemaVersion"`
				MediaType     string          `json:"mediaType"`
				Manifests     *[]descriptor   `json:"manifests"`
				Layers        *[]descriptor   `json:"layers"`
				Blobs         *[]descriptor   `json:"blobs"`
				Subject       json.RawMessage `json:"subject"`
			}
			if err := json.Unmarshal(item.Manifest, &body); err != nil {
				return nil, fmt.Errorf("decode indexed manifest %s@%s: %w", item.Repository, item.Digest, err)
			}
			normalized := artifact{
				ID: row.ID, Repository: item.Repository, Digest: item.Digest,
				AgeTimestamp: item.PushedAt, Tags: []string{}, Children: []string{},
				Subject: json.RawMessage("null"),
			}
			switch body.MediaType {
			case "application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json":
				if body.SchemaVersion != 2 || body.Manifests == nil {
					return nil, errors.New("indexed image index/list is missing schemaVersion/manifests")
				}
				for _, child := range *body.Manifests {
					normalized.Children = append(normalized.Children, child.Digest)
				}
			case "application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json":
				if body.SchemaVersion != 2 || body.Layers == nil {
					return nil, errors.New("indexed image manifest is missing schemaVersion/layers")
				}
			case "application/vnd.oci.artifact.manifest.v1+json":
				if body.Blobs == nil {
					return nil, errors.New("indexed artifact manifest is missing blobs")
				}
			default:
				return nil, fmt.Errorf("unsupported indexed manifest media type %q", body.MediaType)
			}
			if len(body.Subject) > 0 && !bytes.Equal(bytes.TrimSpace(body.Subject), []byte("null")) {
				var subject descriptor
				if err := json.Unmarshal(body.Subject, &subject); err != nil || !digestPattern.MatchString(subject.Digest) {
					return nil, errors.New("indexed manifest has an invalid subject descriptor")
				}
				normalized.Subject, _ = json.Marshal(subject.Digest)
			}
			manifests[key] = normalized
		default:
			return nil, fmt.Errorf("unsupported native ARG artifact kind %q", row.Kind)
		}
	}
	for key, names := range tags {
		item, exists := manifests[key]
		if !exists {
			return nil, errors.New("native tag target is missing from manifest inventory")
		}
		sort.Strings(names)
		item.Tags = names
		manifests[key] = item
	}
	rows := make([]artifact, 0, len(manifests))
	for _, item := range manifests {
		rows = append(rows, item)
	}
	return rows, nil
}

func fetchArtifacts(endpoint, subscription, query string, request func(string, queryRequest) (queryResponse, error)) ([]json.RawMessage, error) {
	rows, err := fetchArtifactRows(endpoint, subscription, query, request)
	if err == nil && len(rows) == 0 {
		return nil, errors.New("ARG returned no artifact inventory for this registry; this does not establish that the registry is empty or has no purge candidates")
	}
	return rows, err
}

func fetchArtifactRows(endpoint, subscription, query string, request func(string, queryRequest) (queryResponse, error)) ([]json.RawMessage, error) {
	var rows []json.RawMessage
	body := queryRequest{
		Subscriptions: []string{subscription}, Query: query,
		Options: queryOptions{ResultFormat: "objectArray", Top: 1000},
	}
	seenTokens := make(map[string]bool)
	expectedTotal := -1
	for {
		page, err := request(endpoint, body)
		if err != nil {
			return nil, err
		}
		if page.Data == nil || page.Count == nil || page.TotalRecords == nil ||
			*page.Count != len(page.Data) || *page.TotalRecords < 0 {
			return nil, errors.New("ARG returned invalid objectArray/count/totalRecords")
		}
		if expectedTotal >= 0 && expectedTotal != *page.TotalRecords {
			return nil, fmt.Errorf("ARG inventory changed during pagination (totalRecords %d -> %d); rerun discovery",
				expectedTotal, *page.TotalRecords)
		}
		expectedTotal = *page.TotalRecords
		rows = append(rows, page.Data...)
		truncated := string(page.ResultTruncated)
		switch truncated {
		case "true", "false", `"true"`, `"false"`:
		default:
			return nil, errors.New("ARG response omitted a valid resultTruncated flag")
		}
		if page.SkipToken == "" {
			if truncated == "true" || truncated == `"true"` || len(rows) != expectedTotal {
				return nil, errors.New("ARG inventory is incomplete and has no continuation token")
			}
			break
		}
		if seenTokens[page.SkipToken] || len(page.Data) == 0 {
			return nil, errors.New("ARG returned an invalid/repeated continuation token")
		}
		seenTokens[page.SkipToken] = true
		body.Options.SkipToken = page.SkipToken
	}
	return rows, nil
}

func selectCandidates(rows []artifact, registryID string, cutoff time.Time) (Selection, error) {
	var result Selection
	if _, err := registrySubscription(registryID); err != nil {
		return result, err
	}
	if len(rows) == 0 {
		return result, errors.New("a nonempty, complete normalized artifact inventory is required")
	}
	inventory := make(map[identity]entry)
	ids := make(map[string]bool)
	for _, row := range rows {
		if !strings.HasPrefix(strings.ToLower(row.ID), strings.ToLower(registryID)+"/") {
			return result, errors.New("artifact id is outside the requested registry")
		}
		if row.Repository == "" || !digestPattern.MatchString(row.Digest) {
			return result, errors.New("repository and manifest digest are required")
		}
		key := identity{row.Repository, row.Digest}
		if _, exists := inventory[key]; exists || ids[row.ID] {
			return result, errors.New("duplicate manifest identity or artifact id in ARG inventory")
		}
		if row.Tags == nil || row.Children == nil || len(row.Subject) == 0 {
			return result, errors.New("tags and children must be explicit arrays; subject must be explicit null or a digest")
		}
		for _, tag := range row.Tags {
			if tag == "" {
				return result, errors.New("tags must contain nonempty strings")
			}
		}
		for _, child := range row.Children {
			if !digestPattern.MatchString(child) {
				return result, errors.New("children must contain manifest digests")
			}
		}
		var subject string
		if !bytes.Equal(bytes.TrimSpace(row.Subject), []byte("null")) {
			if err := json.Unmarshal(row.Subject, &subject); err != nil || !digestPattern.MatchString(subject) {
				return result, errors.New("subject must be explicit null or a manifest digest")
			}
		}
		updated, err := time.Parse(time.RFC3339Nano, row.AgeTimestamp)
		if err != nil {
			return result, fmt.Errorf("invalid ageTimestamp for %s@%s: %w", row.Repository, row.Digest, err)
		}
		inventory[key] = entry{row, updated, subject}
		ids[row.ID] = true
	}

	var pending []identity
	for key, item := range inventory {
		references := append([]string{}, item.artifact.Children...)
		if item.subject != "" {
			references = append(references, item.subject)
			pending = append(pending, identity{key.repository, item.subject})
		}
		for _, reference := range references {
			if _, exists := inventory[identity{key.repository, reference}]; !exists {
				return result, errors.New("inventory lacks a referenced manifest; cannot assess reachability")
			}
		}
		if len(item.artifact.Tags) > 0 || item.subject != "" || !item.updated.Before(cutoff) {
			pending = append(pending, key)
		}
	}
	// Traverse before age filtering: even a young retained parent protects old children.
	protected := make(map[identity]bool)
	for len(pending) > 0 {
		key := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if protected[key] {
			continue
		}
		protected[key] = true
		for _, child := range inventory[key].artifact.Children {
			pending = append(pending, identity{key.repository, child})
		}
	}
	result = Selection{
		Mode: "read-only-candidate-selection", RegistryID: registryID, Cutoff: cutoff,
		InventoryCount: len(inventory), ProtectedCount: len(protected),
		AgeBasis:   "snapshot-ageTimestamp",
		Candidates: make([]Candidate, 0),
		Warning: "Experimental inventory-derived candidates, not a validated deletion plan. " +
			"ARG freshness, completeness and native schema must be verified; " +
			"recheck tags, references and locks on the data plane before any deletion.",
	}
	for key, item := range inventory {
		if item.updated.Before(cutoff) && len(item.artifact.Tags) == 0 && !protected[key] {
			result.Candidates = append(result.Candidates, Candidate{
				item.artifact.Repository, item.artifact.Digest, item.artifact.AgeTimestamp,
				"old-untagged-unreferenced",
			})
		}
	}
	sort.Slice(result.Candidates, func(i, j int) bool {
		a, b := result.Candidates[i], result.Candidates[j]
		if a.Repository != b.Repository {
			return a.Repository < b.Repository
		}
		return a.Digest < b.Digest
	})
	result.CandidateCount = len(result.Candidates)
	return result, nil
}

func FindCandidates(ctx context.Context, options Options, cutoff time.Time) (Selection, error) {
	captured, err := Discover(ctx, options)
	if err != nil {
		return Selection{}, err
	}
	rows, err := normalizeArtifacts(captured.Data, captured.RegistryID)
	if err != nil {
		return Selection{}, err
	}
	result, err := selectCandidates(rows, captured.RegistryID, cutoff)
	if err != nil {
		return Selection{}, err
	}
	result.AgeBasis = "ARG properties.registryArtifact.pushedAt"
	return result, nil
}
