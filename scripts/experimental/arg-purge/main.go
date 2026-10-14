// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

// Experimental read-only ARG discovery and dangling-manifest selection.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	registryPattern = regexp.MustCompile(`(?i)^/subscriptions/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/resourceGroups/[^/'\r\n]+/providers/Microsoft.ContainerRegistry/registries/[a-z0-9]+$`)
	digestPattern   = regexp.MustCompile(`^[a-zA-Z0-9_+.-]+:[0-9a-fA-F]+$`)
	regionPattern   = regexp.MustCompile(`^[a-z0-9]+$`)
	versionPattern  = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}(-preview)?$`)
)

type queryOptions struct {
	ResultFormat string `json:"resultFormat"`
	Top          int    `json:"$top"`
	SkipToken    string `json:"$skipToken,omitempty"`
}

type queryRequest struct {
	Subscriptions []string     `json:"subscriptions"`
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

type discovery struct {
	Mode       string            `json:"mode"`
	RegistryID string            `json:"registryId"`
	Data       []json.RawMessage `json:"data"`
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

type candidate struct {
	Repository   string `json:"repository"`
	Digest       string `json:"digest"`
	AgeTimestamp string `json:"ageTimestamp"`
	Reason       string `json:"reason"`
}

type selection struct {
	Mode           string      `json:"mode"`
	RegistryID     string      `json:"registryId"`
	Cutoff         time.Time   `json:"cutoff"`
	InventoryCount int         `json:"inventoryCount"`
	ProtectedCount int         `json:"protectedCount"`
	CandidateCount int         `json:"candidateCount"`
	AgeBasis       string      `json:"ageBasis"`
	Candidates     []candidate `json:"candidates"`
	Warning        string      `json:"warning"`
}

func registrySubscription(id string) (string, error) {
	match := registryPattern.FindStringSubmatch(id)
	if match == nil {
		return "", errors.New("--registry-id must be a complete registry ARM resource ID")
	}
	return match[1], nil
}

func artifactQuery(registryID string) (string, error) {
	if _, err := registrySubscription(registryID); err != nil {
		return "", err
	}
	query := "ContainerRegistryResources" +
		" | where type =~ 'microsoft.containerregistry/registries/artifacts'" +
		fmt.Sprintf(" | where id startswith '%s/'", registryID)
	return query + "\n| order by id asc", nil
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

func azQuery(endpoint string, body queryRequest) (queryResponse, error) {
	var page queryResponse
	payload, err := json.Marshal(body)
	if err != nil {
		return page, err
	}
	cmd := exec.Command("az", "rest", "--method", "post", "--url", endpoint,
		"--resource", "https://management.azure.com/", "--headers", "Content-Type=application/json",
		"--body", string(payload), "-o", "json")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		return page, fmt.Errorf("ARG request failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	if stderr.Len() > 0 {
		fmt.Fprint(os.Stderr, stderr.String())
	}
	if err := json.Unmarshal(output, &page); err != nil {
		return page, fmt.Errorf("decode ARG response: %w", err)
	}
	return page, nil
}

func fetchArtifacts(endpoint, subscription, query string, request func(string, queryRequest) (queryResponse, error)) ([]json.RawMessage, error) {
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
			return nil, errors.New("ARG inventory changed during pagination; rerun discovery")
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
	if len(rows) == 0 {
		return nil, errors.New("ARG returned no artifact inventory for this registry; this does not establish that the registry is empty or has no purge candidates")
	}
	return rows, nil
}

func selectCandidates(rows []artifact, registryID string, cutoff time.Time) (selection, error) {
	var result selection
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
	result = selection{
		Mode: "read-only-candidate-selection", RegistryID: registryID, Cutoff: cutoff,
		InventoryCount: len(inventory), ProtectedCount: len(protected),
		AgeBasis:   "snapshot-ageTimestamp",
		Candidates: make([]candidate, 0),
		Warning: "Experimental inventory-derived candidates, not a validated deletion plan. " +
			"ARG freshness, completeness and native schema must be verified; " +
			"recheck tags, references and locks on the data plane before any deletion.",
	}
	for key, item := range inventory {
		if item.updated.Before(cutoff) && len(item.artifact.Tags) == 0 && !protected[key] {
			result.Candidates = append(result.Candidates, candidate{
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

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("arg-purge", flag.ContinueOnError)
	registryID := flags.String("registry-id", "", "complete registry ARM resource ID")
	discover := flags.Bool("discover", false, "return raw ARG artifact rows")
	snapshot := flags.String("snapshot", "", "captured discovery JSON or normalized artifact array")
	agoDays := flags.Float64("ago-days", 7, "manifest age threshold in days")
	region := flags.String("endpoint-region", "southeastasia", "'global' or ARM region")
	version := flags.String("api-version", "2024-04-01", "ARG API version")
	if err := flags.Parse(args); err != nil {
		return err
	}
	subscription, err := registrySubscription(*registryID)
	if err != nil {
		return err
	}
	modes := 0
	for _, selected := range []bool{*discover, *snapshot != ""} {
		if selected {
			modes++
		}
	}
	if modes > 1 || flags.NArg() != 0 {
		return errors.New("--discover and --snapshot are mutually exclusive; no positional arguments are accepted")
	}
	const day = float64(24 * time.Hour)
	if math.IsNaN(*agoDays) || math.IsInf(*agoDays, 0) || *agoDays < 0 || *agoDays >= float64(math.MaxInt64)/day {
		return errors.New("--ago-days must be finite, nonnegative and within the Go duration range")
	}
	cutoff := time.Now().UTC().Add(-time.Duration(*agoDays * day))
	var rows []artifact
	nativeSnapshot := false
	if *snapshot != "" {
		data, err := os.ReadFile(*snapshot)
		if err != nil {
			return err
		}
		if bytes.HasPrefix(bytes.TrimSpace(data), []byte("[")) {
			if err := json.Unmarshal(data, &rows); err != nil {
				return fmt.Errorf("decode normalized snapshot: %w", err)
			}
		} else {
			var captured discovery
			if err := json.Unmarshal(data, &captured); err != nil {
				return fmt.Errorf("decode discovery snapshot: %w", err)
			}
			if captured.Mode != "schema-discovery" || !strings.EqualFold(captured.RegistryID, *registryID) {
				return errors.New("snapshot must be a discovery capture for the requested registry")
			}
			rows, err = normalizeArtifacts(captured.Data, *registryID)
			if err != nil {
				return err
			}
			nativeSnapshot = true
		}
	} else {
		if !regionPattern.MatchString(*region) || !versionPattern.MatchString(*version) {
			return errors.New("invalid ARM region or API version")
		}
		host := *region + ".management.azure.com"
		if *region == "global" {
			host = "management.azure.com"
		}
		endpoint := "https://" + host + "/providers/Microsoft.ResourceGraph/resources?api-version=" + *version
		query, err := artifactQuery(*registryID)
		if err != nil {
			return err
		}
		rawRows, err := fetchArtifacts(endpoint, subscription, query, azQuery)
		if err != nil {
			return err
		}
		if *discover {
			encoder := json.NewEncoder(output)
			encoder.SetIndent("", "  ")
			return encoder.Encode(discovery{"schema-discovery", *registryID, rawRows})
		}
		rows, err = normalizeArtifacts(rawRows, *registryID)
		if err != nil {
			return err
		}
	}
	result, err := selectCandidates(rows, *registryID, cutoff)
	if err != nil {
		return err
	}
	if *snapshot == "" {
		result.AgeBasis = "ARG properties.registryArtifact.pushedAt"
	} else if nativeSnapshot {
		result.AgeBasis = "ARG properties.registryArtifact.pushedAt (captured snapshot)"
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "ARG prototype failed:", err)
		os.Exit(1)
	}
}
