// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

// Package arg provides experimental, read-only ARG discovery and manifest-body caching.
package arg

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Default ARG query endpoint settings.
const (
	DefaultRegion     = "southeastasia"
	DefaultAPIVersion = "2024-04-01"
)

// Options configures registry scope and the ARG query endpoint.
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

// Validate checks registry scope and endpoint settings.
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

// Discovery contains raw ARG records and their query scope.
type Discovery struct {
	Mode       string            `json:"mode"`
	RegistryID string            `json:"registryId"`
	Data       []json.RawMessage `json:"data"`
	QueryPages int               `json:"queryPages,omitempty"`
}

type identity struct {
	repository string
	digest     string
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
