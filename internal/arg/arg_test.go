// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package arg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

const testRegistry = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/test/providers/Microsoft.ContainerRegistry/registries/testregistry"

type testArtifactRow struct {
	ID         string
	Repository string
	Digest     string
	Tags       []string
}

func testArtifact(number int) testArtifactRow {
	return testArtifactRow{
		ID:         fmt.Sprintf("%s/artifacts/repo/%d", testRegistry, number),
		Repository: "repo", Digest: fmt.Sprintf("sha256:%064x", number),
	}
}

func nativeArtifact(row testArtifactRow, kind, body string) json.RawMessage {
	tag := ""
	if len(row.Tags) > 0 {
		tag = row.Tags[0]
	}
	return json.RawMessage(fmt.Sprintf(
		`{"id":%q,"kind":%q,"properties":{"registryArtifact":{"repository":%q,"digest":%q,"tag":%q,"manifest":%s}}}`,
		row.ID, kind, row.Repository, row.Digest, tag, body,
	))
}

func testPage(rows []json.RawMessage, total int, token string, truncated bool) queryResponse {
	count := len(rows)
	flag, _ := json.Marshal(truncated)
	return queryResponse{Data: rows, Count: &count, TotalRecords: &total, SkipToken: token, ResultTruncated: flag}
}

func TestQueryScopeAndPagination(t *testing.T) {
	query, err := artifactQuery(testRegistry)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query, "where id startswith '"+testRegistry+"/'") ||
		!strings.HasSuffix(query, "| order by id asc") {
		t.Fatalf("incorrect scope/order: %s", query)
	}
	if _, err := artifactQuery(testRegistry + "'"); err == nil {
		t.Fatal("invalid registry ID accepted")
	}
	var requests []queryRequest
	rows, err := fetchArtifacts("endpoint", "subscription", query, func(_ string, body queryRequest) (queryResponse, error) {
		requests = append(requests, body)
		if len(requests) == 1 {
			return testPage([]json.RawMessage{json.RawMessage(`{"id":"1"}`)}, 2, "next", true), nil
		}
		return testPage([]json.RawMessage{json.RawMessage(`{"id":"2"}`)}, 2, "", false), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || len(requests) != 2 || requests[0].Options.SkipToken != "" ||
		requests[1].Options.SkipToken != "next" || requests[0].Query != requests[1].Query ||
		requests[0].Options.ResultFormat != "objectArray" ||
		!reflect.DeepEqual(requests[0].Subscriptions, []string{"subscription"}) {
		t.Fatalf("incorrect pagination: %+v", requests)
	}
}

func TestRejectEmptyTruncatedChangingRepeatedPages(t *testing.T) {
	one := []json.RawMessage{json.RawMessage(`{"id":"1"}`)}
	cases := [][]queryResponse{
		{testPage([]json.RawMessage{}, 0, "", false)},
		{testPage(one, 2, "", true)},
		{testPage(one, 2, "", false)},
		{testPage(one, 2, "next", false), testPage(one, 3, "", false)},
		{testPage(one, 3, "next", false), testPage(one, 3, "next", false)},
		{{}},
	}
	for i, pages := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			index := 0
			_, err := fetchArtifacts("endpoint", "subscription", "query", func(string, queryRequest) (queryResponse, error) {
				if index >= len(pages) {
					t.Fatal("requested unexpected extra page")
				}
				page := pages[index]
				index++
				return page, nil
			})
			if err == nil {
				t.Fatal("invalid/incomplete pages accepted")
			}
		})
	}
	expected := errors.New("authorization failed")
	_, err := fetchArtifacts("endpoint", "subscription", "query", func(string, queryRequest) (queryResponse, error) {
		return queryResponse{}, expected
	})
	if !errors.Is(err, expected) {
		t.Fatalf("request error lost: %v", err)
	}
}

func TestEndpointSelectionAndValidation(t *testing.T) {
	for _, tc := range []struct {
		options Options
		want    string
	}{
		{Options{RegistryID: testRegistry}, "https://southeastasia.management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2024-04-01"},
		{Options{RegistryID: testRegistry, Region: "global"}, "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2024-04-01"},
		{Options{RegistryID: testRegistry, Region: "westus", APIVersion: "2024-04-01-preview"}, "https://westus.management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2024-04-01-preview"},
	} {
		endpoint, err := tc.options.endpoint()
		if err != nil || endpoint != tc.want {
			t.Fatalf("endpoint=%q, err=%v; want %q", endpoint, err, tc.want)
		}
	}
	for _, options := range []Options{
		{RegistryID: "invalid"},
		{RegistryID: testRegistry, Region: "https://elsewhere"},
		{RegistryID: testRegistry, APIVersion: "2024-04-01&other=value"},
		{},
		{RegistryName: "testregistry' | take 1"},
		{RegistryName: "testregistry.othercloud.example"},
		{RegistryName: "otherregistry", RegistryID: testRegistry},
		{RegistryName: "testregistry", SubscriptionID: "not-a-uuid"},
		{RegistryID: testRegistry, SubscriptionID: "11111111-1111-1111-1111-111111111111"},
	} {
		if err := options.Validate(); err == nil {
			t.Fatalf("invalid options accepted: %+v", options)
		}
	}
}

func TestCanceledDiscoveryDoesNotAuthenticate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Discover(ctx, Options{RegistryID: testRegistry})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled discovery before authentication, got %v", err)
	}
}
