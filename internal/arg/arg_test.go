// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package arg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const testRegistry = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/test/providers/Microsoft.ContainerRegistry/registries/testregistry"

var testCutoff = time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)

func testArtifact(number int) artifact {
	return artifact{
		ID:         fmt.Sprintf("%s/artifacts/repo/%d", testRegistry, number),
		Repository: "repo", Digest: fmt.Sprintf("sha256:%064x", number),
		Tags: []string{}, Children: []string{}, Subject: json.RawMessage("null"),
		AgeTimestamp: testCutoff.Add(-24 * time.Hour).Format(time.RFC3339Nano),
	}
}

func TestSelectOldUntaggedOutsideTaggedGraph(t *testing.T) {
	orphan, orphanChild := testArtifact(1), testArtifact(2)
	orphan.Children = []string{orphanChild.Digest}
	tagged, nested, protected := testArtifact(3), testArtifact(4), testArtifact(5)
	tagged.Tags = []string{"keep"}
	tagged.Children = []string{nested.Digest}
	nested.Children = []string{protected.Digest}
	recent, boundary := testArtifact(6), testArtifact(7)
	recent.AgeTimestamp = testCutoff.Add(time.Hour).Format(time.RFC3339Nano)
	boundary.AgeTimestamp = testCutoff.Format(time.RFC3339Nano)
	result, err := selectCandidates([]artifact{protected, recent, nested, orphan, tagged, orphanChild, boundary}, testRegistry, testCutoff)
	if err != nil {
		t.Fatal(err)
	}
	if result.InventoryCount != 7 || result.CandidateCount != 2 ||
		result.Candidates[0].Digest != orphan.Digest || result.Candidates[1].Digest != orphanChild.Digest {
		t.Fatalf("unexpected selection: %+v", result)
	}
}

func TestSharedChildrenCyclesAndRepositoryScope(t *testing.T) {
	child, tagged, orphan := testArtifact(1), testArtifact(2), testArtifact(3)
	tagged.Tags, tagged.Children = []string{"keep"}, []string{child.Digest}
	orphan.Children = []string{child.Digest}
	otherRepo := testArtifact(1)
	otherRepo.Repository, otherRepo.ID = "other", testRegistry+"/artifacts/other/1"
	cycleA, cycleB := testArtifact(4), testArtifact(5)
	cycleA.Tags, cycleA.Children = []string{"cycle"}, []string{cycleB.Digest}
	cycleB.Children = []string{cycleA.Digest}
	result, err := selectCandidates([]artifact{child, tagged, orphan, otherRepo, cycleA, cycleB}, testRegistry, testCutoff)
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateCount != 2 || result.Candidates[0].Repository != "other" ||
		result.Candidates[1].Digest != orphan.Digest {
		t.Fatalf("unexpected selection: %+v", result)
	}
}

func TestRetainedYoungParentAndSubjectRelationships(t *testing.T) {
	child, young, subject, referrer := testArtifact(1), testArtifact(2), testArtifact(3), testArtifact(4)
	young.AgeTimestamp = testCutoff.Format(time.RFC3339Nano)
	young.Children = []string{child.Digest}
	referrer.Subject, _ = json.Marshal(subject.Digest)
	result, err := selectCandidates([]artifact{child, young, subject, referrer}, testRegistry, testCutoff)
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateCount != 0 || result.Candidates == nil {
		t.Fatalf("expected explicit empty candidate array: %+v", result)
	}
}

func TestNanosecondAgeBoundary(t *testing.T) {
	old, boundary := testArtifact(1), testArtifact(2)
	old.AgeTimestamp = testCutoff.Add(-time.Nanosecond).Format(time.RFC3339Nano)
	boundary.AgeTimestamp = testCutoff.Format(time.RFC3339Nano)
	result, err := selectCandidates([]artifact{boundary, old}, testRegistry, testCutoff)
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateCount != 1 || result.Candidates[0].Digest != old.Digest {
		t.Fatalf("age boundary is not strictly older: %+v", result)
	}
}

func TestRejectUnknownDuplicateAndIncompleteInventory(t *testing.T) {
	cases := map[string]func(*artifact){
		"unknown tags":     func(a *artifact) { a.Tags = nil },
		"unknown children": func(a *artifact) { a.Children = nil },
		"unknown subject":  func(a *artifact) { a.Subject = nil },
		"bad subject":      func(a *artifact) { a.Subject = json.RawMessage(`""`) },
		"bad digest":       func(a *artifact) { a.Digest = "bad" },
		"bad child":        func(a *artifact) { a.Children = []string{"bad"} },
		"empty tag":        func(a *artifact) { a.Tags = []string{""} },
		"bad time":         func(a *artifact) { a.AgeTimestamp = "not-a-date" },
		"no timezone":      func(a *artifact) { a.AgeTimestamp = "2026-01-01T00:00:00" },
		"other registry":   func(a *artifact) { a.ID = "/another/registry/artifact" },
		"missing child":    func(a *artifact) { a.Children = []string{testArtifact(2).Digest} },
		"missing subject":  func(a *artifact) { a.Subject, _ = json.Marshal(testArtifact(2).Digest) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			row := testArtifact(1)
			mutate(&row)
			if _, err := selectCandidates([]artifact{row}, testRegistry, testCutoff); err == nil {
				t.Fatal("invalid inventory accepted")
			}
		})
	}
	for _, rows := range [][]artifact{nil, {testArtifact(1), testArtifact(1)}} {
		if _, err := selectCandidates(rows, testRegistry, testCutoff); err == nil {
			t.Fatal("empty/duplicate inventory accepted")
		}
	}
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
	rows, err := fetchArtifacts("endpoint", "subscription", query, func(endpoint string, body queryRequest) (queryResponse, error) {
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

func TestRejectInvalidModesAndAge(t *testing.T) {
	for _, args := range [][]string{
		{"--registry-id", testRegistry, "--discover", "--snapshot", "unused"},
		{"--registry-id", testRegistry, "--discover", "--ago-days", "-1"},
		{"--registry-id", testRegistry, "--discover", "--ago-days", "NaN"},
		{"--registry-id", testRegistry, "--discover", "--ago-days", "Inf"},
		{"--registry-id", testRegistry, "--discover", "--ago-days", "1000000"},
	} {
		if err := RunPrototype(context.Background(), args, io.Discard); err == nil {
			t.Fatalf("invalid arguments accepted: %v", args)
		}
	}

}

func TestOfflineCommandOutput(t *testing.T) {
	orphan, tagged, child := testArtifact(1), testArtifact(2), testArtifact(3)
	tagged.Tags, tagged.Children = []string{"keep"}, []string{child.Digest}
	rows := []artifact{orphan, tagged, child}
	for i := range rows {
		rows[i].AgeTimestamp = "2020-01-01T00:00:00Z"
	}
	data, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	if err := RunPrototype(context.Background(), []string{"--registry-id", testRegistry, "--snapshot", path, "--ago-days", "7"}, &output); err != nil {
		t.Fatal(err)
	}
	var result Selection
	if err := json.Unmarshal([]byte(output.String()), &result); err != nil {
		t.Fatal(err)
	}
	if result.RegistryID != testRegistry || result.Mode != "read-only-candidate-selection" ||
		result.InventoryCount != 3 || result.ProtectedCount != 2 || result.CandidateCount != 1 ||
		result.Candidates[0].Repository != "repo" || result.Candidates[0].Digest != orphan.Digest ||
		result.Candidates[0].AgeTimestamp != rows[0].AgeTimestamp ||
		result.Candidates[0].Reason != "old-untagged-unreferenced" || result.Warning == "" {
		t.Fatalf("incorrect command output: %s", output.String())
	}
}

func nativeArtifact(row artifact, kind, body string) json.RawMessage {
	tag := ""
	if len(row.Tags) > 0 {
		tag = row.Tags[0]
	}
	return json.RawMessage(fmt.Sprintf(
		`{"id":%q,"kind":%q,"properties":{"registryArtifact":{"repository":%q,"digest":%q,"pushedAt":%q,"tag":%q,"manifest":%s}}}`,
		row.ID, kind, row.Repository, row.Digest, row.AgeTimestamp, tag, body,
	))
}

func TestNativeTagMappingChildrenAndSubject(t *testing.T) {
	child, parent, referrer := testArtifact(1), testArtifact(2), testArtifact(3)
	tag := parent
	tag.ID += "-tag"
	tag.Tags = []string{"keep"}
	rows, err := normalizeArtifacts([]json.RawMessage{
		nativeArtifact(tag, "Tag", "null"),
		nativeArtifact(parent, "Manifest", fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"digest":%q}]}`, child.Digest)),
		nativeArtifact(child, "Manifest", `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`),
		nativeArtifact(referrer, "Manifest", fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[],"subject":{"digest":%q}}`, child.Digest)),
	}, testRegistry)
	if err != nil {
		t.Fatal(err)
	}
	result, err := selectCandidates(rows, testRegistry, testCutoff)
	if err != nil {
		t.Fatal(err)
	}
	if result.InventoryCount != 3 || result.ProtectedCount != 3 || result.CandidateCount != 0 {
		t.Fatalf("native relationships were lost: %+v", result)
	}
}

func TestRejectAmbiguousOrIncompleteNativeRows(t *testing.T) {
	image := testArtifact(1)
	body := `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`
	manifest := nativeArtifact(image, "Manifest", body)
	tag := image
	tag.ID += "-tag"
	tag.Tags = []string{"keep"}
	duplicate := tag
	duplicate.ID += "-another"
	duplicate.Digest = testArtifact(2).Digest
	for name, raw := range map[string][]json.RawMessage{
		"missing target":         {nativeArtifact(tag, "Tag", "null")},
		"ambiguous tag":          {manifest, nativeArtifact(tag, "Tag", "null"), nativeArtifact(duplicate, "Tag", "null")},
		"duplicate manifest":     {manifest, manifest},
		"missing body":           {nativeArtifact(image, "Manifest", "null")},
		"missing layers":         {nativeArtifact(image, "Manifest", `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`)},
		"missing index children": {nativeArtifact(image, "Manifest", `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json"}`)},
		"unsupported kind":       {nativeArtifact(image, "Unknown", body)},
		"unsupported media type": {nativeArtifact(image, "Manifest", `{"mediaType":"unknown"}`)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := normalizeArtifacts(raw, testRegistry); err == nil {
				t.Fatal("ambiguous/incomplete native rows accepted")
			}
		})
	}
}

func TestReplayDiscoveryCapture(t *testing.T) {
	image := testArtifact(1)
	image.AgeTimestamp = "2020-01-01T00:00:00Z"
	captured := Discovery{
		Mode: "schema-discovery", RegistryID: testRegistry,
		Data: []json.RawMessage{nativeArtifact(image, "Manifest",
			`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","layers":[]}`)},
	}
	data, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "discovery.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	if err := RunPrototype(context.Background(), []string{"--registry-id", testRegistry, "--snapshot", path, "--ago-days", "7"}, &output); err != nil {
		t.Fatal(err)
	}
	var result Selection
	if err := json.Unmarshal([]byte(output.String()), &result); err != nil {
		t.Fatal(err)
	}
	if result.CandidateCount != 1 || result.Candidates[0].Digest != image.Digest ||
		result.Candidates[0].AgeTimestamp != image.AgeTimestamp ||
		result.AgeBasis != "ARG properties.registryArtifact.pushedAt (captured snapshot)" {
		t.Fatalf("invalid discovery replay: %s", output.String())
	}
	output.Reset()
	if err := RunPrototype(context.Background(), []string{"--registry", "testregistry.azurecr.io", "--snapshot", path, "--ago-days", "7"}, &output); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(output.String()), &result); err != nil || result.RegistryID != testRegistry {
		t.Fatalf("capture replay failed to infer its registry ID: %+v, err=%v", result, err)
	}
	if err := RunPrototype(context.Background(), []string{"--registry", "otherregistry", "--snapshot", path}, io.Discard); err == nil {
		t.Fatal("capture replay accepted another registry name")
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
	_, err := FindCandidates(ctx, Options{RegistryID: testRegistry}, testCutoff)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled discovery before authentication, got %v", err)
	}
}
