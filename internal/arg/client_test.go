// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package arg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azure/acr-cli/internal/requestcount"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

type testCredential struct {
	scopes []string
	err    error
}

func (c *testCredential) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	if err := ctx.Err(); err != nil {
		return azcore.AccessToken{}, err
	}
	c.scopes = options.Scopes
	return azcore.AccessToken{Token: "test-token", ExpiresOn: time.Now().Add(time.Hour)}, c.err
}

func testHTTPClient(t *testing.T, credential azcore.TokenCredential, handler http.HandlerFunc) *client {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	return newClient(server.URL+"/providers/Microsoft.ResourceGraph/resources?api-version=2024-04-01",
		credential, &policy.ClientOptions{
			Transport: server.Client(), Retry: policy.RetryOptions{MaxRetries: -1},
		})
}

func registryResource(id, name string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"id":%q,"name":%q}`, id, name))
}

func TestNativeHTTPDiscoveryAndPaginationWithoutCLI(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	orphan, child, parent := testArtifact(1), testArtifact(2), testArtifact(3)
	tag := parent
	tag.ID += "-tag"
	tag.Tags = []string{"keep"}
	raw := []json.RawMessage{
		nativeArtifact(tag, "Tag", "null"),
		nativeArtifact(parent, "Manifest", fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"digest":%q}]}`, child.Digest)),
		nativeArtifact(child, "Manifest", `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","layers":[]}`),
		nativeArtifact(orphan, "Manifest", `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","layers":[]}`),
	}
	credential := &testCredential{}
	var requests []queryRequest
	c := testHTTPClient(t, credential, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/providers/Microsoft.ResourceGraph/resources" ||
			r.URL.Query().Get("api-version") != "2024-04-01" ||
			r.Header.Get("Authorization") != "Bearer test-token" ||
			r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
		var body queryRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		requests = append(requests, body)
		var response queryResponse
		switch len(requests) {
		case 1:
			if body.Subscriptions != nil || !strings.HasPrefix(body.Query, "Resources |") ||
				!strings.Contains(body.Query, "name =~ 'testregistry'") || body.Options.Top != 2 {
				t.Errorf("unexpected registry lookup: %+v", body)
			}
			response = testPage([]json.RawMessage{registryResource(testRegistry, "testregistry")}, 1, "", false)
		case 2:
			if !strings.Contains(body.Query, "id startswith '"+testRegistry+"/'") ||
				body.Options.Top != 1000 || body.Options.SkipToken != "" {
				t.Errorf("unexpected artifact query: %+v", body)
			}
			response = testPage(raw[:2], 4, "next", true)
		case 3:
			if body.Query != requests[1].Query || body.Options.SkipToken != "next" {
				t.Errorf("unexpected next page: %+v", body)
			}
			response = testPage(raw[2:], 4, "", false)
		default:
			t.Errorf("unexpected extra request: %+v", body)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Error(err)
		}
	})
	captured, err := c.discover(context.Background(), Options{RegistryName: "TestRegistry.AZURECR.IO"})
	if err != nil {
		t.Fatal(err)
	}
	if captured.RegistryID != testRegistry || len(requests) != 3 ||
		!reflect.DeepEqual(requests[1].Subscriptions, []string{"00000000-0000-0000-0000-000000000000"}) ||
		!reflect.DeepEqual(requests[1].Subscriptions, requests[2].Subscriptions) ||
		!reflect.DeepEqual(credential.scopes, []string{"https://management.azure.com/.default"}) {
		t.Fatalf("incorrect discovery or ARM token scope: %+v", requests)
	}
	if !reflect.DeepEqual(captured.Data, raw) {
		t.Fatalf("native HTTP discovery changed raw records: %+v", captured.Data)
	}
}

func TestExplicitRegistryIDSkipsLookup(t *testing.T) {
	requests := 0
	c := testHTTPClient(t, &testCredential{}, func(w http.ResponseWriter, r *http.Request) {
		requests++
		var body queryRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}

		if strings.HasPrefix(body.Query, "Resources |") {
			t.Error("explicit registry ID triggered unnecessary resolution")
		}
		if err := json.NewEncoder(w).Encode(testPage([]json.RawMessage{json.RawMessage(`{"id":"row"}`)}, 1, "", false)); err != nil {
			t.Error(err)
		}
	})
	captured, err := c.discover(context.Background(), Options{RegistryName: "testregistry", RegistryID: testRegistry})
	if err != nil || requests != 1 || captured.RegistryID != testRegistry {
		t.Fatalf("explicit scope failed: %+v, err=%v, requests=%d", captured, err, requests)
	}
}

func TestManifestOnlyDiscoveryScopesRepositoriesAndAllowsEmptyInventory(t *testing.T) {
	for _, tc := range []struct {
		name  string
		total int
		pages int
	}{
		{"empty", 0, 1},
		{"single page", 1, 1},
		{"multiple pages", 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			c := testHTTPClient(t, &testCredential{}, func(w http.ResponseWriter, r *http.Request) {
				requests++
				var body queryRequest
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if !strings.Contains(body.Query, "where kind =~ 'Manifest'") ||
					!strings.Contains(body.Query, `in ("repo","team/a'b")`) ||
					!strings.HasSuffix(body.Query, "| order by id asc") {
					t.Errorf("incorrect manifest-only scope: %s", body.Query)
				}
				rows := []json.RawMessage{}
				token := ""
				if tc.total != 0 {
					rows = append(rows, nativeArtifact(testArtifact(requests), "Manifest", cacheImageBody))
				}
				if tc.pages > 1 {
					if requests == 1 {
						token = "next"
					} else if body.Options.SkipToken != "next" {
						t.Errorf("manifest pagination lost its continuation: %+v", body.Options)
					}
				}
				if err := json.NewEncoder(w).Encode(testPage(rows, tc.total, token, false)); err != nil {
					t.Error(err)
				}
			})
			captured, err := c.discoverArtifacts(context.Background(), Options{RegistryID: testRegistry}, []string{"repo", "team/a'b"})
			if err != nil {
				t.Fatal(err)
			}
			cache, err := NewManifestCache(captured)
			if err != nil {
				t.Fatal(err)
			}
			if requests != tc.pages || cache.Stats().QueryPages != tc.pages || cache.Stats().CachedBodies != tc.total {
				t.Fatalf("manifest discovery failed: cache=%+v, err=%v", cache, err)
			}
		})
	}
	if _, err := manifestQuery(testRegistry, []string{}); err == nil {
		t.Fatal("empty repository scope was accepted")
	}
}

func TestRegistryLookupUsesOptionalSubscription(t *testing.T) {
	const subscription = "00000000-0000-0000-0000-000000000000"
	c := testHTTPClient(t, &testCredential{}, func(w http.ResponseWriter, r *http.Request) {
		var body queryRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(body.Subscriptions, []string{subscription}) {
			t.Errorf("registry query lost explicit subscription scope: %+v", body)
		}
		if err := json.NewEncoder(w).Encode(testPage([]json.RawMessage{registryResource(testRegistry, "testregistry")}, 1, "", false)); err != nil {
			t.Error(err)
		}
	})
	id, err := c.resolveRegistry(context.Background(), Options{RegistryName: "testregistry", SubscriptionID: subscription})
	if err != nil || id != testRegistry {
		t.Fatalf("scoped lookup failed: id=%q, err=%v", id, err)
	}
}

func TestRegistryLookupFailsClosed(t *testing.T) {
	valid := []json.RawMessage{registryResource(testRegistry, "testregistry")}
	for name, page := range map[string]queryResponse{
		"missing metadata":   {},
		"not visible":        testPage([]json.RawMessage{}, 0, "", false),
		"ambiguous":          testPage(append(append([]json.RawMessage{}, valid...), valid...), 2, "", false),
		"truncated":          testPage(valid, 1, "", true),
		"continuation":       testPage(valid, 1, "next", false),
		"unexpected name":    testPage([]json.RawMessage{registryResource(testRegistry, "otherregistry")}, 1, "", false),
		"missing ID":         testPage([]json.RawMessage{registryResource("", "testregistry")}, 1, "", false),
		"wrong resource ID":  testPage([]json.RawMessage{registryResource(testRegistry+"other", "testregistry")}, 1, "", false),
		"inconsistent count": testPage(valid, 0, "", false),
	} {
		t.Run(name, func(t *testing.T) {
			requests := 0
			c := testHTTPClient(t, &testCredential{}, func(w http.ResponseWriter, _ *http.Request) {
				requests++
				if err := json.NewEncoder(w).Encode(page); err != nil {
					t.Error(err)
				}
			})
			if _, err := c.discover(context.Background(), Options{RegistryName: "testregistry"}); err == nil || requests != 1 {
				t.Fatalf("invalid discovery accepted or queried artifacts: err=%v, requests=%d", err, requests)
			}
		})
	}
}

func TestHTTPAndCredentialErrorsAreNotHidden(t *testing.T) {
	t.Run("ARM authorization", func(t *testing.T) {
		c := testHTTPClient(t, &testCredential{}, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"error":{"code":"AuthorizationFailed","message":"denied"}}`)
		})
		_, err := c.discover(context.Background(), Options{RegistryName: "testregistry"})
		var responseError *azcore.ResponseError
		if !errors.As(err, &responseError) || responseError.StatusCode != http.StatusForbidden ||
			responseError.ErrorCode != "AuthorizationFailed" {
			t.Fatalf("ARM error details lost: %v", err)
		}
	})
	t.Run("malformed response", func(t *testing.T) {
		c := testHTTPClient(t, &testCredential{}, func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, "not json")
		})
		if _, err := c.discover(context.Background(), Options{RegistryName: "testregistry"}); err == nil ||
			!strings.Contains(err.Error(), "decode ARG response") {
			t.Fatalf("malformed response was hidden: %v", err)
		}
	})
	t.Run("credential failure", func(t *testing.T) {
		expected := errors.New("test ARM authentication failed")
		requests := 0
		c := testHTTPClient(t, &testCredential{err: expected}, func(_ http.ResponseWriter, _ *http.Request) {
			requests++
		})
		ctx, counter := requestcount.WithCounter(context.Background())
		_, err := c.discover(ctx, Options{RegistryName: "testregistry"})
		if !errors.Is(err, expected) || requests != 0 {
			t.Fatalf("authentication failure lost: err=%v, requests=%d", err, requests)
		}
		if !strings.Contains(counter.Summary(), "total=0 ") {
			t.Fatalf("credential failure counted as an ARG query: %s", counter.Summary())
		}
	})
}

func TestNativeClientRetriesThrottledQuery(t *testing.T) {
	requests := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		if requests == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"code":"TooManyRequests","message":"retry"}}`)
			return
		}
		if err := json.NewEncoder(w).Encode(testPage([]json.RawMessage{registryResource(testRegistry, "testregistry")}, 1, "", false)); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	c := newClient(server.URL, &testCredential{}, &policy.ClientOptions{
		Transport: server.Client(), Retry: policy.RetryOptions{MaxRetries: 1, RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond},
	})
	ctx, counter := requestcount.WithCounter(context.Background())
	id, err := c.resolveRegistry(ctx, Options{RegistryName: "testregistry"})
	if err != nil || id != testRegistry || requests != 2 {
		t.Fatalf("throttled query did not recover: id=%q, err=%v, requests=%d", id, err, requests)
	}
	if !strings.Contains(counter.Summary(), "arg_queries=2 ") {
		t.Fatalf("ARG retry attempts were not counted: %s", counter.Summary())
	}
}
