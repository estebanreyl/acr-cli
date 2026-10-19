// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package arg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/acr-cli/internal/requestcount"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

type client struct {
	endpoint string
	pipeline runtime.Pipeline
}

func newClient(endpoint string, credential azcore.TokenCredential, options *policy.ClientOptions) *client {
	return &client{
		endpoint: endpoint,
		pipeline: runtime.NewPipeline("acr-cli-arg", "v0.0.0", runtime.PipelineOptions{
			PerRetry: []policy.Policy{
				runtime.NewBearerTokenPolicy(credential, []string{"https://management.azure.com/.default"}, nil),
				queryCountPolicy{},
			},
		}, options),
	}
}

type queryCountPolicy struct{}

func (queryCountPolicy) Do(req *policy.Request) (*http.Response, error) {
	if counter := requestcount.FromContext(req.Raw().Context()); counter != nil {
		counter.Add("arg_queries")
	}
	return req.Next()
}

func (c *client) query(ctx context.Context, body queryRequest) (queryResponse, error) {
	var page queryResponse
	request, err := runtime.NewRequest(ctx, http.MethodPost, c.endpoint)
	if err != nil {
		return page, err
	}
	if err := runtime.MarshalAsJSON(request, body); err != nil {
		return page, err
	}
	response, err := c.pipeline.Do(request)
	if err != nil {
		return page, fmt.Errorf("ARG request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return page, runtime.NewResponseError(response)
	}
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		return page, fmt.Errorf("decode ARG response: %w", err)
	}
	return page, nil
}

func (c *client) resolveRegistry(ctx context.Context, options Options) (string, error) {
	if options.RegistryID != "" {
		return options.RegistryID, nil
	}
	name, err := registryName(options.RegistryName)
	if err != nil {
		return "", err
	}
	body := queryRequest{
		Query: "Resources | where type =~ 'microsoft.containerregistry/registries'" +
			fmt.Sprintf(" | where name =~ '%s' | project id, name | order by id asc", name),
		Options: queryOptions{ResultFormat: "objectArray", Top: 2},
	}
	if options.SubscriptionID != "" {
		body.Subscriptions = []string{options.SubscriptionID}
	}
	page, err := c.query(ctx, body)
	if err != nil {
		return "", fmt.Errorf("resolve registry through ARG: %w", err)
	}
	if page.Data == nil || page.Count == nil || page.TotalRecords == nil ||
		*page.Count != len(page.Data) || *page.TotalRecords < 0 {
		return "", errors.New("ARG returned invalid registry discovery metadata")
	}
	truncated := string(page.ResultTruncated)
	if truncated != "false" && truncated != `"false"` || page.SkipToken != "" || *page.TotalRecords > 1 || len(page.Data) > 1 {
		return "", errors.New("ARG registry discovery is ambiguous or incomplete; specify a subscription or explicit registry ARM ID")
	}
	if *page.TotalRecords == 0 && len(page.Data) == 0 {
		return "", fmt.Errorf("registry %q was not found in ARG's Resources table for this identity/scope", name)
	}
	if *page.TotalRecords != 1 || len(page.Data) != 1 {
		return "", errors.New("ARG returned inconsistent registry discovery metadata")
	}
	var resource struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(page.Data[0], &resource); err != nil {
		return "", fmt.Errorf("decode ARG registry resource: %w", err)
	}
	if !strings.EqualFold(resource.Name, name) {
		return "", errors.New("ARG returned a registry resource with an unexpected name")
	}
	if _, err := registrySubscription(resource.ID); err != nil {
		return "", fmt.Errorf("invalid resolved registry scope: %w", err)
	}
	options.RegistryID = resource.ID
	if err := options.Validate(); err != nil {
		return "", fmt.Errorf("invalid resolved registry scope: %w", err)
	}
	return resource.ID, nil
}

func (c *client) discover(ctx context.Context, options Options) (Discovery, error) {
	return c.discoverArtifacts(ctx, options, nil)
}

func (c *client) discoverArtifacts(ctx context.Context, options Options, repositories []string) (Discovery, error) {
	var result Discovery
	if err := options.Validate(); err != nil {
		return result, err
	}
	registryID, err := c.resolveRegistry(ctx, options)
	if err != nil {
		return result, err
	}
	query, err := artifactQuery(registryID)
	if repositories != nil {
		query, err = manifestQuery(registryID, repositories)
	}
	if err != nil {
		return result, err
	}
	subscription, err := registrySubscription(registryID)
	if err != nil {
		return result, err
	}
	fetch := fetchArtifacts
	if repositories != nil {
		fetch = fetchArtifactRows
	}
	pages := 0
	rows, err := fetch(c.endpoint, subscription, query, func(_ string, body queryRequest) (queryResponse, error) {
		pages++
		return c.query(ctx, body)
	})
	if err != nil {
		return result, fmt.Errorf("query ARG artifacts for registry %s: %w", registryID, err)
	}
	return Discovery{Mode: "schema-discovery", RegistryID: registryID, Data: rows, QueryPages: pages}, nil
}

func defaultClient(ctx context.Context, options Options) (*client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := options.Validate(); err != nil {
		return nil, err
	}
	endpoint, err := options.endpoint()
	if err != nil {
		return nil, err
	}
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("initialize ARM credential: %w", err)
	}
	return newClient(endpoint, credential, &policy.ClientOptions{
		Retry: policy.RetryOptions{TryTimeout: time.Minute},
	}), nil
}

func Discover(ctx context.Context, options Options) (Discovery, error) {
	c, err := defaultClient(ctx, options)
	if err != nil {
		return Discovery{}, err
	}
	return c.discover(ctx, options)
}
