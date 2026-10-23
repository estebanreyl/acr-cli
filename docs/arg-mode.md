# ARG Mode: Faster Cleanup Planning and Custom Policies

ARG mode uses Azure Resource Graph (ARG) as a manifest-body cache for
`acr purge`. It keeps live registry metadata as the source of truth for
cleanup decisions, while replacing thousands of individual manifest GETs
with paginated ARG queries.

This guide explains the implementation, shows the queries it sends, and
provides building blocks for more tailored cleanup policies.

> **Experimental and dry-run-only.** `--arg` requires `--dry-run` and either
> `--untagged-only` or `--untagged`. It does not enable deletion. ARG artifact
> indexing must be enabled for your registry; do not assume that every registry
> exposes this inventory. The examples target the public Azure cloud.

## Quick Start

Configure normal registry authentication, for example through your existing
Docker configuration or `--config`. Separately, provide an ARM identity with
access to the registry resource and its ARG artifact records.

For local development:

```sh
az login
az acr login --name <registry-name>
export AZURE_TOKEN_CREDENTIALS=AzureCLICredential

acr purge -r <registry-name> \
    --arg --dry-run --untagged-only --ago 7d \
    --debug-request-counts
```

Azure CLI is only an optional credential source. The Go implementation sends
ARG requests directly using Azure Identity's `DefaultAzureCredential`.
Hosted environments can use managed identity, workload identity, or
service-principal credentials instead. Registry credentials do not grant
ARM access, and an ARM login alone does not configure registry authentication.
See the [authentication options](../README.md#experimental-arg-manifest-cache).

This mode is available in the experimental source implementation described
here, not a claim that an existing released CLI image contains these flags.

## How the Request Reduction Works

Ordinary purge lists metadata, then fetches manifest bodies to inspect
multi-architecture dependencies and OCI subjects. ARG mode changes only
the source of those bodies.

```text
Registry catalog / repository filters
                 |
                 v
Selected repositories ----> ARG Manifest queries ----> Body cache
                 |                                  (repository, digest)
                 v                                         |
Live tag and manifest metadata                             |
                 |                                         |
                 v                                         v
Existing purge planner <--------------------------- GetManifest wrapper
  current tags / simulated untagging                   | hit: indexed body
  age / locks / keep                                  | miss: registry GET
  nested-index traversal / subject checks              |
                 |                                     |
                 v                                     |
         Dry-run deletion plan <------------------------+
```

| Information | Source | Purpose |
| --- | --- | --- |
| Repository selection | Registry catalog and existing filters | Scope the operation and ARG cache |
| Current tag assignments | Registry metadata | Identify tagged manifests and account for planned untagging |
| Age | Registry `lastUpdateTime` | Apply ordinary `--ago` semantics |
| Lock attributes | Registry metadata | Apply existing lock handling |
| Index children | Manifest body `manifests[].digest` | Protect dependencies of retained indexes, including nested indexes |
| Referrer subject | Manifest body `subject.digest` | Preserve existing subject-bearing referrer handling |
| Manifest body | ARG cache, with explicit registry GET fallback | Avoid one registry request per body inspection |

The cache key is **repository plus digest**, not a tag name. The existing
purge planner still decides which manifests are eligible; an ARG row cannot
create a candidate or become a protection root by itself.

Historical ARG **Tag entities are never queried or consumed** by this mode.
They are not current tag assignments. Do not pick the latest history entry
or protect every historical target as an approximation. In particular, doing
so would prevent cleanup of manifests made untagged by the current operation.
Top-level ARG resource `tags` are not an image's current tag list either.

The command loads all indexed manifest bodies for selected repositories,
not just bodies that appear old or untagged. A retained parent can protect
older descendants; filtering the graph to proposed deletion candidates can
hide those dependencies.

### Missing Data and Errors

Missing, invalid, unsupported, or ambiguous duplicate body identities use
counted registry GET fallbacks when the planner requests them. Rejected bodies
also produce warnings. An empty successful ARG inventory is an empty cache,
not proof that the registry is empty.

Authentication, HTTP, malformed inventory, and incomplete/inconsistent
pagination errors abort cache initialization. They do not silently turn the
entire operation into a registry-only run.

Digest-addressed bodies are immutable, but indexed copies still need fidelity
validation. The implementation does not cryptographically verify indexed
bodies against their digests. Index/list child descriptors must have a valid
digest and a nonempty media type; otherwise the body is rejected and fetched
from the registry. Schema checks still cannot detect omitted children or
subjects in otherwise valid bodies. Current-state listings are not an atomic
snapshot across both services. Dry-run parity is useful evidence, not a
deletion-safety guarantee.

## Common Cleanup Scenarios

All examples below produce plans only. Replace registry and repository names
with your own.

### Old Untagged Manifests

Inspect all repositories for eligible untagged manifests older than 30 days:

```sh
acr purge -r <registry-name> --arg --dry-run \
    --untagged-only --ago 30d
```

An untagged child of a retained multi-architecture index is not necessarily
eligible for cleanup. Existing dependency and referrer checks still apply.
Without `--ago`, untagged-only mode considers all past manifests.

### Repository-Scoped Cleanup With Retention

Limit cleanup to build repositories and keep the five most recent eligible
untagged manifests per repository:

```sh
acr purge -r <registry-name> --arg --dry-run \
    --untagged-only --ago 7d --keep 5 \
    --filter '^build/.*$:ignored'
```

The tag portion of `--filter` is ignored in untagged-only mode.
`--keep` does not mean five manifests for the whole registry.

### Expired CI Tags Followed by Manifest Cleanup

Simulate removing old `ci-*` tags, then evaluate untagged manifests using the
post-untag working state:

```sh
acr purge -r <registry-name> --arg --dry-run \
    --untagged --ago 14d --keep 5 \
    --filter '^build/.*$:^ci-.*$'
```

Nonmatching tags remain and continue to protect their targets. `--keep`
applies independently to matching tags and eligible untagged manifests,
per repository. The manifest phase is not restricted to manifests that had
`ci-*` tags: it also considers other eligible untagged manifests in the selected
repositories. This is not a tag-prefix-based manifest ownership policy.

### Minimum and Maximum Retention

ARG mode uses the same count policies as ordinary purge. These limits apply
per repository using live registry timestamps and current tag assignments,
not ARG ordering or historical Tag entities.

| Option | Behavior |
| --- | --- |
| `--keep N` | Preserve the newest N items among those otherwise selected for deletion |
| `--min-tags N` | Protect the newest N matching tags from age-based deletion |
| `--max-tags N` | Select matching tags outside the newest N, regardless of age |
| `--min-untagged-manifests N` | Protect the newest N dangling manifest candidates from age-based deletion |
| `--max-untagged-manifests N` | Select dangling manifest candidates outside the newest N, regardless of age |

Minimums require `--ago`. Maximums can be used without an age cutoff; when
combined with `--ago`, either exceeding the maximum or satisfying the age
rule selects an item. A minimum protects the newest items from the age rule.
`--keep` cannot be combined with any minimum/maximum flag.

For example, protect the newest 20 dangling candidates, delete other candidates
older than 30 days, and also select candidates outside the newest 100:

```sh
acr purge -r <registry-name> --arg --dry-run \
    --untagged-only --ago 30d \
    --min-untagged-manifests 20 --max-untagged-manifests 100
```

Or simulate a count-only tag and manifest cleanup, without an age cutoff:

```sh
acr purge -r <registry-name> --arg --dry-run \
    --untagged --filter '^build/.*$:^ci-.*$' \
    --max-tags 50 --max-untagged-manifests 100
```

Tag policies run first; manifest policies use the simulated post-untag state.
Tagged objects, referrers, and children of retained indexes remain protected.
Dependency protection can leave more items than the requested maximum.
All limits are nonnegative, zero is meaningful, and a minimum cannot exceed
its corresponding maximum. Tag limits cannot be used with `--untagged-only`.

### Compare With Ordinary Purge

Run the same scenario twice, adding only `--arg` for the second invocation:

```sh
acr purge -r <registry-name> --dry-run --untagged-only \
    --ago 7d --concurrency 4 --debug-request-counts

acr purge -r <registry-name> --dry-run --untagged-only \
    --ago 7d --concurrency 4 --debug-request-counts --arg
```

Compare sorted `Would delete:` identities, not just counts. A changing registry
can legitimately produce different plans between runs; investigate any
difference rather than assuming the cache is equivalent.

## Queries Sent by the CLI

These are the logical queries generated by the implementation, formatted for
readability. Replace placeholders before running them. Repository names are
expanded from registry-side selection and quoted individually by the client;
the CLI does not send the repository regex to ARG.

### 1. Resolve the Registry Resource

Unless `--arg-registry-id` is provided:

```kusto
Resources
| where type =~ 'microsoft.containerregistry/registries'
| where name =~ '<registry-name>'
| project id, name
| order by id asc
```

The request asks for at most two rows to detect ambiguity. By default it
queries subscriptions accessible to the ARM identity;
`--arg-subscription <subscription-uuid>` constrains lookup. Resolution must
produce exactly one complete match.

### 2. Fetch Manifest Bodies for Selected Repositories

```kusto
ContainerRegistryResources
| where type =~ 'microsoft.containerregistry/registries/artifacts'
| where id startswith '/subscriptions/<subscription-uuid>/resourceGroups/<resource-group>/providers/Microsoft.ContainerRegistry/registries/<registry-name>/'
| where kind =~ 'Manifest'
| where tostring(properties.registryArtifact.repository) in ("build/service-a","build/service-b")
| order by id asc
```

The subscription is also supplied in the API request body. There is no age
filter, Tag query, current-tag inference, or deletion-set computation in this
query. Full records retain `id` for pagination and include
`properties.registryArtifact.manifest`.

### API and Pagination

The default endpoint is:

```text
POST https://southeastasia.management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2024-04-01
```

Southeast Asia is the experiment's default ARM endpoint, not a restriction
that registries must be located there. `--arg-region global` uses
`https://management.azure.com`; `--arg-region` and `--arg-api-version` allow
explicit overrides. Endpoint availability and access must be verified for
your environment.

A manifest-query request has this shape:

```json
{
  "subscriptions": ["<subscription-uuid>"],
  "query": "<manifest query above>",
  "options": {
    "resultFormat": "objectArray",
    "$top": 1000
  }
}
```

Follow the response's `$skipToken` by adding it to `options` on subsequent
requests. Continue while a token is present, even if `resultTruncated` is
`"false"`. The CLI checks counts, totals, continuation completeness, and
repeated tokens; changing totals cause an error rather than a partial plan.

For an interactive API probe, save the request as `request.json`:

```sh
az rest --method post \
    --url 'https://southeastasia.management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2024-04-01' \
    --resource 'https://management.azure.com/' \
    --headers Content-Type=application/json \
    --body @request.json
```

This shows one request, not a complete pagination loop. `az graph query`
is convenient for exploratory queries but does not expose the same
regional-endpoint/API-version controls. It requires the `resource-graph`
extension; the Go CLI does not.

## Kusto Query Building Blocks

ARG accepts a subset of Kusto Query Language (KQL), not the full Azure Data
Explorer language. The following queries target the observed public ARG
`ContainerRegistryResources` schema. They do not assume access to an internal
backing Kusto cluster or a private table.

These are exploratory inventory queries, **not deletion plans**. Repeat the
registry scope in every query. Projection/aggregation changes the response
shape; use the full-record query above for the CLI body cache.

### Summarize Indexed Content

```kusto
ContainerRegistryResources
| where type =~ 'microsoft.containerregistry/registries/artifacts'
| where id startswith '/subscriptions/<subscription-uuid>/resourceGroups/<resource-group>/providers/Microsoft.ContainerRegistry/registries/<registry-name>/'
| where kind =~ 'Manifest'
| extend repository = tostring(properties.registryArtifact.repository),
         mediaType = tostring(properties.registryArtifact.manifest.mediaType)
| summarize indexedManifests = count() by repository, mediaType
| order by repository asc, mediaType asc
```

These are indexed-record counts, not verified current registry inventory or
untagged counts. Coverage, duplicates, and freshness must be checked separately.

### Inspect Parent/Child Edges

```kusto
ContainerRegistryResources
| where type =~ 'microsoft.containerregistry/registries/artifacts'
| where id startswith '/subscriptions/<subscription-uuid>/resourceGroups/<resource-group>/providers/Microsoft.ContainerRegistry/registries/<registry-name>/'
| where kind =~ 'Manifest'
| where tostring(properties.registryArtifact.manifest.mediaType) in ("application/vnd.oci.image.index.v1+json","application/vnd.docker.distribution.manifest.list.v2+json")
| mv-expand child = properties.registryArtifact.manifest.manifests
| project repository = tostring(properties.registryArtifact.repository),
          parentDigest = tostring(properties.registryArtifact.digest),
          childDigest = tostring(child.digest),
          childMediaType = tostring(child.mediaType)
```

Each output row is one direct edge. Join and traverse by repository plus
digest, not digest alone. Children can themselves be indexes; one `mv-expand`
does not compute transitive reachability. The CLI traverses nested indexes
locally. Config and layer blob digests are not child manifest edges.

ARG limits array expansion; an interactive edge query is not proof that every
descriptor was returned. Fetch the original bodies and inspect arrays locally
for complete traversal, as the CLI does.

### Inspect Referrer Subjects

```kusto
ContainerRegistryResources
| where type =~ 'microsoft.containerregistry/registries/artifacts'
| where id startswith '/subscriptions/<subscription-uuid>/resourceGroups/<resource-group>/providers/Microsoft.ContainerRegistry/registries/<registry-name>/'
| where kind =~ 'Manifest'
| extend subjectDigest = tostring(properties.registryArtifact.manifest.subject.digest)
| where isnotempty(subjectDigest)
| project repository = tostring(properties.registryArtifact.repository),
          referrerDigest = tostring(properties.registryArtifact.digest),
          subjectDigest,
          artifactType = tostring(properties.registryArtifact.manifest.artifactType)
```

The edge points **from referrer to subject**. For example, a signature may
refer to an image digest through `subject`. This query identifies indexed
subject-bearing bodies; an empty result does not prove the absence of registry
referrers. It is not equivalent to the registry referrers API.

Existing purge preserves subject-bearing referrers and, for retained
subject-bearing indexes, their child dependencies. Do not assume that this
implements a custom policy that automatically retains every subject of every
referrer, or deletes signatures when their subject becomes eligible.

### Explore Organization-Specific Annotations

For registries whose images carry an agreed manifest annotation:

```kusto
ContainerRegistryResources
| where type =~ 'microsoft.containerregistry/registries/artifacts'
| where id startswith '/subscriptions/<subscription-uuid>/resourceGroups/<resource-group>/providers/Microsoft.ContainerRegistry/registries/<registry-name>/'
| where kind =~ 'Manifest'
| where tostring(properties.registryArtifact.manifest.annotations["com.example.lifecycle"]) == 'temporary'
| project repository = tostring(properties.registryArtifact.repository),
          digest = tostring(properties.registryArtifact.digest),
          indexedPushedAt = todatetime(properties.registryArtifact.pushedAt)
```

`com.example.lifecycle` is an illustrative customer-defined key, not a
built-in ACR lifecycle contract. The CLI does not currently accept arbitrary
KQL or filter purge by annotations. Such a query can supply policy hints to
your own planner, but must not shrink its dependency/protection inventory.
ARG `pushedAt` is not a substitute for registry `lastUpdateTime`.

## Building a Custom Cleanup Planner

For more tailored policies, separate **policy selection** from **protection**
and **execution**:

1. Define ownership and retention policy. Examples include different ages for
   CI versus release repositories, build annotations, or keeping a deployment
   allowlist supplied by your deployment system.
2. Read current registry metadata for the full scope. Use current tags, dates,
   and locks, not historical ARG Tag entities. For a multi-registry planner,
   include registry identity in every key as well as repository and digest.
3. Load complete Manifest bodies for that scope through ARG. Resolve missing
   or rejected bodies explicitly from the registry; record coverage and
   fallback counts. Treat policy-query matches as hints joined to live
   inventory, never as proof that a manifest still exists or is untagged.
4. Calculate planned tag removals, then remaining current tag assignments.
   Preserve the effects of tags that do not match the policy.
5. Build parent/child and referrer/subject relationships. Traverse retained
   indexes transitively and define referrer behavior explicitly. Never remove
   retained roots or intermediate indexes from the graph simply because they
   fail the age or annotation filter.
6. Apply policy eligibility minus protection, then produce a reasoned dry-run
   plan. Report why items are retained as well as why they are selected.
   Fail explicitly on unresolved data rather than presenting a partial plan
   as complete.
7. Before implementing a separate executor, define freshness checks,
   current-state revalidation, lock handling, and recovery from partial
   untagging/deletion. This experimental CLI does not provide ARG execution.

For offline analysis in full Kusto/Azure Data Explorer, you can ingest the
exported rows and live registry metadata into tables you control. Define their
schemas explicitly; the examples above do not imply that public ARG metadata
and registry current-state tables can be joined on the service automatically.
Keep nested graph traversal in your planner unless your analysis environment
provides and validates an equivalent complete implementation.

## Reading Results and Measuring Savings

Example cache output:

```text
ARG body cache: indexed=21684 rejected=0 pages=22 hits=11139 registry GET fallbacks=0 (missing=0 invalid=0)
```

`indexed` counts cached identities; `hits` counts body requests, which can
repeat a digest. `pages` counts manifest-query pages, excluding registry
lookup and HTTP retries. The request-counter summary includes those query
attempts and separates registry authentication, catalog, tags, manifest
metadata, manifest GETs, and other registry requests.

`--debug-request-counts` includes SDK retries and failed send attempts. It
excludes ARM credential acquisition, external processes, and HTTP-client
redirects. It records no request URLs, headers, tokens, or bodies, and also
prints on failure. It is not a complete network trace.

### Observed Full-Registry Comparison

One paired experiment on October 19, 2026 covered 40 repositories and
21,684 indexed manifest bodies, with `--ago 0d --concurrency 4`:

| Dry-Run Scenario | Original Requests | Hybrid Requests | Original Seconds | Hybrid Seconds |
| --- | ---: | ---: | ---: | ---: |
| Untagged-only | 11,486 | 303 | 508.09 | 348.70 |
| Untag, then cleanup | 11,255 | 343 | 600.78 | 351.22 |

Exact deletion identities matched: 9,724 manifests in the first scenario;
1,707 tags plus 20,772 manifests in the second. Each hybrid run used 22
manifest pages plus one registry lookup, with no rejected bodies or fallback
GETs. Total request attempts fell by 97.36% and 96.95%; elapsed time fell by
31.37% and 41.54%, respectively.

These are observations, not throughput guarantees or controlled benchmark
averages. Smaller scenarios can be slower because ARG initialization and
body download outweigh saved GETs. Tag/metadata requests, memory use, inventory
size, indexing coverage, network conditions, and concurrency still matter.
Candidate parity, including repositories containing referrer samples, does
not independently verify every indexed body or subject.

## Implementation and Investigation References

| Component | Source |
| --- | --- |
| Purge integration and flags | [`cmd/acr/purge.go`](../cmd/acr/purge.go) |
| Manifest-body client wrapper | [`cmd/acr/purge_arg.go`](../cmd/acr/purge_arg.go) |
| ARG native HTTP, authentication, registry lookup | [`internal/arg/client.go`](../internal/arg/client.go) |
| Manifest query, cache validation, fallback counters | [`internal/arg/manifest_cache.go`](../internal/arg/manifest_cache.go) |
| Existing dependency and referrer behavior | [`cmd/repository/image_functions.go`](../cmd/repository/image_functions.go) |
| Temporary HTTP attempt counters | [`internal/requestcount/counter.go`](../internal/requestcount/counter.go) |

The [ARG purge integration requirements spec](https://github.com/azure-management-and-platforms/acr-specs/pull/209)
retains the original pure-ARG experiments, schema findings, limitations, and
comparison details. It is historical evidence, not a replacement for this
guide. The standalone pure-ARG prototype and its historical-tag candidate
selector have been removed.
Use `acr purge --arg --dry-run` for the supported hybrid experiment.
