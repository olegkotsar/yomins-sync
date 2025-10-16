# S3 Object Listing Strategies

This module exposes three ways to enumerate objects from an S3-compatible bucket. Choose the approach that best matches your data layout and performance goals.

## API

```go
ListObjectsFlat(ctx context.Context, prefix string) ([]model.RemoteFile, error)
ListObjectsRecursiveStream(ctx context.Context) (<-chan model.RemoteFile, <-chan error)
ListObjectsByLevelStream(ctx context.Context, level int) (<-chan model.RemoteFile, <-chan error)
```

### `ListObjectsFlat`
**What it does:** Fetches **all objects under a single prefix** (no delimiter), returning a slice.

**When to use:**
- Small to medium listings where you want an in-memory slice (debugging, spot checks, tests).
- One-off scans of a known prefix.

**Notes:** Non‑streaming. For very large listings you may prefer one of the streaming variants to avoid memory pressure.

---

### `ListObjectsRecursiveStream` (default)
**What it does:** Performs a **full recursive traversal** starting at the root prefix using `Delimiter:"/"` to discover sub‑prefixes, then streams files as they are discovered.

**When to use:**
- **General purpose / safe default** when you don’t know the depth or distribution of files.
- You want results to start flowing immediately (backpressure-friendly via channels).

**Trade‑offs:**
- Can generate **more API requests** when many prefixes contain few files (each discovered prefix triggers at least one `ListObjectsV2` call).
- Very robust for irregular or unknown hierarchies.

---

### `ListObjectsByLevelStream`
**What it does:** First **collects prefixes up to a specified level** using `Delimiter:"/"`, then, for each collected prefix, lists files **flat (no delimiter)** to maximize objects per request. Work is distributed across workers by prefix.

**When to use:**
- You know (or assume) a useful split depth for your tree (e.g., **level 2**).
- You want **predictable work partitioning** across workers.
- Directories contain **hundreds/thousands of files** so each flat listing is efficient.

**Trade‑offs:**
- A small upfront cost to collect prefixes at the chosen level.
- If many collected prefixes are sparse, benefits diminish.

---

## Choosing a strategy

- Start with **`ListObjectsRecursiveStream`** unless you have a clear reason to tune. It’s the **most universal** and is **enabled by default** in typical configurations.
- Switch to **`ListObjectsByLevelStream`** when:
  - Your data is concentrated under a consistent depth (e.g., second‑level folders), and
  - You want to **reduce request count** by listing each prefix flat, and/or
  - You want to **scale workers** one‑to‑one (or many‑to‑one) with known prefixes.

---

## Performance (benchmarks & observations)

**Dataset characteristics (author’s environment):**
- ~9,505,700 files
- Most files reside in **second‑level directories**

**Results:**

| Workers | RPS limit | Strategy    | Files    | Duration |
|---------|-----------|-------------|----------|----------|
| 10      | 100       | Recursive   | 9,505,700| 14m50.188s |
| 20      | 100       | By Level    | 9,505,700| 14m07.425s |
| 20      | 300       | Recursive   | 9,505,700| 7m34.668s  |
| 20      | 300       | By Level    | 9,505,700| **2m27.366s** |

**Interpretation (opinionated, data‑dependent):**
- On this dataset (heavy second‑level fan‑out), **`ListObjectsByLevelStream` at level 2** is **significantly faster**, especially with higher RPS, because each worker lists large, dense prefixes **flat** (maximizing ~1000 keys per request) after a quick, bounded prefix discovery.
- **`Recursive`** remains a solid default and performs competitively at lower RPS, but it tends to issue **more requests** on deep or sparse trees due to delimiter‑based discovery at every level.

> Your mileage will vary. The optimal choice depends on your directory depth, file density per prefix, network latency, allowed RPS, and worker count.

---

## Practical guidance

- **If uncertain:** Use **`ListObjectsRecursiveStream`**.
- **If most files live under level 2 (or a known depth):** Try **`ListObjectsByLevelStream(level=<that depth>)`** and align **worker count** with the number of collected prefixes.
- **Tune RPS & workers together:** Higher RPS magnifies the benefit of the **By‑Level** approach when prefixes are dense.
- **Monitor request counts and throttling:** Flat listings reduce calls per file; watch for 429s/slowdowns and adjust `MaxRPS`/workers accordingly.

---

## Minimal usage examples

```go
// Recursive stream (default/general purpose)
filesCh, errCh := src.ListObjectsRecursiveStream(ctx)
for {
    select {
    case f, ok := <-filesCh:
        if !ok { filesCh = nil }
        // process f
    case err, ok := <-errCh:
        if ok { /* handle error */ }
    }
    if filesCh == nil && errCh == nil { break }
}
```

```go
// By‑level stream (e.g., level 2)
filesCh, errCh := src.ListObjectsByLevelStream(ctx, 2)
// handle channels as above
```

```go
// Flat listing under a specific prefix (returns a slice)
files, err := src.ListObjectsFlat(ctx, "2025/08/")
if err != nil { /* handle */ }
for _, f := range files { /* process */ }
```

---

## Notes
- Streaming variants are backpressure‑friendly; control throughput with **worker count** and **RPS limiter**.
- `ListObjectsByLevelStream(level=0)` is effectively a flat recursive listing from root and usually not useful—prefer `Recursive` instead unless you have a specific reason.
- Always validate with your own data: measure **total duration**, **requests made**, and **throttle errors**.

