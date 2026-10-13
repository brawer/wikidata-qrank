<!--
SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
SPDX-License-Identifier: MIT
-->

# Downloads and provenance

Every build of QRank publishes these files under
`https://qrank.toolforge.org/download/`:

| File | Format | Retention |
|---|---|---|
| `datapackage.json` | [Frictionless Data Package](https://datapackage.org/) v2 | latest only |
| `qrank-<YYYYMMDD>.csv.gz` | gzip-compressed CSV, columns `Entity,QRank` | 3 most recent |
| `qrank-stats-<YYYYMMDD>.json` | JSON, a sample of the QRank distribution | 3 most recent |
| `qrank-<YYYYMMDD>.cdx.json` | [CycloneDX](https://cyclonedx.org) 1.7 bill of materials | kept indefinitely |

`<YYYYMMDD>` is the version of the build, which is the date of the item
signals that the ranking was computed from. Dated files never change once
published, and the server marks them as immutable for caching.

The long-standing URLs keep working, and always serve the version that
`datapackage.json` names:

| URL | Serves |
|---|---|
| `/download/qrank.csv.gz` | the current `qrank-<YYYYMMDD>.csv.gz` |
| `/download/qrank-stats.json` | the current `qrank-stats-<YYYYMMDD>.json` |
| `/download/qrank.cdx.json` | the current `qrank-<YYYYMMDD>.cdx.json` |

These responses carry a `Link` header with `rel="canonical"` that names the
dated file, such as `</download/qrank-20261004.csv.gz>`, so you can tell which
version you received. They
also support [conditional
requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Conditional_requests)
with `ETag`, as before.


## The data package

`datapackage.json` is the entry point. It names the current version and lists
every file by relative path, byte size and SHA-256:

```json
{
  "$schema": "https://datapackage.org/profiles/2.0/datapackage.json",
  "name": "qrank",
  "version": "2026-10-04",
  "resources": [
    { "name": "qrank", "path": "qrank-20261004.csv.gz",
      "bytes": 112233445, "hash": "sha256:dc51…", "schema": { … } },
    { "name": "stats", "path": "qrank-stats-20261004.json",
      "bytes": 41234, "hash": "sha256:4f1e…" },
    { "name": "bom", "path": "qrank-20261004.cdx.json",
      "bytes": 5678, "hash": "sha256:e2cb…", "describes": "qrank" }
  ]
}
```

Each `path` is a bare filename. Resolve it against the URL of
`datapackage.json`, which puts it under `https://qrank.toolforge.org/download/`.
The `qrank` resource has a [Table Schema](https://datapackage.org/standard/table-schema/):
`Entity` is a Wikidata item ID matching `^Q[1-9][0-9]*$` and is the primary
key, and `QRank` is an integer of at least 1. Rows are sorted by decreasing
QRank. Items without pageviews are left out.

The builder uploads `datapackage.json` last, after all the files it lists.
If you can read a version in `datapackage.json`, all of its files are there.


## Checking for updates

Fetch `datapackage.json` and compare `version` to the one you have. Only
download the ranking if it changed.

```sh
curl -s https://qrank.toolforge.org/download/datapackage.json | jq -r .version
```

If you already poll `/download/qrank.csv.gz` with `If-None-Match`, that keeps
working too.


## Verifying a download

1. Fetch `datapackage.json`.
2. Resolve the `path` of the `qrank` resource, and fetch it.
3. Check that the SHA-256 of the bytes from step 2 equals the resource's
   `hash`, without the `sha256:` prefix.
4. For full provenance, fetch the `bom` resource, and check that its
   `metadata.component.hashes` has the same SHA-256.

```sh
base=https://qrank.toolforge.org/download
path=$(curl -s $base/datapackage.json | jq -r '.resources[] | select(.name == "qrank") | .path')
hash=$(curl -s $base/datapackage.json | jq -r '.resources[] | select(.name == "qrank") | .hash')
curl -sO $base/$path
echo "${hash#sha256:}  $path" | sha256sum --check
```

If a new build was published between steps 1 and 2, the dated file still
exists for a while, because the server keeps the three most recent versions.


## The bill of materials

Each `qrank-<YYYYMMDD>.cdx.json` is a [CycloneDX](https://cyclonedx.org) 1.7
document about one dated QRank file:

- `metadata.component` is the QRank file. It has the build's `version` (an
  ISO date), the SHA-256 and SHA-512 of the exact bytes, and a
  `pkg:generic/qrank@<date>` Package-URL with `checksum` and `download_url`
  qualifiers.
- `components` lists the inputs, and `dependencies` links the QRank file to
  them:
  - **Wikimedia database dumps**, as one component. Its version is the newest
    dump date of any wiki, and the `qrank:dumps:wikis` property gives the
    number of wikis. Some archived wikis have not been dumped in years but
    still get pageviews, so there is no oldest date.
  - **Wikimedia pageviews** from
    [`pageview_complete`](https://dumps.wikimedia.org/other/pageview_complete/).
    Its version is the last day included, and properties give the first day
    and the number of weeks.
- `metadata.tools` lists every version of `qrank-builder` that wrote a file
  used in the build, each with a `pkg:github/brawer/wikidata-qrank` Package-URL.
  QRank reuses intermediate files from earlier runs, such as the page signals
  of a wiki that has not been dumped again, so a single build can involve more
  than one release. The version that ran the final step points at its exact
  commit, and earlier versions point at their release tag.
- `formulation` has one workflow, "QRank build", that takes the dumps and
  pageviews as inputs, produces the QRank file, and references every builder
  version listed in `metadata.tools`. The internal steps of the pipeline are
  not part of the BOM.

`metadata.timestamp` is the version date of the build, not when the BOM was
written. Generating the BOM again for the same build gives the same bytes.


## Citing QRank in your own BOM

If your pipeline publishes a bill of materials for its own output, you can
record QRank as one of its inputs:

- Add a `data` component, and copy its `purl` and `hashes` from our
  `metadata.component`. The Package-URL is self-contained:

  ```
  pkg:generic/qrank@<date>?checksum=sha256:<hex>&download_url=<encoded url>
  ```

- Give that component an `externalReference` of type `bom` that points to
  `https://qrank.toolforge.org/download/qrank-<YYYYMMDD>.cdx.json`, with that
  file's SHA-256 in `hashes`. It holds the full provenance: the Wikimedia
  inputs and the builder versions.

- To point at the QRank file inside our BOM, rather than at the whole
  document, use a [BOM-Link](https://cyclonedx.org/capabilities/bomlink/):
  `urn:cdx:<serialNumber>/<version>#<bom-ref>`, with the `serialNumber`
  (without its `urn:uuid:` prefix) and `version` of our BOM, and the
  `bom-ref` of its `metadata.component`, such as `qrank-20261004`.
  A `bom-ref` is only an identifier within one BOM document, so a bare
  `bom-ref` cannot refer to our BOM from yours.

We keep every BOM indefinitely, so the dated URL keeps working. If your
provenance needs to outlive this project's hosting, also archive a copy next
to your output.
