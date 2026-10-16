<!--
SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
SPDX-License-Identifier: MIT
-->

# Wikidata QRank: Technical Design

QRank ranks [Wikidata](https://www.wikidata.org/) items by how often
their pages on Wikipedia, Wikivoyage, Wikibooks, Wikispecies and other
Wikimedia projects get viewed. A ranking signal helps when time or
space is too limited to handle everything: when **fixing data**, it
tells which items matter most; in **cartography**, it tells which
features deserve prominence. For example, [this map of Swiss
castles](https://castle-map.infs.ch/#46.82825,8.19305,8z) uses QRank
to decide which castles get a large icon and which ones a tiny dot.


## Goals

* Rank Wikidata items with good coverage across topics, cultures and
  regions.
* Keep the ranking fresh with automatic updates.
* Be resilient to short-term spikes and seasonal effects.
* Publish the ranking for bulk download, in a format that is trivial to
  understand and to read in common programming languages.
* Make it verifiable which inputs and which software produced each
  published version.

For now, a website to browse the ranking and an API to query single
items are *non-goals*.


## Overview

QRank runs on [Toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge)
as two components of one image, configured in
[toolforge.yaml](../toolforge.yaml):

* `qrank-builder`, a daily scheduled job, computes the ranking. It
  reads the Wikimedia dumps from `/public/dumps/public`, and keeps its
  intermediate and published files in the S3 bucket `qrank`.
* `webserver`, a continuous job, serves
  [qrank.toolforge.org](https://qrank.toolforge.org/) from a local copy
  of the published files.

Files under `public/` in the bucket are published; everything else is
internal to the pipeline. [Downloads and provenance](downloads.md)
describes the published files for the people who use them.


## Build pipeline

The pipeline is incremental. Each stage stores its output in the
bucket, and a later run only re-builds what is missing or out of date:

| Stage | Stored as | Re-built when |
|---|---|---|
| 1. weekly pageviews | `pageviews/pageviews-YYYY-Www.zst` | a new week becomes available |
| 2. page signals, per wiki | `page_signals/<wiki>-<dumpdate>-page_signals.zst` | that wiki gets a new dump |
| 3a. item coordinates | `item_coordinates/wikidatawiki-<dumpdate>-item_coordinates.zst` | Wikidata gets a new dump |
| 3b. item signals | `public/item_signals-YYYYMMDD.parquet` | any input is newer |
| 4. ranking and statistics | `public/qrank-YYYYMMDD.csv.gz`, `public/qrank-stats-YYYYMMDD.json` | new item signals |
| 5. bill of materials and data package | `public/qrank-YYYYMMDD.cdx.json`, `public/datapackage.json` | together with stage 4 |

All intermediate files are sorted and zstd-compressed.

1. **Weekly pageviews.**
   [pageviews.go](../cmd/qrank-builder/pageviews.go) sums the daily
   [Pageview complete](https://dumps.wikimedia.org/other/pageview_complete/readme.html)
   files of each [ISO week](https://en.wikipedia.org/wiki/ISO_week_date)
   by wiki and page ID. The build uses the 52 most recent complete weeks,
   and deletes older ones. For example, the line `en.wikipedia,3422,7`
   means that [en.wikipedia.org/?curid=3422](https://en.wikipedia.org/?curid=3422)
   was viewed 7 times that week.

2. **Page signals.** For each of the roughly thousand Wikimedia wikis,
   [pagesignals.go](../cmd/qrank-builder/pagesignals.go) reads the
   `page` and `page_props` tables from the wiki's latest dump. It writes
   one line per page that is about a Wikidata item: page ID, item ID,
   and the page's size in bytes of wikitext. For Wikidata itself, the
   line also has the item's number of claims, identifiers and sitelinks.
   Since a wiki's file is only re-built after a new dump, most page
   signals are reused from earlier runs. The build also extracts titles,
   redirects, interwiki links and page-to-item mappings for every wiki;
   these are not used for the ranking yet, but prepare for
   [PageRank](#future-work).

3. **Item coordinates.** [coordinates.go](../cmd/qrank-builder/coordinates.go)
   reads the `geo_tags` table of the Wikidata dump, keeps all
   coordinates on Earth, and maps their page IDs to item IDs with the
   Wikidata page signals. It writes lines such as `72,47.37,8.54`,
   sorted by zoom-10 Web Mercator tile rather than by item. This lets
   the next step look them up in the OSMViews GeoTIFF in spatial order,
   which keeps its tile cache warm: for the 12 million coordinates in
   Wikidata, this takes about 15 seconds instead of more than ten
   minutes in item order.

   **Item signals.** [itemsignals.go](../cmd/qrank-builder/itemsignals.go)
   joins the pageviews with the page signals. Both are sorted by wiki
   and page ID, so [linemerger.go](../cmd/qrank-builder/linemerger.go)
   merges them in one linear scan, using a
   [min heap](https://en.wikipedia.org/wiki/Heap_(data_structure)). The
   resulting per-page records are sorted by item ID and summed, giving
   one row per item with the columns `item`, `pageviews_52w`,
   `wikitext_bytes`, `statement_count`, `identifier_count`,
   `sitelink_count` and `osmviews_score`. For the latter,
   [osmviews.go](../cmd/qrank-builder/osmviews.go) looks up the item
   coordinates in the current [OSMViews](https://osmviews.brawer.ch)
   GeoTIFF with [osmviews-go](https://github.com/brawer/osmviews-go),
   and keeps the highest score per item. The GeoTIFF is about 600 MB.
   The bucket keeps the two newest versions under
   `osmviews/osmviews-YYYYMMDD.tiff`, next to their bill of materials,
   so the build does not download it again while the upstream version
   is unchanged, and falls back to the newest cached version if
   osmviews.brawer.ch is unreachable. A new OSMViews version alone does
   not trigger a new build; it gets picked up with the next new dump or
   pageviews week. [item_signals_writer.go](../cmd/qrank-builder/item_signals_writer.go)
   writes them as [Apache Parquet](https://parquet.apache.org/) with
   zstd compression, in row groups of about a million items, so the
   writer needs little memory and readers can skip ranges of items.
   The file is published; see [Downloads](downloads.md#item-signals).
   The date in the file name is the newest date of any input: a dump
   date, or the last day of the latest pageviews week.

4. **Ranking and statistics.** [qrank.go](../cmd/qrank-builder/qrank.go)
   sorts the items by `pageviews_52w`, which is their QRank, in
   decreasing order and with the item ID as secondary key. Items without
   pageviews are left out. The result is a gzip-compressed CSV file with
   the columns `Entity` and `QRank`. [stats.go](../cmd/qrank-builder/stats.go)
   then writes a small JSON file for plotting the QRank distribution:
   the top 50 items, the median, and a sample of the rest.

5. **Bill of materials and data package.** The build describes the
   ranking and the item signals in a [CycloneDX](https://cyclonedx.org)
   1.7 bill of materials ([bom.go](../cmd/qrank-builder/bom.go)), and
   lists the ranking, the item signals, the statistics and the BOM with
   their sizes and SHA-256 digests in a
   [Frictionless Data Package](https://datapackage.org/) v2
   ([datapackage.go](../cmd/qrank-builder/datapackage.go)). It uploads
   the BOM, the statistics, the ranking and finally `datapackage.json`,
   so a version named in `datapackage.json` is always complete. A
   ranking counts as fresh only if all four files exist and
   `datapackage.json` names its version and lists its item signals.
   The item signals are already in storage from stage 3.

The bucket keeps the three newest versions of each wiki's files, of the
item signals, and of the ranking and statistics. BOMs are kept forever,
since they are small and document what was published.


## Provenance

Because the pipeline reuses intermediate files, one ranking can be the
work of several `qrank-builder` releases. For example, the page signals
of a wiki that has not been dumped again since the last release were
written by an older release. To record this, the builder tracks which
software wrote each file:

* Every file the builder stores carries its producer as S3 user
  metadata, such as `X-Amz-Meta-Qrank-Software: QRank/v2.2.0+1a2b3c4d5e6f`.
  `PutInStorage()` in [s3.go](../cmd/qrank-builder/s3.go) sets it, so
  every stage gets it. Page signals and weekly pageviews without this
  stamp are treated as stale.
* Building the item signals also stores
  `item_signals/item_signals-YYYYMMDD.provenance.json`
  ([provenance.go](../cmd/qrank-builder/provenance.go)). It records the
  newest dump date, the number of wikis, the pageviews date range, and
  the stamps of all page signals, pageviews and item coordinates that
  were read, and the OSMViews version with its hashes. Item signals
  without such a record, or with a record that does not name OSMViews,
  are re-built.
* The BOM is built from that record. It names the Wikimedia dumps,
  pageviews and OSMViews as inputs, and lists every builder version in a single
  "QRank build" workflow. Internal pipeline steps are not part of the BOM.

The BOM and data package are written by hand with `encoding/json`
rather than with a library. This keeps dependencies small and the output
byte-stable: building the same version again gives the same bytes. CI
validates golden copies with `cyclonedx-cli` and against the Frictionless
v2 schema.

[Downloads and provenance](downloads.md) explains how to verify a
download against the data package and the BOM, and how to cite QRank
in your own BOM.


## Webserver

[storage.go](../cmd/webserver/storage.go) periodically lists the
published files in the bucket and copies new ones to a local working
directory. [main.go](../cmd/webserver/main.go) serves them under
`/download/`:

* Each retained dated file, such as `qrank-20261004.csv.gz`, is served
  under its own name with `Cache-Control: immutable`.
* The undated names `qrank.csv.gz`, `qrank-stats.json`,
  `qrank.cdx.json` and `item_signals.parquet` serve the version that
  `datapackage.json` names, with a `Link` header with
  `rel="canonical"` that names the dated file.
* Downloads support HTTP range requests, which Parquet readers such
  as DuckDB use to fetch only the columns and row groups they need.
* All downloads support
  [conditional requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Conditional_requests),
  using the object's entity tag from storage as `ETag`.

The home page is a static string.


## Performance

* **Parallelism.** Per-wiki files are built by one worker per CPU core,
  and the seven daily files of a pageviews week are read in parallel.
* **External sorting.** Sorting uses an
  [external sort](https://en.wikipedia.org/wiki/External_sorting), so
  the builder handles far more data than fits in its 3 GB of memory.
* **Compression.** Intermediate files use
  [zstd](https://en.wikipedia.org/wiki/Zstd), which compresses well and
  decompresses fast. The published ranking uses gzip, which every
  client can read.
* **Local copies.** Stages download their inputs from the bucket to
  local disk before processing, rather than streaming them, because
  long reads from Wikimedia's object storage proved unreliable
  ([#40](https://github.com/brawer/wikidata-qrank/issues/40)).

It is not a problem if the ranking is a few days stale.


## Future work

**Signal smearing.** QRank is the plain sum of pageviews, so items
without Wikimedia pages get no rank. Propagating a fraction of an
item's rank to related items, such as from an author to their
publications or from a painter to their works, could fill that gap.

**PageRank.** Running [PageRank](https://en.wikipedia.org/wiki/PageRank)
on the links between Wikimedia pages would be similar to the
[Danker](https://github.com/athalhammer/danker) project, but would
offer a downloadable dataset built inside the Wikimedia infrastructure.
The interwiki links and titles that the pipeline already extracts are
groundwork for this.
