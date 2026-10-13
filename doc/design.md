<!--
SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
SPDX-License-Identifier: MIT
-->

# Wikidata QRank: Technical Design

QRank is a ranking signal for [Wikidata](https://www.wikidata.org/)
entities.  It gets computed by aggregating page view statistics for
Wikipedia, Wikitravel, Wikibooks, Wikispecies and other Wikimedia
projects.  A ranking signal like QRank is useful when time or space is
too limited to handle everything.  For example, when **improving
data**, it often makes sense to focus on the most important issues; a
ranking signal helps to decide on importance.  Likewise, high-quality
**maps** need a ranking signal for cartographic prominence; [this map
of Swiss castles](https://castle-map.infs.ch/#46.82825,8.19305,8z)
uses QRank to decide which castles deserve a large icon and which ones
just a tiny dot.


## Goals

* Compute a ranking signal for Wikidata with good coverage
  across diverse topics, cultures and geographic regions.
* Keep the signal fresh, with regular automatic updates.
* Be resilient to short-term popularity spikes and seasonal effects.
* Make the signal available for bulk download. The format should be
  trivial to understand, and easily read in common programming
  languages.

Initially, it is an explicit *non-goal* to build a website for
people to interactively browse the ranking, or an API for software
to query individual rankings. Both would be useful, but for the time
being, we focus on exposing ranking data as a bulk downloadable
file.


## Overview

The QRank system consists of two parts. Both run in the
[Wikimedia Cloud](https://wikitech.wikimedia.org/wiki/Help:Cloud_Services_Introduction) on
[Toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge).

* `qrank-builder` is an automated pipeline that computes the ranking.

* `qrank-webserver` handles queries for
  [qrank.toolforge.org](https://qrank.toolforge.org/), serving
  the ranking data to the outside.


## Detailed design: Build pipeline

Like all build pipelines, `qrank-builder` reads input, produces
intermediate files, and does some shuffling to finally build its output.
Intermediate files are kept in object storage. At every step of the
pipeline, we check whether the output has already been computed in a
previous run of the pipeline; if so, the step is skipped.

1. The build starts with Wikimedia pageviews. From the
   [Pageview complete](https://dumps.wikimedia.org/other/pageview_complete/readme.html)
   dataset, [pageviews.go](../cmd/qrank-builder/pageviews.go) aggregates
   daily view counts into one file per
   [ISO week](https://en.wikipedia.org/wiki/ISO_week_date), keyed by wiki
   and page ID. The build keeps the 52 most recent weeks; a new week is
   added when its daily dumps are available, and the oldest one is
   dropped. The result gets stored as a sorted, zstd-compressed CSV file.

   💾 For example, the file `pageviews/pageviews-2026-W40.zst` could
   contain a line `en.wikipedia,3422,7`, which means that the page
   [https://en.wikipedia.org/?curid=3422](https://en.wikipedia.org/?curid=3422)
   has been viewed 7 times during that week.

2. The build continues by extracting page signals from the
   [database dumps](https://dumps.wikimedia.org/) of every Wikimedia
   wiki, about a thousand of them. For each wiki,
   [pagesignals.go](../cmd/qrank-builder/pagesignals.go) reads the `page`
   and `page_props` tables, and writes one line per page that is about a
   Wikidata item: the page ID, the item ID, and the page's size in bytes
   of wikitext. For pages of Wikidata itself, the line also has the
   number of claims, identifiers and sitelinks of the item. A wiki's file
   only gets re-built when that wiki has a new dump, so most runs re-use
   most page signals from earlier runs.

   💾 For example, the file `page_signals/rmwiki-20261001-page_signals.zst`
   holds the page signals of the Romansh Wikipedia, built from its dump
   of October 1, 2026.

   In the same way, the build also extracts page titles, redirects,
   interwiki links and the mapping from pages to items for every wiki.
   These are not used for the ranking yet; they are groundwork for
   computing [PageRank](#pagerank).

3. The build continues by joining the weekly pageviews from step 1 with
   the page signals from step 2. Because all inputs to this step are
   sorted by the same key, wiki and page ID, a single linear scan
   suffices. The logic for merging the inputs is in
   [linemerger.go](../cmd/qrank-builder/linemerger.go), using a
   [min heap](https://en.wikipedia.org/wiki/Heap_(data_structure)).
   The intermediate output is a long series of per-page signals keyed by
   item. They get sorted by item ID with an external sort, and read back
   in order. At this time, all signals for the same item appear grouped
   together, so we can sum them in linear time. See
   [itemsignals.go](../cmd/qrank-builder/itemsignals.go).

   💾 The result is published as `item_signals-YYYYMMDD.csv.zst`, with
   the columns `item`, `pageviews_52w`, `wikitext_bytes`, `claims`,
   `identifiers` and `sitelinks`. For example, the row for
   [Q72](https://www.wikidata.org/wiki/Q72) (Zürich) sums the pageviews
   of the articles about Zürich in all languages and Wikimedia projects
   over the past 52 weeks. The date in the file name is the newest date
   of any input, either a dump or the last day of the latest pageviews
   week.

4. The build continues by ranking the items by their pageviews over
   the past 52 weeks, which is the QRank. Items without any pageviews
   are left out. If two items have the same QRank, the item ID is used
   as secondary key. The comparison function is `QRankLess()` in
   [qrank.go](../cmd/qrank-builder/qrank.go).

   💾 The result is stored as a gzip-compressed CSV file
   `qrank-YYYYMMDD.csv.gz` with the columns `Entity` and `QRank`,
   sorted by decreasing QRank.

5. The build finishes by computing some statistics about the output,
   which get stored into a small JSON file. Currently, this is just
   the SHA-256 hash of the `qrank` file; the `qrank-webserver`
   (see below) uses this as an entity tag for conditional HTTP
   requests. In the future, it would  make sense to compute additional
   stats, for example histograms on rank distributions, and store them
   into the same JSON file.

   💾 For example, the file `stats-20210215.json` weighs 133 bytes.


## Detailed design: Webserver

The webserver is a trivial HTTP server. In production, it runs
on the Wikimedia Cloud behind [nginx](https://nginx.org/).

The main serving code is in [main.go](../cmd/qrank-webserver/main.go).
Requests for the home page are currently handled by returning a static string;
requests for a file download get handled from the file system.

Clients can efficiently check for updates to the ranking file
because `qrank-webserver` supports
[conditional requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Conditional_requests). For the `ETag` entity tag, we currently use the SHA-256
hash of the ranking file, computed by the final stage of `qrank-builder`
and stored in the `stats` file.

A background task of `qrank-webserver` periodically checks the local
file system. When the server starts up, and whenever new data is
available, the code in
[dataloader.go](../cmd/qrank-webserver/dataloader.go) loads the latest
`stats` file (but not the large ranking file) into memory.


## Performance

To make use of multi-core machines, `qrank-builder` splits the work
in smaller tasks and distributes them to parallel worker threads.

* When processing **pageviews**, the daily log files get handled
  in parallel.

* When processing **Wikidata dumps**, we split the large input file
  (62 GB as of March 2021, but growing quickly) into a set of chunks
  that get processed in parallel. To split the compressed input, we
  look for the “magic” six-byte sequence that appears at the beginning
  of bzip2 compression blocks. In a well-compressed file, a new block
  should start roughly every hundred kilobytes: At bzip compression
  level 9, the decompression buffer is 900 KB; with 10x compression,
  the compressed block would be about 100 KB long. In practice,
  Wikidata dumps seem to contain much smaller blocks (sometimes just a
  few hundred bytes), which may be one reason why Wikidata dump files
  are so large. Anyhow, once we found a potential block start, we
  start decompressing the block. Typically, compression blocks can
  start anywhere in the middle of a Wikidata entity; this is because
  Wikidata's current bzip2 compressor does not align compression
  blocks with entity boundaries.  We therefore skip the first
  (partial) line in the block, and extract the ID of the entity in the
  *second* line. However, since the “magic” six-byte sequence can also
  appear in the middle of a compression block, our decompression
  attempt may fail with a bzip2 decoding error.  If this happens, we
  will not use the affected block for splitting.  The logic for the
  splitting is in function `SplitWikidataDump()` in
  [entities.go](../cmd/qrank-builder/entities.go). Our splitting logic
  is somewhat similar in spirit to [lbzip2](https://lbzip2.org/), but
  our implementation is simpler because we know the structure of the
  decompressed stream.

Beyond parallelization, we have implemented other peformance
improvements.

* For sorting, we use an
  [external sorting algorithm](https://en.wikipedia.org/wiki/External_sorting).
  This allows `qrank-builder` to process very large amounts of data
  on cheap machines with relatively little main memory.

* Wikidata dumps contain entities in a rather verbose JSON format.
  By implementing a specialized parser, we have reduced the time
  to process one Wikidata entity by roughly 90%, from 228 μs to 21.9 μs.
  The corresponding benchmark is in function `BenchmarkProcessEntity()`
  in [entities_test.go](../cmd/qrank-builder/entities_test.go).

* A large fraction of the pipeline's time is spent in compression
  and decompression. For its intermediate data files, the `qrank-builder`
  pipeline uses [Brötli compression](https://en.wikipedia.org/wiki/Brotli).
  When benchmarking various compression algorithms on the actual payload,
  Brötli gave similar file sizes to bzip2, but at speeds comparable to
  flate/gzip. However, we decided to *not* use Brötli compression for
  the externally exposed `qrank` file. A lesser-known compression format
  would make it harder for downstream clients to consume the rankings.

Compressing intermediate files for performance sounds rather
counter-intuitive.  Essentially, this helps to work around a quirk of
the Wikimedia Cloud.  Although Wikimedia seems to have pretty good hardware,
it still uses the ancient [NFS file
system](https://en.wikipedia.org/wiki/Network_File_System) for
storage which does not scale well to high loads. In order to
protect its NFS servers from overload, the Wikimedia Cloud is
throttling access to a very low 5 MBit/s. When
benchmarking `qrank-builder` on [Digital
Ocean](https://www.digitalocean.com/) whose block storage is [based
on](https://www.digitalocean.com/blog/why-we-chose-ceph-to-build-block-storage/)
the open-source [Ceph](https://ceph.io/) system, we could read and
write files from Ceph-mounted volumes at up to 200 MBit/s, roughly
40 times faster. Although Wikimedia has started to modernize its
storage infrastructure, the data dumps, which are the input to the
QRank build pipeline, can still only be accessed over NFS (or an
HTTP server that seems subject to similar levels of throttling).
Likewise, Toolforge is currently mounting a tool's private data directory
via NFS. When the Wikimedia Cloud finished its migration to more
scalable storage systems, it might be worth trying to remove the
compression of intermediate files in `qrank-builder`, or to reduce
the compression level.

Said this, the build pipeline's current bottleneck seems to be CPU and
not I/O, so the file system may not actually matter all that much.
Still, it is a little surprising that the pipeline can process
the exact same data so much faster on DigitalOcean than in the
Wikimedia Cloud. In any case, it's not really a big problem if the
ranking signal is stale by a couple days.


## Future work

### Signal smearing

Currently, the QRank values are simply aggregated raw view counts; no
“signal smearing” has been implemented yet. This might be an area
for future improvement, since it would give a rank to entities that
have no Wikimedia pages themselves. For example, it may be beneficial
to propagate some fraction of an author's rank to their publications;
likewise from a painter to their works of art.


### PageRank

A related, quite obvious idea would be to run an iterative version of
the [PageRank algorithm](https://en.wikipedia.org/wiki/PageRank) on
Wikidata.  When going this route, it might be beneficial to restrict
the analysis to those parts of the WikiData graph that actually
reflect quality or importance.  For example, running PageRank on the
citation graph of the world's research literature is likely to work
quite well. As a counterexample, boosting the ranking of cities based
on who happened to be born at a place may or may not be worth the
effort, especially when seeding the ranking with pageviews for the
city. Anyhow, as of March 2021, it seems a bit early to really try
this; academic works and their citation graph still have very limited
coverage in Wikidata. As the [Scholia
project](https://www.wikidata.org/wiki/Wikidata:Scholia) is making
progress, it may be beneficial to revisit this at some later time.
There has been [research on Wikipedia
pagerank](https://www.aifb.kit.edu/images/e/e5/Wikipedia_pagerank1.pdf)
that may be relevant if anyone wants to look into this.


### Query API

It would be very useful to offer an Application Programming Interface
to query the rank of Wikidata entities. To integrate into the existing
ecosystem, such as [Wikidata Query Service](https://query.wikidata.org/)
and similar services, we could expose a
[SPARQL endpoint](https://www.w3.org/TR/sparql11-overview/)
for [federated queries](https://www.w3.org/TR/sparql11-federated-query/).

A realistic approach might be writing a Java server that invokes
the [query engine](https://jena.apache.org/documentation/query/index.html)
of [Apache Jena](https://jena.apache.org/) on a custom data store
built from the existing QRank data files. This would best be implemented
as a separate project, but it may make sense to proxy queries from
[qrank.toolforge.org](https://qrank.toolforge.org/) to that other service.
Then, the system complexity would be hidden from external developers.
This variant might be a great project for a Java programmer.

Alternatively, one could also write a SPARQL implementation in Go and
make it part of the existing `qrank-webserver`. From a systems
perspective, this would be easier to maintain (less moving parts) than
a separate Java process, but one would have to re-implement parts of
Apache Jena in the Go programming language.  The key difficulty would
be designing the libary in a lean way, avoiding the complexity that is
typical of Java frameworks.  This seems certainly doable, but it would
be substantial work, ideally done by a very experienced programmer.

Another alternative would be coming up with some ad-hoc query language
instead of SPARQL. That would take less effort to implement, but then
the integration with existing services gets more complicated.
