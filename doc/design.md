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
At every step of the pipeline, we check whether the output has already
been computed in a previous run of the pipeline; if so, the step is skipped.

1. The build currently starts with Wikimedia pageviews. From the
   [Pageview
   complete](https://dumps.wikimedia.org/other/pageview_complete/readme.html)
   dataset, [pageviews.go](../cmd/qrank-builder/pageviews.go) aggregates
   monthly view counts for the past twelve months.  The result gets
   stored as a sorted and compressed text file.

   💾 For example, the file `pageviews-2021-02.br` contains a line
`en.wikipedia/seabird 8204`, which means that the English Wikipedia
article for [Seabird](https://en.wikipedia.org/wiki/Seabird) has been
viewed 8204 times in February 2021. In total, the monthly file for
February 2021 contains 118.2 million such lines.  After compression,
it weighs 8.9 MB in storage.

2. The build continues by extracting Wikimedia site links from latest
   [Wikidata database dump](https://www.wikidata.org/wiki/Wikidata:Database_download),
   and associating them with the corresponding Wikidata entity ID. Again,
   the result gets stored as a sorted and compressed text file.

   💾 For example, the file `sitelinks-2021-02-15.br` contains a line
`en.wikipedia/seabird Q55808`, which means that this page of the English
Wikipedia is about entity [Q55808](https://www.wikidata.org/wiki/Q55808).
In total, the sitelinks file extracted from the Wikidata dump of February
15, 2021 contains 76.7 million such lines (because lots of Wikidata entities
have no sitelinks at all), weighing 783.5 MB after compression.

3. The build continues by joining `pageviews` for the previous twelve
   months, which were computed in step 1, with `sitelinks` from step 2.
   Because all inputs to this step use the same key, and because the
   input files are sorted by that key, we can do a simple
   linear scan without reshuffling. The logic for merging the input
   input files is in [linemerger.go](../cmd/qrank-builder/linemerger.go);
   it uses a [priority heap queue](https://en.wikipedia.org/wiki/Priority_queue)
   internally. The intermediate output is a long series of (entity, count)
   pairs. They get sorted by entity ID into a temporary file, and read back
   in order. At this time, all view counts for the same entity will appear
   grouped together, so we can easily (in linear time) compute the sum.

   💾 For example, the file `qviews-2021-02-15.br` contains a line
   `Q55808 329861`. This means that from February 2020 to January 2021,
   Wikimedia pages about [Q55808](https://www.wikidata.org/wiki/Q55808)
   (Seabird) have been viewed 329861 times, aggregated over all languages
   and Wikimedia projects for the entire year. In total, the file contains
   27.3 million such lines, weighing 103.9 MB after compression.

4. The build continues by sorting the view counts by decreasing popularity.
   If the pages about two entities were viewed equally often,
   the entity ID is used as secondary key. The comparison function is
   `QRankLess()` in [qrank.go](../cmd/qrank-builder/qrank.go).

   💾 The file format, content and approximate size of `qrank-2021-02-15.br`
   is the same as the `qviews` file of the previous step, only the sorting
   is different.

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
  to execute `ProcessEntity()` by roughly 90%, from 228 μs to 21.9 μs.
  The corresponding benchmark is in function `BenchmarkProcessEntity()`
  in [test_entities.go](../cmd/qrank-builder/test_entities.go). However,
  because bzip2 is such an expensive format to decode, the bzip2 splitting
  (see above) had a bigger impact on the overall runtime than this
  micro-optimization.

* A large fraction of the pipeline's time is spent in compression
  and decompression. For its intermediate data files, the `qrank-builder`
  pipeline uses [Brötli compression](https://en.wikipedia.org/wiki/Brotli).
  When benchmarking various compression algorithms on the actual payload,
  Brötli gave similar file sizes to bzip2, but at speeds comparable to
  flate/gzip. However, we decided to *not* use Brötli compression for
  the externally exposed `qrank` file. A lesser-known compression format
  would make it harder for downstream clients to consume the rankings.


## Future work

### Signal smearing

Currently, the QRank values are simply aggregated raw view counts; no
“signal smearing” has been implemented yet. This might be be an area
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
this; academic works and its citation graph have still very limited
coverage in Wikidata. As the [Scholia
project](https://www.wikidata.org/wiki/Wikidata:Scholia) is making
progress, it may be beneficial to revisit this at some later time.
There has been [research on Wikipedia
pagerank](https://www.aifb.kit.edu/images/e/e5/Wikipedia_pagerank1.pdf)
that may be relevant if anyone wants to look into this.
