# Wikidata QRank: Design

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

Initially, it will explicitly *not* be our goal to offer a website where
people can interactively browse the ranked data, even though this would
be very cute. Likewise, it will initially *not* be our goal to offer an API
for external software to quickly query the rank of individual Wikidata entities.
Again, this would be useful, and we may well add it some later time.
For the time being, however, we will focus on exposing the ranking data in bulk
as a downloadable file.


## Overview

The QRank system consists of two parts. Both parts are running on the
Wikimedia Cloud infrastructure within the
[Toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge)
environment.

* `qrank-builder` is an automated pipeline that computes the ranking.
* `qrank-webserver` is an small webserver that exposes the ranking file
  to the outside.


## Detailed design: Build pipeline

Like all build pipelines, `qrank-builder` reads input, produces
intermediate files, and does some shuffling to finally build its output.

1. The build currently starts with Wikimedia pageviews. From the
[Pageview complete](https://dumps.wikimedia.org/other/pageview_complete/readme.html) dataset, [pageviews.go](../cmd/qrank-builder/pageviews.go)
aggregates monthly view counts. The result gets stored
as a sorted and compressed text file. For example, the file
`pageviews-2021-02.br` contains the line `en.wikipedia/seabird 8204`,
which means that the English Wikipedia article on [Seabird](https://en.wikipedia.org/wiki/Seabird) has been viewed 8204 times in February 2021. In total,
the monthly file for February 2021 contains 118.2 million such entries.
After compression, it needs 8.9 MB in storage.

2. The build continues by processing the latest
[Wikidata database dump](https://www.wikidata.org/wiki/Wikidata:Database_download). 

3. (TODO: Describe the other steps of the build pipeline.)

Currently, we have not implemented any signal smearing: The rankings
are just the aggregated viewcounts. This may well be refined over
time.  For example, it probably would make sense to propagate some
fraction of an author's fame to their publications, or some fraction
of a painter's fame to their works. Another, rather obvious idea would
be to run a PageRank-like algorithm on the citation graph; but as of
2021, it seems too early to do this; the modeling of research
literature in Wikidata is still very incomplete.


## Performance

To make use of multi-core machines, `qrank-builder` splits the work
in smaller tasks and distributes them to parallel worker threads.

* When processing **pageviews**, the daily log files get handled
  in parallel.

* When processing **Wikidata dumps**, we split the large input file
  (62 MB as of March 2021, but growing quickly) into a set of chunks
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

* Wikidata dumps contain entities in a rather verbose JSON format.
  By implementing a specialized parser, we have reduced the time
  to execute `ProcessEntity()` by roughly 90%, from 228 μs to 21.9 μs.
  The corresponding benchmark is in function `BenchmarkProcessEntity()`
  in [test_entities.go](../cmd/qrank-builder/test_entities.go). However,
  because bzip2 is such an expensive format to decode, the bzip2 splitting
  (see above) had a bigger impact on the overall runtime than this
  micro-optimization.

* For our intermediate data files, which are internal to the QRank system
  and not exposed to the public, we use [Brötli compression](https://en.wikipedia.org/wiki/Brotli). When we were benchmarking various compression algorithms
  on the internal QRank files, Brötli gave file sizes that were similar to
  or smaller than bzip2, but at speed comparable to flate/gzip.


## Detailed design: Webserver

The webserver is a trivial HTTP server. In production, it runs
on the Wikimedia Cloud behind [nginx](https://nginx.org/).

A background task periodically checks the local file system.
When new data is available, the code in [dataloader.go](../cmd/qrank-webserver/dataloader.go) loads the file hash (but not the file) into memory.

The main serving code is in [main.go](../cmd/qrank-webserver/main.go).
Requests for the home page are currently handled by returning a static string;
requests for a file download get handled from the file system.
The file hash serves as entity tag in [Conditional HTTP requests](https://tools.ietf.org/html/rfc7232).
