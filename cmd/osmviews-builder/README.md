<!--
SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
SPDX-License-Identifier: MIT
-->

# OSMViews Builder

The `osmviews-builder` tool is a cronjob that computes `osmviews.tiff`
and `osmviews-stats.json` from OpenStreetMap tile log impressions.


## Release instructions

We should set up an automatic release process, but are blocked on
[T194332](https://phabricator.wikimedia.org/T194332). Meanwhile,
here’s how to manually push a new version of the binary to production.
(Wikimedia wants to run production binaries from NFS, ouch).

```bash
$ GOOS=linux go build ./cmd/osmviews-builder && scp ./osmviews-builder sascha@bastion.toolforge.org:/data/project/qrank/bin/osmviews-builder
```
