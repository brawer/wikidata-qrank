<!--
SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
SPDX-License-Identifier: MIT
-->

# QRank Builder

The `qrank-builder` tool is a cronjob that computes `qrank.csv.gz`
and `qrank-stats.json` from log impressions and Wikidata; see the
[design document](../../doc/design.md) for details.


## Release instructions

We should set up an automatic release process, but are blocked on
[T194332](https://phabricator.wikimedia.org/T194332). Meanwhile,
here’s how to manually push a new version of the binary to NFS.
In Wikimedia’s Kubernetes setup, cronjobs are supposed to be run
from NFS, ouch.

```bash
$ GOOS=linux go build ./cmd/qrank-builder && scp ./qrank-builder sascha@bastion.toolforge.org:/data/project/qrank/bin/qrank-builder
```
