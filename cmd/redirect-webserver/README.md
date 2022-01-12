<!--
SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
SPDX-License-Identifier: MIT
-->

# Wikidata QRank: Redirect Webserver

Before January 2022, the QRank project had been running on
[Toolforge](https://toolforge.org/), but then it moved to the
[Wikimedia Cloud](https://wmcloud.org/).  This webserver runs on
the old Toolforge infrastructure at
[https://qrank.toolforge.org/](qrank.toolforge.org) and redirects any
incoming requests to the new location.

## Release instructions

We currently don’t have a real release process for this mini-server.
If ever needed, here’s how to manually push a new version of the binary.

```bash
$ GOOS=linux go build ./cmd/redirect-webserver && scp ./redirect-webserver sascha@bastion.toolforge.org:/data/project/qrank/bin/redirect-webserver
$ ssh sascha@bastion.toolforge.org
sascha@tools-sgebastion-07:~$ become qrank
tools.qrank@tools-sgebastion-07:~$ webservice --backend=gridengine generic restart /data/project/qrank/bin/redirect-webserver 
```
