<!--
SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
SPDX-License-Identifier: MIT
-->

# Webserver

The webserver handles requests for [qrank.toolforge.org](https://qrank.toolforge.org/). It runs on the Wikimedia Toolforge infrastructure.


## Release instructions

We should set up an automatic release process, but are blocked on
[T194332](https://phabricator.wikimedia.org/T194332). Meanwhile,
here’s how to manually push a new version of the binary to the server.

```bash
GOOS=linux go build ./cmd/webserver
scp webserver sascha@bastion.toolforge.org:/data/project/qrank/bin/webserver
```

To restart the webserver:

```bash
ssh bastion.toolforge.org
become qrank
toolforge webservice bookworm start /data/project/qrank/bin/webserver
```
