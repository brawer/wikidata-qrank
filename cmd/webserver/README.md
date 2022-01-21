<!--
SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
SPDX-License-Identifier: MIT
-->

# Webserver

The webserver handles requests for [qrank.wmcloud.org](https://qrank.wmcloud.org/). It runs on the Wikimedia Cloud VPS infrastructure behind a reverse
HTTP proxy.


## Release instructions

We should set up an automatic release process, but are blocked on
[T194332](https://phabricator.wikimedia.org/T194332). Meanwhile,
here’s how to manually push a new version of the binary to the server.

```bash
GOOS=linux go build ./cmd/webserver
scp -J sascha@bastion.wmcloud.org ./webserver sascha@172.16.3.68:bin/webserver
```

On the server, we have the following configuration file
in `/etc/systemd/system/qrank-webserver.service`:

```
[Unit]
Description=QRank Webserver
After=network.target

[Service]
Type=simple
Restart=always
User=sascha
WorkingDirectory=/home/sascha
ExecStart=/home/sascha/bin/webserver \
  --port=8080 \
  --storage-key=keys/storage-key

[Install]
WantedBy=multi-user.target
```

After logging into the server via ssh, control it like this:

```bash
sudo systemctl status qrank-webserver.service
sudo systemctl daemon-reload qrank-webserver.service
sudo systemctl stop qrank-webserver.service
sudo systemctl start qrank-webserver.service
```
