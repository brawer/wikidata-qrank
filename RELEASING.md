<!--
SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
SPDX-License-Identifier: MIT
-->

# Releasing

Releases are automated with
[release-please](https://github.com/googleapis/release-please). Merging a
`feat:`/`fix:`/`perf:` pull request updates an open release PR. Merging that
PR cuts a `vX.Y.Z` tag and a GitHub release, and the tag push redeploys the
webserver and `qrank-builder` to Toolforge. Nothing is published to a package
registry.

## How it works

1. Every pull request has a
   [Conventional Commits](https://www.conventionalcommits.org) title (`feat:`,
   `fix:`, `perf:`, `docs:`, `refactor:`, `test:`, `build:`, `ci:`, `chore:`).
   PRs are squash-merged, so the title becomes the commit on `main`. The
   `pr-title` check enforces this.
2. [`.github/workflows/release-please.yml`](.github/workflows/release-please.yml)
   watches `main` and keeps a single open **"chore(main): release X.Y.Z"** pull
   request. It updates `CHANGELOG.md` from the commit history, sets the version
   in [`internal/version/release.go`](internal/version/release.go), and updates
   `.release-please-manifest.json`. It runs as the **`brawer-release-bot`**
   GitHub App (see [One-time setup](#one-time-setup)). A workflow run started
   by the built-in `GITHUB_TOKEN` cannot itself start further runs, so the App
   identity is what lets the tag push trigger the deploy.
3. Review that PR and squash-merge it when you want to cut the release.
   release-please then pushes the `vX.Y.Z` tag and creates the GitHub release.

## Choosing the version number

The Go module path is `github.com/brawer/wikidata-qrank/v2`, so releases are
`v2.y.z`. The first release is forced to `2.0.0` by `"release-as"` in
[`release-please-config.json`](release-please-config.json); **remove that line
after the first release**, or every later release PR will also say 2.0.0.
History before `bootstrap-sha` (the commit that introduced version reporting)
is not scanned.

After that, release-please picks the bump from the commit types since the last
release: `fix:`/`perf:` → patch, `feat:` → minor, and a `!` after the type or
a `BREAKING CHANGE:` footer → major.

Only `feat:`, `fix:` and `perf:` commits cut a release and appear in
`CHANGELOG.md`. `docs:`, `refactor:`, `test:`, `build:`, `ci:` and `chore:`
are silent; they ride along with the next real release. To ship a silent
change on its own, or to force a specific number, put `Release-As: X.Y.Z` in
a commit body.

## What the tag does

Pushing the `vX.Y.Z` tag triggers CI
([`.github/workflows/go.yml`](.github/workflows/go.yml)). After the build and
tests pass, the `deploy` job calls the Toolforge components API, which
rebuilds the image from `main` with the Toolforge Build Service and recreates
both components, the `webserver` and the daily `qrank-builder` job (see
[`toolforge.yaml`](toolforge.yaml)). Watch it at
<https://github.com/brawer/wikidata-qrank/actions>.

**Do not merge anything to `main` until the deploy job finishes.** The
deployment builds `main` at `HEAD` (`toolforge.yaml` pins `ref: main`), so
`main` must still point at the tagged commit when the build runs.

## Verify

```sh
# Server header carries the deployed version:
curl -sI https://qrank.toolforge.org/ | grep -i '^server:'
# → QRank/vX.Y.Z+<commit>

# The webserver logs its version and Go toolchain on startup:
ssh login.toolforge.org
become qrank
kubectl logs deployment/webserver | grep 'built with'
# → QRank/vX.Y.Z+<commit>, built with go1.N, listening …
```

`qrank-builder` logs `starting QRank/vX.Y.Z+<commit>, built with go1.N` at
startup, both to its job log and to `logs/qrank-builder.log` in the tool's
data directory. Both binaries also print their version with `-version`.

The version comes from `internal/version/release.go` (set by release-please,
compiled in by the buildpack); the `+<commit>` suffix is the exact source
revision, which `go build` embeds. Builds that aren't a release, and every
build before the first release, report `QRank/git-<commit>` instead.

## If the automatic deploy fails

Deploy from the bastion by hand:

```sh
ssh login.toolforge.org
become qrank
toolforge components deployment create --description "release vX.Y.Z"
```

`toolforge components config show` prints the currently registered config;
`toolforge components config create toolforge.yaml` re-registers it from a
local copy of the file if it has drifted.

## Changing the deployment shape

Component resources, the schedule, the health check, and so on live in
[`toolforge.yaml`](toolforge.yaml). Because it declares
`source_url: …/main/toolforge.yaml`, the components API re-fetches it from
GitHub on every deployment, so a merged change to that file takes effect on
the next release with no bastion step.

## One-time setup

- The `main` ruleset requires the `build` and `pr-title` checks and
  squash-only merges.
- **`brawer-release-bot` GitHub App**, the same one as for brawer/osmviews:
  1. On the App's page, **Install App** and add `brawer/wikidata-qrank`. The
     App needs `Contents: Read and write` and `Pull requests: Read and write`.
  2. In this repo, **Settings → Secrets and variables → Actions**: add a
     **variable** `RELEASE_PLEASE_APP_CLIENT_ID` (the App's Client ID) and a
     **secret** `RELEASE_PLEASE_APP_PRIVATE_KEY` (a private key `.pem`
     generated on the App page). Until the variable exists, the
     `release-please` workflow is skipped.
- **Toolforge deploy token**: create a components API deploy token for the
  `qrank` tool on the bastion and store it as the `TOOLS_DEPLOY_TOKEN`
  repository secret.
- **Switch Toolforge over to components**, once, on the bastion:

  ```sh
  ssh login.toolforge.org
  become qrank
  toolforge components config create toolforge.yaml  # from a checkout of main
  toolforge webservice stop
  toolforge jobs delete qrank-builder
  toolforge components deployment create --description "switch to components"
  ```
