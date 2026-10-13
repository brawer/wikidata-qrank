// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

module github.com/brawer/wikidata-qrank/v2

// The Heroku Go Buildpack on Toolforge installs the Go release named in
// the `go` line, and its list of releases lags behind Go's (heroku/go 2.2.3
// knows up to 1.26.3). If it does not know the release, the build fails.
// The toolchain line makes that Go download and build with a newer, patched
// release; CI's setup-go and govulncheck use it too.
go 1.26.3

toolchain go1.26.9

// Tell the Heroku Go Buildpack (on Wikimedia Toolforge Build Service)
// what binaries we want to have installed into the production container.
//
// https://github.com/heroku/heroku-buildpack-go?tab=readme-ov-file#go-module-specifics
// +heroku install ./cmd/qrank-builder ./cmd/webserver

require (
	github.com/dsnet/compress v0.0.1
	github.com/fogleman/gg v1.3.0
	github.com/klauspost/compress v1.20.3
	github.com/lanrat/extsort v1.5.0
	github.com/minio/minio-go/v7 v7.3.0
	github.com/prometheus/client_golang v1.25.0
	golang.org/x/sync v0.24.0
	golang.org/x/text v0.43.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dustin/go-humanize v1.1.0 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.1 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/tinylib/msgp v1.6.5 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/image v0.47.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	gopkg.in/ini.v1 v1.67.3 // indirect
)
