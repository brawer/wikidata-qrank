// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

module github.com/brawer/wikidata-qrank/v2

// TODO: Update golang version. However, Wikimedia Toolforge Build Service
// is still on golang 1.21.5; https://phabricator.wikimedia.org/T363854
go 1.26.9

// Tell the Heroku Go Buildpack (on Wikimedia Toolforge Build Service)
// what binaries we want to have installed into the production container.
//
// https://github.com/heroku/heroku-buildpack-go?tab=readme-ov-file#go-module-specifics
// +heroku install ./cmd/qrank-builder ./cmd/webserver

require (
	github.com/andybalholm/brotli v1.2.6
	github.com/dsnet/compress v0.0.1
	github.com/fogleman/gg v1.3.0
	github.com/klauspost/compress v1.20.1
	github.com/lanrat/extsort v1.5.0
	github.com/minio/minio-go/v7 v7.3.0
	github.com/prometheus/client_golang v1.25.0
	golang.org/x/sync v0.23.0
	golang.org/x/text v0.42.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/tinylib/msgp v1.6.4 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/image v0.41.0 // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	gopkg.in/ini.v1 v1.67.3 // indirect
)
