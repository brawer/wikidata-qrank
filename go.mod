// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

module github.com/brawer/wikidata-qrank/v2

// TODO: Update golang version. However, Wikimedia Toolforge Build Service
// is still on golang 1.21.5; https://phabricator.wikimedia.org/T363854
go 1.23.0

toolchain go1.24.1

// Tell the Heroku Go Buildpack (on Wikimedia Toolforge Build Service)
// what binaries we want to have installed into the production container.
//
// https://github.com/heroku/heroku-buildpack-go?tab=readme-ov-file#go-module-specifics
// +heroku install ./cmd/qrank-builder ./cmd/webserver

require (
	github.com/andybalholm/brotli v1.1.1
	github.com/dsnet/compress v0.0.1
	github.com/fogleman/gg v1.3.0
	github.com/klauspost/compress v1.17.11
	github.com/lanrat/extsort v1.0.2
	github.com/minio/minio-go/v7 v7.0.86
	github.com/orcaman/writerseeker v0.0.0-20200621085525-1d3f536ff85e
	github.com/prometheus/client_golang v1.20.5
	github.com/ulikunitz/xz v0.5.12
	golang.org/x/sync v0.12.0
	golang.org/x/text v0.23.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/minio/crc64nvme v1.0.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rs/xid v1.6.0 // indirect
	golang.org/x/crypto v0.36.0 // indirect
	golang.org/x/image v0.24.0 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)
