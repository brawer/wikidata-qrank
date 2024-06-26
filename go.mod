// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

module github.com/brawer/wikidata-qrank/v2

// TODO: Update golang version. However, Wikimedia Toolforge Build Service
// is still on golang 1.21.5; https://phabricator.wikimedia.org/T363854
go 1.21

// Tell the Heroku Go Buildpack (on Wikimedia Toolforge Build Service)
// what binaries we want to have installed into the production container.
//
// https://github.com/heroku/heroku-buildpack-go?tab=readme-ov-file#go-module-specifics
// +heroku install ./cmd/qrank-builder ./cmd/webserver

require (
	github.com/andybalholm/brotli v1.1.0
	github.com/dsnet/compress v0.0.1
	github.com/fogleman/gg v1.3.0
	github.com/klauspost/compress v1.17.7
	github.com/lanrat/extsort v1.0.0
	github.com/minio/minio-go/v7 v7.0.69
	github.com/orcaman/writerseeker v0.0.0-20200621085525-1d3f536ff85e
	github.com/prometheus/client_golang v1.19.0
	github.com/ulikunitz/xz v0.5.11
	golang.org/x/sync v0.7.0
	golang.org/x/text v0.16.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.2.7 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/prometheus/client_model v0.6.0 // indirect
	github.com/prometheus/common v0.50.0 // indirect
	github.com/prometheus/procfs v0.13.0 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/stretchr/testify v1.7.0 // indirect
	golang.org/x/crypto v0.21.0 // indirect
	golang.org/x/image v0.18.0 // indirect
	golang.org/x/net v0.23.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
)
