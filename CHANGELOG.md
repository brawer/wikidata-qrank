# Changelog

## [2.2.0](https://github.com/brawer/wikidata-qrank/compare/v2.1.2...v2.2.0) (2026-10-13)


### 🆕 Features

* **builder:** publish a Frictionless data package with a CycloneDX BOM ([b93ad83](https://github.com/brawer/wikidata-qrank/commit/b93ad83a1fb6e945c209be2f8a1f251e29474789))
* **builder:** re-build intermediate files that lack a software stamp ([4ef0996](https://github.com/brawer/wikidata-qrank/commit/4ef09961d5b80bd296c7a84d534ab5345e77646b))
* **builder:** record the producing version of every stored file ([7a0cfcc](https://github.com/brawer/wikidata-qrank/commit/7a0cfcc265882fddaa99d3bd09ccc5770dc858c8))
* **webserver:** serve dated download URLs and datapackage.json ([d42a8c4](https://github.com/brawer/wikidata-qrank/commit/d42a8c46199e9c78ec609f89bc1b737f8e72d4df))

## [2.1.2](https://github.com/brawer/wikidata-qrank/compare/v2.1.1...v2.1.2) (2026-10-13)


### 🐞 Fixes

* **builder:** fall back to older namespace file if latest is broken ([2c6af59](https://github.com/brawer/wikidata-qrank/commit/2c6af59ebaceff0b67c96b80b16ab54cab4cdc61)), closes [#90](https://github.com/brawer/wikidata-qrank/issues/90)
* **builder:** log why a namespace file is malformed ([634c744](https://github.com/brawer/wikidata-qrank/commit/634c74473f8171d9c770dd4211a600f84c8a80e5)), closes [#90](https://github.com/brawer/wikidata-qrank/issues/90)
* let the Toolforge build find a Go it knows ([f17f435](https://github.com/brawer/wikidata-qrank/commit/f17f435d1b2b0f07b8c99b5c363a87af1b943136))

## [2.1.1](https://github.com/brawer/wikidata-qrank/compare/v2.1.0...v2.1.1) (2026-10-12)


### 🐞 Fixes

* **builder:** remove the nameless storage key flag ([0ea528e](https://github.com/brawer/wikidata-qrank/commit/0ea528eda94a9359f83b929a13a5462fe3839f3a)), closes [#84](https://github.com/brawer/wikidata-qrank/issues/84)

## [2.1.0](https://github.com/brawer/wikidata-qrank/compare/v2.0.1...v2.1.0) (2026-10-12)


### 🆕 Features

* **builder:** publish QRank again, computed from item signals ([045efb9](https://github.com/brawer/wikidata-qrank/commit/045efb9acdffaab282ccad852b4addd9a04fac75))


### 🐞 Fixes

* **builder:** send the real version in the storage User-Agent ([2baee31](https://github.com/brawer/wikidata-qrank/commit/2baee319949151b1ca1038cf1c9219cb33062e05))
* **deps:** update Go dependencies ([df8985a](https://github.com/brawer/wikidata-qrank/commit/df8985a615f7d949ea13c14ab4b4e2f4fec61f09))

## [2.0.1](https://github.com/brawer/wikidata-qrank/compare/v2.0.0...v2.0.1) (2026-10-12)


### 🐞 Fixes

* **builder:** prune old redirects, pageviews and item_signals from storage ([fa92b1a](https://github.com/brawer/wikidata-qrank/commit/fa92b1afc21d21dbe6a024526f475fa2d4d33c08))

## 2.0.0 (2026-10-12)


### 🆕 Features

* report the build version in the Server header and builder log ([2182b7c](https://github.com/brawer/wikidata-qrank/commit/2182b7cca61f4d4a99b7017a56b2b5e65d80211d))


### 🐞 Fixes

* **release:** include the version-reporting feature in the first release ([711a800](https://github.com/brawer/wikidata-qrank/commit/711a800723ae4ffed6ac376c939457398157c17c))
