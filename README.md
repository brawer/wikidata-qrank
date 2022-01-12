<!--
SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
SPDX-License-Identifier: MIT
-->

# Wikidata QRank

[![CI](https://github.com/brawer/wikidata-qrank/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/brawer/wikidata-qrank/actions/workflows/go.yml)
[![Data License: CC0-1.0](https://img.shields.io/badge/Data%20License-CC0%201.0-lightgrey.svg)](http://creativecommons.org/publicdomain/zero/1.0/)
[![Code License: MIT](https://img.shields.io/badge/Code%20License-MIT-lightgrey.svg)](https://opensource.org/licenses/MIT)
[![REUSE status](https://api.reuse.software/badge/github.com/brawer/wikidata-qrank)](https://api.reuse.software/info/github.com/brawer/wikidata-qrank)

QRank is a ranking signal for [Wikidata](https://www.wikidata.org/) entities.
It gets computed by aggregating page view statistics for Wikipedia, Wikitravel,
Wikibooks, Wikispecies and other Wikimedia projects.
For example, according to the QRank signal, the fictional character
[Pippi Longstocking](https://www.wikidata.org/wiki/Q6668)
ranks lower than [Harry Potter](https://www.wikidata.org/wiki/Q8337),
but still much higher than the obscure
[Äffle & Pferdle](https://www.wikidata.org/wiki/Q252869).


| Entity                                                   | Label              |    QRank |
| -------------------------------------------------------: | :----------------- | --------:|
| [Q8337](https://www.wikidata.org/wiki/Q8337)             | Harry Potter       | 17602336 |
| [Q6668](https://www.wikidata.org/wiki/Q6668)             | Pippi Longstocking |  2470590 |
| [Q252869](https://www.wikidata.org/wiki/Q252869)         | Äffle & Pferdle    |    24545 |


A ranking signal is useful when time or space are too limited
to handle everything. When **fixing data problems**, use QRank
for maximal impact of your work. In **cartography**,
use QRank to display important features more prominently; [this map of Swiss castles](https://castle-map.infs.ch/#46.82825,8.19305,8z) uses QRank to decide which castles deserve a large symbol.

For a **technical description** of the system, see the
[Design Document](doc/design.md). To **download ranking data**,
head over to [qrank.toolforge.org](https://qrank.toolforge.org/).


## License

*Data:* Like Wikidata, the [QRank data](https://qrank.toolforge.org/)
is dedicated to the Public domain
via [CC0-1.0](https://creativecommons.org/publicdomain/zero/1.0/).
To the extent possible under law, we have waived all copyright and related
or neighboring rights to this work. This work is published from Switzerland,
using infrastructure of the Wikimedia Foundation in the United States.

*Code:* The source code of the program for computing the QRank signal
is released under the [MIT license](https://github.com/brawer/wikidata-qrank/blob/main/LICENSE).
