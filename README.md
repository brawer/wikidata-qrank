# Wikidata QRank

[![CI](https://github.com/brawer/wikidata-qrank/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/brawer/wikidata-qrank/actions/workflows/go.yml)
[![Data License: CC0-1.0](https://img.shields.io/badge/Data%20License-CC0%201.0-lightgrey.svg)](http://creativecommons.org/publicdomain/zero/1.0/)
[![Code License: MIT](https://img.shields.io/badge/Code%20License-MIT-lightgrey.svg)](https://opensource.org/licenses/MIT)

QRank is a ranking signal for [Wikidata](https://www.wikidata.org/) entities.
It gets computed by aggregating page view statistics for Wikipedia, Wikitravel,
Wikibooks, Wikispecies and other Wikimedia projects.
For example, according to the QRank signal, the fictional character
[Pippi Longstocking](https://www.wikidata.org/wiki/Q6668)
ranks lower than [Harry Potter](https://www.wikidata.org/wiki/Q8337),
but still much higher than the relatively obscure
[Äffle & Pferdle](https://www.wikidata.org/wiki/Q252869).


| Entity                                                   | Label              |    QRank |
| -------------------------------------------------------: | :----------------- | --------:|
| [Q8337](https://www.wikidata.org/wiki/Q8337)             | Harry Potter       | 17602336 |
| [Q6668](https://www.wikidata.org/wiki/Q6668)             | Pippi Longstocking |  2470590 |
| [Q252869](https://www.wikidata.org/wiki/Q252869)         | Äffle & Pferdle    |    24545 |


Such a ranking signal is useful when time or space are too limited
to handle everything. For example:

* When **fixing data problems**, start with the highest ranking entities
  for maximal impact of your work.

* When **drawing geographic maps**, show higher-ranking features
  more prominently. For example,
  [this map of Swiss castles and ruins](https://castle-map.infs.ch/#46.82825,8.19305,8z) uses QRank to decide which castles deserve a large icon and which
  ones just a small dot.


## License

*Data:* Just like Wikidata, the QRank data files are dedicated to the Public domain
via [CC0-1.0](https://creativecommons.org/publicdomain/zero/1.0/).
To the extent possible under law, we have waived all copyright and related
or neighboring rights to this work. This work is published from Switzerland.

*Code:* The source code of the program for computing the QRank signal
is released under the [MIT license](https://github.com/brawer/wikidata-qrank/blob/main/LICENSE).
