# QRank

[![CI](https://github.com/brawer/toolforge-qrank/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/brawer/toolforge-qrank/actions/workflows/go.yml)
[![Data License: CC0-1.0](https://img.shields.io/badge/Data%20License-CC0%201.0-lightgrey.svg)](http://creativecommons.org/publicdomain/zero/1.0/)
[![Code License: MIT](https://img.shields.io/badge/Code%20License-MIT-lightgrey.svg)](https://opensource.org/licenses/MIT)

QRank is a ranking signal for Wikidata entities. It gets computed
by aggregating page view statistics for Wikipedia, Wikitravel,
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


Of course, QRank is not just for fictional characters: Use it to rank
everything from animals, brands, cities, libraries, museums, ... to
zoos, or anything else with a Wikimedia page. To avoid seasonal
effects, QRank is calculated over a one-year sliding window.


## License

*Data:* Just like Wikidata, the QRank data files are dedicated to the Public domain
via [CC0-1.0](https://creativecommons.org/publicdomain/zero/1.0/).
To the extent possible under law, we have waived all copyright and related
or neighboring rights to this work. This work is published from Switzerland.

*Code:* The source code of the program for computing the QRank signal
is released under the [LICENSE](MIT license).
