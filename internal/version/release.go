// SPDX-FileCopyrightText: 2026 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package version

// Release is the released version a build was cut from. Once release-please
// is set up, it will set this in each release commit; it is "" on any build
// before the first release, in which case Resolve falls back to the source
// revision.
const Release = "" // x-release-please-version
