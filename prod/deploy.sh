# SPDX-FileCopyrightText: 2024 Sascha Brawer <sascha@brawer.ch>
# SPDX-License-Identifier: MIT
#
# To deploy fresh binaries, ssh to bastion.toolforge.org, become qrank,
# and run this script.
#
# TODO: We should completely automate our release process. Unfortunately,
# Wikimedia Toolforge does not support this yet.
#
# https://phabricator.wikimedia.org/T194332
# https://wikitech.wikimedia.org/wiki/Wikimedia_Cloud_Services_team/EnhancementProposals/Toolforge_push_to_deploy

toolforge build start https://github.com/brawer/wikidata-qrank

toolforge jobs delete qrank-builder

toolforge \
    jobs run \
    --schedule "@daily" \
    --command qrank-builder \
    --image tool-qrank/tool-qrank:latest \
    --mount all \
    --cpu 3 \
    --mem 3Gi \
    qrank-builder
