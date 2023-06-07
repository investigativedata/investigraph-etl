#!/bin/bash

# set -e

if [ ! -f "/data/prefect/initialized" ]; then
    echo "initializing ..."
    echo `date +"%Y-%m-%d %H:%M:%S"` > /data/prefect/initialized
    echo "adding local datasets block ..."
    investigraph add-block -b local-file-system/datasets -u /data/local/datasets
    echo "adding github datasets block ..."
    investigraph add-block -b github/investigraph-datasets -u https://github.com/investigativedata/investigraph-datasets.git
    echo "applying default deployment ..."
    prefect deployment build investigraph/pipeline.py:run -n investigraph-local --apply
fi

# FIXME
# exec sh -c "$@"
$@

