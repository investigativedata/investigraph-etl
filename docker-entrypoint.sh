#!/bin/bash

# set -e

if [ ! -f "/data/prefect/initialized" ]; then
    touch /data/prefect/initialized
    prefect deployment build investigraph/pipeline.py:run -n investigraph-local --apply
fi

# FIXME
# exec sh -c "$@"
$@

