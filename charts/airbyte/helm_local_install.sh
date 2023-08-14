#!/bin/bash

# enable command tracing with blue text
blue_text='\033[94m'
default_text='\033[39m'
PS4="$blue_text"'${BASH_SOURCE}:${LINENO}: '"$default_text"
set -o xtrace

# Backup original Chart.yaml
mv Chart.yaml Chart.yaml.prod

# Replace with dev Chart.yaml
mv Chart.yaml.local Chart.yaml

# Create a local helm installation called 'local' using the local charts.
# Additional arguments passed in to the script are appended to the end of the `helm install`
# command so that additional flags can be passed, like --set <value> or --values <file>
helm dep update && helm install local . "$@"

# Replace original Chart.yaml
mv Chart.yaml Chart.yaml.local
mv Chart.yaml.prod Chart.yaml

# turn off command tracing as cleanup
set +o xtrace