# airbyte-helm

Utilities for testing and generating Helm charts.

### Running Tests

The tests in this repository are meant to be run against the Airbyte Helm Chart (OSS). 

```
export HELM_CHART_PATH=<path_to_helm_chart; e.g. $HOME/developer/github/airbytehq/airbyte-platform-internal/oss/charts/airbyte>
go test ./tests -tags=template
```

There are a few different tags for the test:
* `template` - these are template tests which render the Helm templates and verify the yaml that will be submitted to Kubernetes
* `install` - these will spin up a local K8s clutser (Kind) and test installing the chart
* `storage_config` - these tests verify the various options for storage configuration (i.e. logs, state)
