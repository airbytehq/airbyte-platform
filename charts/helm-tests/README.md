# airbyte-helm

Utilities for testing and generating Helm charts.

### Running Tests

The tests in this repository are meant to be run against the Airbyte Helm Chart (OSS). 

```
go test -timeout=0s -v -count=1 ./tests
```

The `-count=1` is important to avoid Go's test caching, which doesn't work with our external helm files.

If you're using VSCode, you might want to add the following to settings.json:
```
    "go.testTimeout": "0s",
    "go.testFlags": [
        "-count=1"
    ]
```

The `./tests` directory contains tests that render the chart and verify the output.
The `./integration_tests` directory contains tests that run a k8s cluster and actually install the chart in the cluster.