# Downloading Local Connector Registry

The local connector registry is downloaded from the metadata service using the `downloadConnectorRegistry` task defined in this module.

This task is automatically run when Airbyte is built. The downloaded specs are shipped with Airbyte to account for air-gapped airbyte
deployments. This also allows Airbyte to start with pre-populated connectors. After initialization, we expect most deployments to query the remote
metadata service for the most up-to-date connector versions.
