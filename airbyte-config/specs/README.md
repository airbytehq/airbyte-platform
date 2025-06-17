# Downloading Local Connector Registry

This module contains a version of the connector registry, which is baked into the JAR. 
The registry is shipped with Airbyte to account for air-gapped airbyte deployments. 
This also allows Airbyte to start with pre-populated connectors. 

Run ./gradlew :downloadConnectorRegistry to manually update this file.