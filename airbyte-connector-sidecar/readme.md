# airbyte-connector-sidecar

The Airbyte Connector Sidecar is a Kotlin application that runs alongside connector containers to monitor, process, and handle communication between connectors and the Airbyte platform. It acts as an intermediary that watches connector processes, processes their output messages, and reports results back to the workload API.

## Overview

The sidecar pattern allows Airbyte to:
- Act as a bridge between the connector and the platform
- Process and validate connector messages (spec, check, discover operations)
- Handle connector failures and timeouts gracefully  
- Report connector status and results to the control plane
- Manage connector lifecycle independently of the main platform

## Architecture

The sidecar consists of several key components:

- **ConnectorWatcher**: Main entry point that monitors file system for connector outputs
- **ConnectorMessageProcessor**: Processes different types of connector messages (spec, check, discover)  
- **HeartbeatMonitor**: Synchronizes with the workload server. Kills if workload server says so, or if it cannot reach the workload server
- **SidecarLogContextFactory**: Manages logging context for traceability

## Supported Operations

The sidecar handles the following connector operations:

- **SPEC**: Retrieve connector specification
- **CHECK**: Test connection to source/destination
- **DISCOVER**: Discover available data schema from source

## Configuration

Key configuration options (from `application.yml`):

| Property | Default | Description |
|----------|---------|-------------|
| `airbyte.sidecar.file-timeout-minutes` | 9 | Timeout for connector operations |
| `airbyte.sidecar.file-timeout-minutes-within-sync` | 30 | Extended timeout when run as part of a sync operation |
| `AIRBYTE_CONFIG_DIR` | `/config` | Directory containing connector configuration |
| `WORKLOAD_API_HOST` | - | Workload API endpoint for reporting results |
| `INTERNAL_API_HOST` | - | Internal API endpoint for platform communication |

## File System Integration

The sidecar monitors specific files in the connector workspace:

- **Input files**: `input.json`, configuration files
- **Output files**: Connector writes results that sidecar reads, then writes to object storage
- **Well-known types**: `WellKnownTypes.json` for protocol validation

## Build & Deploy

### Building
```bash
./gradlew :oss:airbyte-connector-sidecar:build
```

### Docker Image
```bash  
./gradlew :oss:airbyte-connector-sidecar:buildDockerImage
```

The sidecar is deployed as a Docker container (`airbyte/connector-sidecar`) that runs alongside connector containers in Kubernetes pods.

## Usage

The sidecar is typically launched automatically by the Airbyte workload launcher and should not be run manually. It expects:

1. Connector configuration files in `/config` directory
2. Network access to workload API and internal API  
3. Shared filesystem with the connector container
4. Environment variables for API endpoints and authentication

## Monitoring

The sidecar provides metrics via Micrometer to:
- OpenTelemetry (OTLP) collectors
- StatsD/DataDog for monitoring connector performance and failures

## Development

### Prerequisites
- Kotlin 1.8+
- JVM 17+
- Micronaut framework knowledge

### Key Dependencies
- **Micronaut**: Application framework
- **Airbyte Protocol**: Message format definitions
- **Jackson**: JSON/YAML processing
- **Retrofit**: HTTP client for API calls

### Testing
```bash
./gradlew :oss:airbyte-connector-sidecar:test
```

Test files are located in `src/test/kotlin/` with sample configurations in `src/test/resources/`.

## Error Handling

The sidecar handles various failure scenarios:
- Connector timeouts and crashes
- Invalid message formats  
- API communication failures
- File system issues

Failures are reported to the workload API with detailed error information for debugging.
