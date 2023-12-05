# airbyte-analytics

This module contains the logic for analytics tracking.

## Supported Analytics Trackers

* Logging/Standard Out
* [Segment](https://segment.com/)

## Configuration

The analytics tracker is initialized automatically based on the current service configuration.  The type of the
analytics tracker can be configured by setting the `TRACKING_STRATEGY` environment variable.  Valid options
for this environment variable include:

* `LOGGING`
* `SEGMENT`

In addition to the type of analytics tracker, the following environment variables must also be set:

* `AIRBYTE_ROLE`
* `AIRBYTE_VERSION`
* `DEPLOYMENT_MODE`

## Development

In order to use this module in a service at runtime,  add the following configuration to the service's `application.yml` file:

```yaml
airbyte:
  deployment-mode: ${DEPLOYMENT_MODE:OSS}
  role: ${AIRBYTE_ROLE:dev}
  tracking-strategy: ${TRACKING_STRATEGY:LOGGING}  
  version: ${AIRBYTE_VERSION:dev}
```

Finally, you must also declare the following singleton beans:

```kotlin
@Singleton
@Named("deploymentIdSupplier")
fun deploymentIdSupplier(jobPersistence:JobPersistence): Supplier<UUID> {
    return Supplier { jobPersistence.getDeployment().orElseThrow() }
}

@Singleton
@Named("workspaceFetcher")
fun workspaceFetcher(workspaceApi: WorkspaceApi): Function<UUID, WorkspaceRead> {
    return Function { workspaceId:UUID -> workspaceApi.getWorkspace(WorkspaceIdRequestBody().workspaceId(workspaceId).includeTombstone(true)) }
}

@Singleton
fun airbyteVersion(@Value("\${airbyte.version}") airbyteVersion: String): AirbyteVersion {
    return AirbyteVersion(airbyteVersion)
}

@Singleton
fun deploymentMode(@Value("\${airbyte.deployment-mode}") deploymentMode: String): Configs.DeploymentMode  {
    return convertToEnum(deploymentMode, Configs.DeploymentMode::valueOf, Configs.DeploymentMode.OSS);
}

private fun <T> convertToEnum(value: String, creatorFunction: Function<String, T>, defaultValue: T): T {
    return if (StringUtils.isNotEmpty(value)) creatorFunction.apply(value.uppercase()) else defaultValue
}

```

Note that the configuration above is an example and may need to be modified to fit the including service.