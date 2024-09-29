package io.airbyte.analytics.config

import io.airbyte.api.client.model.generated.DeploymentMetadataRead
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Configs
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.micronaut.core.util.StringUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import java.util.function.Function
import java.util.function.Supplier

val BLANK_UUID = UUID(0, 0)

@Factory
class AnalyticsTrackingBeanFactory {
  @Singleton
  @Named("deploymentSupplier")
  fun deploymentSupplier(
    airbyteVersion: AirbyteVersion,
    deploymentMode: Configs.DeploymentMode,
  ): Supplier<DeploymentMetadataRead> =
    Supplier {
      DeploymentMetadataRead(
        id = BLANK_UUID,
        mode = deploymentMode.name,
        version = airbyteVersion.serialize(),
      )
    }

  @Singleton
  @Named("workspaceFetcher")
  fun workspaceFetcher(): Function<UUID, WorkspaceRead> {
    return Function {
        workspaceId: UUID ->
      WorkspaceRead(
        workspaceId = workspaceId,
        customerId = workspaceId,
        name = "",
        slug = "",
        initialSetupComplete = true,
        organizationId = workspaceId,
      )
    }
  }

  @Singleton
  @Requires(missingBeans = [AirbyteVersion::class])
  fun airbyteVersion(
    @Value("\${airbyte.version}") airbyteVersion: String,
  ): AirbyteVersion = AirbyteVersion(airbyteVersion)

  @Singleton
  @Requires(missingBeans = [Configs.DeploymentMode::class])
  fun deploymentMode(
    @Value("\${airbyte.deployment-mode}") deploymentMode: String,
  ): Configs.DeploymentMode = convertToEnum(deploymentMode, Configs.DeploymentMode::valueOf, Configs.DeploymentMode.OSS)

  private fun <T> convertToEnum(
    value: String,
    creatorFunction: Function<String, T>,
    @Suppress("SameParameterValue") defaultValue: T,
  ): T = if (StringUtils.isNotEmpty(value)) creatorFunction.apply(value.uppercase()) else defaultValue
}
