package io.airbyte.api.server.apiTracking

import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Configs
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.core.util.StringUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import java.util.function.Function
import java.util.function.Supplier

@Factory
class AnalyticsTrackingBeanFactory {
  @Singleton
  @Named("deploymentId")
  fun deploymentId(): UUID {
    return UUID.randomUUID()
  }

  @Singleton
  @Named("deploymentIdSupplier")
  fun deploymentIdSupplier(
    @Named("deploymentId") deploymentId: UUID,
  ): Supplier<UUID> {
    return Supplier { deploymentId }
  }

  @Singleton
  @Named("workspaceFetcher")
  fun workspaceFetcher(): Function<UUID, WorkspaceRead> {
    return Function { workspaceId: UUID -> WorkspaceRead().workspaceId(workspaceId) }
  }

  @Singleton
  fun airbyteVersion(
    @Value("\${airbyte.version}") airbyteVersion: String,
  ): AirbyteVersion {
    return AirbyteVersion(airbyteVersion)
  }

  @Singleton
  fun deploymentMode(
    @Value("\${airbyte.deployment-mode}") deploymentMode: String,
  ): Configs.DeploymentMode {
    return convertToEnum(deploymentMode, Configs.DeploymentMode::valueOf, Configs.DeploymentMode.OSS)
  }

  private fun <T> convertToEnum(
    value: String,
    creatorFunction: Function<String, T>,
    defaultValue: T,
  ): T {
    return if (StringUtils.isNotEmpty(value)) creatorFunction.apply(value.uppercase()) else defaultValue
  }
}
