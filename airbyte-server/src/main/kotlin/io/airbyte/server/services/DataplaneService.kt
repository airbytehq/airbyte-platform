package io.airbyte.server.services

import io.airbyte.api.model.generated.ActorType
import io.airbyte.api.model.generated.WorkloadPriority
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.Geography
import io.airbyte.config.StandardSync
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.NetworkSecurityTokenKey
import io.airbyte.featureflag.CloudProvider
import io.airbyte.featureflag.CloudProviderRegion
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.GeographicRegion
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Priority
import io.airbyte.featureflag.Priority.Companion.HIGH_PRIORITY
import io.airbyte.featureflag.WorkloadApiRouting
import io.airbyte.featureflag.Workspace
import jakarta.inject.Singleton
import jakarta.validation.constraints.NotNull
import java.util.UUID

@Singleton
class DataplaneService(
  private val connectionService: ConnectionService,
  private val workspaceService: WorkspaceService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val featureFlagClient: FeatureFlagClient,
  private val scopedConfigurationService: ScopedConfigurationService,
) {
  /**
   * Get queue name from given data. Pulled from the WorkloadService.
   */
  fun getQueueName(
    connectionId: UUID?,
    actorType: ActorType?,
    actorId: UUID?,
    workspaceId: UUID?,
    priority: @NotNull WorkloadPriority,
  ): String {
    val connection = connectionId?.let { connectionService.getStandardSync(connectionId) }
    val resolvedWorkspaceId = workspaceId ?: resolveWorkspaceId(connection, actorType, actorId)
    val geography = getGeography(connection, resolvedWorkspaceId)

    val context =
      when (hasNetworkSecurityTokenConfig(resolvedWorkspaceId)) {
        true -> {
          buildNetworkSecurityTokenFeatureFlagContext(
            workspaceId = resolvedWorkspaceId,
            connectionId = connectionId,
            geography = geography,
          )
        }
        false -> {
          buildFeatureFlagContext(
            workspaceId = resolvedWorkspaceId,
            connectionId = connectionId,
            geography = geography,
            priority = priority,
          )
        }
      }
    return featureFlagClient.stringVariation(WorkloadApiRouting, Multi(context))
  }

  private fun resolveWorkspaceId(
    connection: StandardSync?,
    actorType: ActorType?,
    actorId: UUID?,
  ): UUID? {
    return connection?.let {
      destinationService.getDestinationConnection(connection.destinationId).workspaceId
    } ?: actorType?.let {
      when (actorType) {
        ActorType.SOURCE -> sourceService.getSourceConnection(actorId).workspaceId
        ActorType.DESTINATION -> destinationService.getDestinationConnection(actorId).workspaceId
        else -> null
      }
    }
  }

  /**
   * Given a connectionId and workspaceId, attempt to resolve geography.
   */
  private fun getGeography(
    connection: StandardSync?,
    workspaceId: UUID?,
  ): Geography {
    try {
      return connection?.let {
        connection.geography
      } ?: workspaceId?.let {
        workspaceService.getGeographyForWorkspace(workspaceId)
      } ?: Geography.AUTO
    } catch (e: Exception) {
      throw BadRequestProblem(ProblemMessageData().message("Unable to find geography of for connection [$connection], workspace [$workspaceId]"))
    }
  }

  private fun hasNetworkSecurityTokenConfig(workspaceId: UUID?): Boolean {
    return workspaceId?.let {
      scopedConfigurationService.getScopedConfigurations(
        NetworkSecurityTokenKey,
        mapOf(ConfigScopeType.WORKSPACE to workspaceId),
      ).isNotEmpty()
    } ?: false
  }

  /**
   * Build the feature flag context for network security token.
   * Uses geographic region (new).
   */
  private fun buildNetworkSecurityTokenFeatureFlagContext(
    workspaceId: UUID?,
    connectionId: UUID?,
    geography: Geography,
    priority: WorkloadPriority = WorkloadPriority.DEFAULT,
  ): MutableList<Context> {
    val context =
      mutableListOf(
        GeographicRegion(geography.toGeographicRegion()),
        CloudProvider(CloudProvider.AWS),
        CloudProviderRegion(CloudProviderRegion.AWS_US_EAST_1),
      )

    workspaceId?.let {
      context.add(Workspace(workspaceId))
    }

    connectionId?.let {
      context.add(Connection(connectionId))
    }

    if (WorkloadPriority.HIGH == priority) {
      context.add(Priority(HIGH_PRIORITY))
    }

    return context
  }
}

/**
 * Builds the old feature flag context for routing.
 * Uses geography.
 */
private fun buildFeatureFlagContext(
  workspaceId: UUID?,
  connectionId: UUID?,
  geography: Geography,
  priority: WorkloadPriority = WorkloadPriority.DEFAULT,
): MutableList<Context> {
  val context = mutableListOf<Context>(io.airbyte.featureflag.Geography(geography.toString()))

  workspaceId?.let {
    context.add(Workspace(workspaceId))
  }

  connectionId?.let {
    context.add(Connection(connectionId))
  }

  if (WorkloadPriority.HIGH == priority) {
    context.add(Priority(HIGH_PRIORITY))
  }

  return context
}

fun Geography.toGeographicRegion(): String {
  return when (this) {
    Geography.AUTO -> GeographicRegion.US
    Geography.US -> GeographicRegion.US
    Geography.EU -> GeographicRegion.EU
  }
}
