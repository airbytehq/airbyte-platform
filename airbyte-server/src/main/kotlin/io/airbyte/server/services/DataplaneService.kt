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
  private fun resolveWorkspaceId(
    connection: StandardSync?,
    actorType: ActorType?,
    actorId: UUID?,
  ): UUID {
    return connection?.let {
      destinationService.getDestinationConnection(connection.destinationId).workspaceId
    } ?: actorType?.let {
      when (actorType) {
        ActorType.SOURCE -> sourceService.getSourceConnection(actorId).workspaceId
        ActorType.DESTINATION -> destinationService.getDestinationConnection(actorId).workspaceId
        else -> null
      }
    } ?: run {
      throw BadRequestProblem(
        ProblemMessageData().message(
          "Unable to resolve workspace id for connection [${connection?.connectionId}], actor [${actorType?.name}], actorId [$actorId]",
        ),
      )
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

    getQueueWithScopedConfig(resolvedWorkspaceId, connectionId, geography)?.let {
      return it
    }

    val context = mutableListOf(io.airbyte.featureflag.Geography(geography.toString()), Workspace(resolvedWorkspaceId))
    if (WorkloadPriority.HIGH == priority) {
      context.add(Priority(HIGH_PRIORITY))
    }
    connectionId?.let {
      context.add(Connection(it))
    }

    return featureFlagClient.stringVariation(WorkloadApiRouting, Multi(context))
  }

  private fun getQueueWithScopedConfig(
    workspaceId: UUID,
    connectionId: UUID?,
    geography: Geography,
  ): String? {
    val scopedConfigs =
      scopedConfigurationService.getScopedConfigurations(
        NetworkSecurityTokenKey,
        mapOf(ConfigScopeType.WORKSPACE to workspaceId),
      )

    // Very hardcoded for now
    val context =
      mutableListOf(
        CloudProvider(CloudProvider.AWS),
        GeographicRegion(geography.toGeographicRegion()),
        Workspace(workspaceId.toString()),
        CloudProviderRegion(CloudProviderRegion.AWS_US_EAST_1),
      )

    connectionId?.let {
      context.add(Connection(it))
    }

    if (scopedConfigs.isNotEmpty()) {
      return featureFlagClient.stringVariation(WorkloadApiRouting, Multi(context))
    }

    return null
  }
}

fun Geography.toGeographicRegion(): String {
  return when (this) {
    Geography.AUTO -> GeographicRegion.US
    Geography.US -> GeographicRegion.US
    Geography.EU -> GeographicRegion.EU
  }
}
