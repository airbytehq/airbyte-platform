/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.api.model.generated.ActorType
import io.airbyte.api.model.generated.WorkloadPriority
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.DataplaneNameAlreadyExistsProblem
import io.airbyte.commons.constants.GEOGRAPHY_AUTO
import io.airbyte.commons.constants.GEOGRAPHY_EU
import io.airbyte.commons.constants.GEOGRAPHY_US
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.Dataplane
import io.airbyte.config.StandardSync
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DataplaneAuthService
import io.airbyte.data.services.DataplaneService
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
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import jakarta.validation.constraints.NotNull
import org.jooq.exception.DataAccessException
import java.util.UUID

@Singleton
open class DataplaneService(
  private val connectionService: ConnectionService,
  private val workspaceService: WorkspaceService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val featureFlagClient: FeatureFlagClient,
  private val scopedConfigurationService: ScopedConfigurationService,
  private val dataplaneDataService: DataplaneService,
  private val dataplaneAuthService: DataplaneAuthService,
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
    val connection = connectionId?.let { connectionService.getStandardSync(it) }
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
  ): UUID? =
    connection?.let {
      destinationService.getDestinationConnection(it.destinationId).workspaceId
    } ?: when (actorType) {
      ActorType.SOURCE -> sourceService.getSourceConnection(actorId).workspaceId
      ActorType.DESTINATION -> destinationService.getDestinationConnection(actorId).workspaceId
      else -> null
    }

  /**
   * Given a connectionId and workspaceId, attempt to resolve geography.
   */
  private fun getGeography(
    connection: StandardSync?,
    workspaceId: UUID?,
  ): String {
    try {
      return connection?.geography
        ?: workspaceId?.let {
          workspaceService.getGeographyForWorkspace(it)
        } ?: GEOGRAPHY_AUTO
    } catch (_: Exception) {
      throw BadRequestProblem(ProblemMessageData().message("Unable to find geography of for connection [$connection], workspace [$workspaceId]"))
    }
  }

  private fun hasNetworkSecurityTokenConfig(workspaceId: UUID?): Boolean =
    workspaceId?.let {
      scopedConfigurationService
        .getScopedConfigurations(
          NetworkSecurityTokenKey,
          mapOf(ConfigScopeType.WORKSPACE to it),
        ).isNotEmpty()
    } ?: false

  /**
   * Build the feature flag context for network security token.
   * Uses geographic region (new).
   */
  private fun buildNetworkSecurityTokenFeatureFlagContext(
    workspaceId: UUID?,
    connectionId: UUID?,
    geography: String,
    priority: WorkloadPriority = WorkloadPriority.DEFAULT,
  ): MutableList<Context> {
    val context =
      mutableListOf(
        GeographicRegion(geography.toGeographicRegion()),
        CloudProvider(CloudProvider.AWS),
        CloudProviderRegion(CloudProviderRegion.AWS_US_EAST_1),
      )

    workspaceId?.let { context.add(Workspace(it)) }
    connectionId?.let { context.add(Connection(it)) }

    if (WorkloadPriority.HIGH == priority) {
      context.add(Priority(HIGH_PRIORITY))
    }

    return context
  }

  fun createCredentials(dataplaneId: UUID): io.airbyte.config.DataplaneClientCredentials = dataplaneAuthService.createCredentials(dataplaneId)

  fun listDataplanes(dataplaneGroupId: UUID): List<Dataplane> = dataplaneDataService.listDataplanes(dataplaneGroupId, false)

  fun updateDataplane(
    dataplaneId: UUID,
    updatedName: String,
    updatedEnabled: Boolean,
  ): Dataplane {
    val existingDataplane = dataplaneDataService.getDataplane(dataplaneId)

    val updatedDataplane =
      existingDataplane.apply {
        name = updatedName
        enabled = updatedEnabled
      }

    return writeDataplane(updatedDataplane)
  }

  @Transactional("config")
  open fun deleteDataplane(dataplaneId: UUID): Dataplane {
    val existingDataplane = dataplaneDataService.getDataplane(dataplaneId)
    val tombstonedDataplane =
      existingDataplane.apply {
        tombstone = true
      }

    dataplaneAuthService.listCredentialsByDataplaneId(existingDataplane.id).map { dataplaneAuthService.deleteCredentials(it.id) }
    return writeDataplane(tombstonedDataplane)
  }

  fun getToken(
    clientId: String,
    clientSecret: String,
  ): String = dataplaneAuthService.getToken(clientId, clientSecret)

  fun getDataplaneFromClientId(clientId: String): Dataplane {
    val dataplaneId = dataplaneAuthService.getDataplaneId(clientId)
    return dataplaneDataService.getDataplane(dataplaneId)
  }

  fun writeDataplane(dataplane: Dataplane): Dataplane {
    try {
      return dataplaneDataService.writeDataplane(dataplane)
    } catch (e: DataAccessException) {
      if (e.message?.contains("duplicate key value violates unique constraint") == true &&
        e.message?.contains("dataplane_dataplane_group_id_name_key") == true
      ) {
        throw DataplaneNameAlreadyExistsProblem()
      }
      throw e
    }
  }
}

/**
 * Builds the old feature flag context for routing.
 * Uses geography.
 */
private fun buildFeatureFlagContext(
  workspaceId: UUID?,
  connectionId: UUID?,
  geography: String,
  priority: WorkloadPriority = WorkloadPriority.DEFAULT,
): MutableList<Context> {
  val context = mutableListOf<Context>(io.airbyte.featureflag.Geography(geography.toString()))

  workspaceId?.let { context.add(Workspace(it)) }
  connectionId?.let { context.add(Connection(it)) }

  if (WorkloadPriority.HIGH == priority) {
    context.add(Priority(HIGH_PRIORITY))
  }

  return context
}

fun String.toGeographicRegion(): String =
  when (this) {
    GEOGRAPHY_AUTO -> GeographicRegion.US
    GEOGRAPHY_US -> GeographicRegion.US
    GEOGRAPHY_EU -> GeographicRegion.EU
    else -> {
      // TODO: how is this used? will this interfere with the Privatelink dataplane?
      throw IllegalArgumentException("Unknown geographic region: $this")
    }
  }
