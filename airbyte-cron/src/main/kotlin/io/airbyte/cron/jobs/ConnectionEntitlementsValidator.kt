/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import datadog.trace.api.Trace
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.config.ActorType
import io.airbyte.cron.SCHEDULED_TRACE_OPERATION_NAME
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.persistence.job.WorkspaceHelper
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger { }

/**
 * Ensures that active connections are entitled to use their respective connectors.
 * If an organization is not entitled to use a connector, all connections using that connector are disabled.
 */
@Singleton
class ConnectionEntitlementsValidator(
  private val destinationService: DestinationService,
  private val sourceService: SourceService,
  private val connectionService: ConnectionService,
  private val workspaceHelper: WorkspaceHelper,
  private val licenseEntitlementChecker: LicenseEntitlementChecker,
) {
  @Scheduled(fixedRate = "1h")
  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  fun validateEntitlements() {
    logger.info { "Validating entitlements for actively used source connectors..." }
    sourceService
      .listPublicSourceDefinitions(false)
      .filter { it.enterprise }
      .forEach { sourceDef ->
        val connectionsByOrg = getActiveConnectionsByOrg(sourceDef.sourceDefinitionId, ActorType.SOURCE)
        checkEntitlementsAndDisable(connectionsByOrg, sourceDef.sourceDefinitionId, ActorType.SOURCE)
      }

    logger.info { "Validating entitlements for actively used destination connectors..." }
    destinationService
      .listPublicDestinationDefinitions(false)
      .filter { it.enterprise }
      .forEach { destDef ->
        val connectionsByOrg = getActiveConnectionsByOrg(destDef.destinationDefinitionId, ActorType.DESTINATION)
        checkEntitlementsAndDisable(connectionsByOrg, destDef.destinationDefinitionId, ActorType.DESTINATION)
      }
  }

  private fun getActiveConnectionsByOrg(
    actorDefinitionId: UUID,
    actorType: ActorType,
  ): Map<UUID, List<UUID>> {
    val connections = connectionService.listConnectionsByActorDefinitionIdAndType(actorDefinitionId, actorType.toString(), false, false)
    if (connections.isEmpty()) {
      return emptyMap()
    }

    val sourcesById = sourceService.listSourcesWithIds(connections.map { it.sourceId }).associateBy { it.sourceId }
    val orgToConnectionsMap = mutableMapOf<UUID, MutableList<UUID>>()
    for (connection in connections) {
      val workspaceId = sourcesById[connection.sourceId]!!.workspaceId
      val organizationId = workspaceHelper.getOrganizationForWorkspace(workspaceId)
      orgToConnectionsMap
        .getOrPut(organizationId) { mutableListOf() }
        .add(connection.connectionId)
    }

    return orgToConnectionsMap
  }

  private fun checkEntitlementsAndDisable(
    connectionsByOrg: Map<UUID, List<UUID>>,
    actorDefinitionId: UUID,
    actorType: ActorType,
  ) {
    val entitlement =
      when (actorType) {
        ActorType.SOURCE -> Entitlement.SOURCE_CONNECTOR
        ActorType.DESTINATION -> Entitlement.DESTINATION_CONNECTOR
      }

    for ((organizationId, connIds) in connectionsByOrg) {
      val isEntitled = licenseEntitlementChecker.checkEntitlement(organizationId, entitlement, actorDefinitionId)
      if (!isEntitled) {
        connectionService.disableConnectionsById(connIds)
        logger.info { "Disabled ${connIds.size} connections for organization $organizationId using connector $actorDefinitionId" }
      }
    }
  }
}
