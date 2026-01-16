/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.commons.entitlements.EntitlementHelper
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.ConnectorEntitlement
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.entitlements.models.FasterSyncFrequencyEntitlement
import io.airbyte.commons.entitlements.models.MappersEntitlement
import io.airbyte.config.StandardSync
import io.airbyte.config.helpers.CronExpressionHelper
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.models.OrganizationId
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Helper class for checking connection entitlements.
 */
@Singleton
class ConnectionEntitlementHelper(
  private val connectionService: ConnectionService,
  private val cronExpressionHelper: CronExpressionHelper,
  private val entitlementService: EntitlementService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val entitlementHelper: EntitlementHelper,
) {
  companion object {
    private val logger = KotlinLogging.logger {}
  }

  /**
   * Checks if an organization is entitled to use both the source and destination connectors.
   *
   * @param sourceDefinitionId The source definition ID
   * @param destinationDefinitionId The destination definition ID
   * @param organizationId The organization ID
   * @return true if entitled to both connectors, false otherwise
   */
  fun isEntitledToConnection(
    connection: StandardSync,
    subHourSyncIds: Collection<UUID>,
    sourceDefinitionId: UUID,
    destinationDefinitionId: UUID,
    organizationId: OrganizationId,
  ): Boolean {
    val sourceIsEntitled = isEntitledToConnector(sourceDefinitionId, organizationId)
    val destinationIsEntitled = isEntitledToConnector(destinationDefinitionId, organizationId)

    if (subHourSyncIds.contains(connection.connectionId)) {
      if (!entitlementService.checkEntitlement(organizationId, FasterSyncFrequencyEntitlement).isEntitled) {
        return false
      }
    }

    if (connection.catalog.streams.any { it.mappers.isNotEmpty() }) {
      if (!entitlementService.checkEntitlement(organizationId, MappersEntitlement).isEntitled) {
        return false
      }
    }

    return sourceIsEntitled && destinationIsEntitled
  }

  private fun isEntitledToConnector(
    connectorDefinitionId: UUID,
    organizationId: OrganizationId,
  ): Boolean {
    val entitlement = Entitlements.fromId(ConnectorEntitlement.PREFIX + connectorDefinitionId)
    return if (entitlement != null) {
      entitlementService.checkEntitlement(organizationId, entitlement).isEntitled
    } else {
      true
    }
  }

  /**
   * Unlocks all LOCKED connections for an organization that they are entitled to use.
   * For each entitled connection, updates the status to INACTIVE and removes the statusReason.
   *
   * @param organizationId The organization ID
   */
  fun unlockEntitledConnectionsForOrganization(organizationId: OrganizationId) {
    logger.info { "Unlocking entitled connections for organization: $organizationId" }

    val connectionIds = connectionService.listConnectionIdsForOrganization(organizationId.value)
    val subHourSyncIds = entitlementHelper.findSubHourSyncIds(organizationId)

    logger.debug { "Found ${connectionIds.size} connections for organization $organizationId" }

    connectionIds.forEach { connectionId ->
      try {
        val connection = connectionService.getStandardSync(connectionId)

        if (connection.status == StandardSync.Status.LOCKED) {
          logger.debug { "Checking entitlement for locked connection: $connectionId" }

          val sourceDefinition = sourceService.getSourceDefinitionFromSource(connection.sourceId)
          val destinationDefinition = destinationService.getDestinationDefinitionFromDestination(connection.destinationId)

          val isEntitled =
            isEntitledToConnection(
              connection,
              subHourSyncIds,
              sourceDefinition.sourceDefinitionId,
              destinationDefinition.destinationDefinitionId,
              organizationId,
            )

          if (isEntitled) {
            logger.info { "Unlocking connection $connectionId (was entitled)" }
            connectionService.updateConnectionStatus(connectionId, StandardSync.Status.INACTIVE, null)
          } else {
            logger.debug { "Connection $connectionId remains locked (not entitled)" }
          }
        }
      } catch (e: Exception) {
        logger.error(e) { "Error processing connection $connectionId for organization $organizationId" }
      }
    }

    logger.info { "Finished unlocking entitled connections for organization: $organizationId" }
  }
}
