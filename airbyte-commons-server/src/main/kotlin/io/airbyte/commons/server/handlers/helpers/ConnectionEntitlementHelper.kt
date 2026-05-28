/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.commons.entitlements.EntitlementHelper
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.ConnectorEntitlement
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.entitlements.models.FasterSyncFrequencyEntitlement
import io.airbyte.commons.entitlements.models.FifteenMinuteSyncFrequencyEntitlement
import io.airbyte.commons.entitlements.models.MappersEntitlement
import io.airbyte.config.StandardSync
import io.airbyte.config.helpers.CronExpressionHelper
import io.airbyte.config.helpers.ScheduleHelpers.setBasicHourlySchedule
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
    private const val FIFTEEN_MINUTE_SYNC_FLOOR_MINUTES = 15L
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

    if (!hasRequiredSyncFrequencyEntitlement(connection, subHourSyncIds, organizationId)) {
      return false
    }

    if (connection.catalog.streams.any { it.mappers.isNotEmpty() }) {
      if (!entitlementService.checkEntitlement(organizationId, MappersEntitlement).isEntitled) {
        return false
      }
    }

    return sourceIsEntitled && destinationIsEntitled
  }

  private fun hasRequiredSyncFrequencyEntitlement(
    connection: StandardSync,
    subHourSyncIds: Collection<UUID>,
    organizationId: OrganizationId,
  ): Boolean {
    if (!subHourSyncIds.contains(connection.connectionId)) {
      return true
    }

    if (entitlementService.checkEntitlement(organizationId, FasterSyncFrequencyEntitlement).isEntitled) {
      return true
    }

    if (!entitlementService.checkEntitlement(organizationId, FifteenMinuteSyncFrequencyEntitlement).isEntitled) {
      return false
    }

    return !requiresLegacyFasterSyncEntitlement(connection)
  }

  private fun requiresLegacyFasterSyncEntitlement(connection: StandardSync): Boolean {
    val basicSchedule = connection.scheduleData?.basicSchedule
    if (basicSchedule != null) {
      return basicSchedule.timeUnit == io.airbyte.config.BasicSchedule.TimeUnit.MINUTES &&
        basicSchedule.units < FIFTEEN_MINUTE_SYNC_FLOOR_MINUTES
    }

    val cronSchedule = connection.scheduleData?.cron
    if (cronSchedule != null) {
      return try {
        cronExpressionHelper.executesMoreThanOncePerFifteenMinutes(cronSchedule.cronExpression)
      } catch (e: IllegalArgumentException) {
        logger.warn(e) {
          "Invalid cron expression while validating connection ${connection.connectionId} for 15-minute sync entitlement"
        }
        true
      }
    }

    val legacySchedule = connection.schedule
    if (legacySchedule != null) {
      return legacySchedule.timeUnit == io.airbyte.config.Schedule.TimeUnit.MINUTES &&
        legacySchedule.units < FIFTEEN_MINUTE_SYNC_FLOOR_MINUTES
    }

    logger.warn {
      "Unable to determine sub-hour sync cadence for connection ${connection.connectionId}; requiring legacy faster-sync entitlement to unlock it"
    }
    return true
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

  private fun lacksSubHourSyncEntitlement(organizationId: OrganizationId): Boolean =
    !entitlementService.checkEntitlement(organizationId, FasterSyncFrequencyEntitlement).isEntitled &&
      !entitlementService.checkEntitlement(organizationId, FifteenMinuteSyncFrequencyEntitlement).isEntitled

  /**
   * Unlocks all LOCKED connections for an organization that they are entitled to use.
   * For each entitled connection, updates the status to INACTIVE and removes the statusReason.
   *
   * @param organizationId The organization ID
   */
  fun unlockEntitledConnectionsForOrganization(organizationId: OrganizationId) {
    logger.info { "Unlocking entitled connections for organization: $organizationId" }

    val connectionIds = connectionService.listConnectionIdsForOrganization(organizationId.value)
    val subHourSyncIds = entitlementHelper.findSubHourSyncIds(organizationId).toSet()
    val shouldDowngradeSubHourSyncs = subHourSyncIds.isNotEmpty() && lacksSubHourSyncEntitlement(organizationId)

    logger.debug { "Found ${connectionIds.size} connections for organization $organizationId" }

    connectionIds.forEach { connectionId ->
      try {
        val connection = connectionService.getStandardSync(connectionId)

        if (connection.status == StandardSync.Status.LOCKED) {
          logger.debug { "Checking entitlement for locked connection: $connectionId" }

          val effectiveSubHourSyncIds =
            if (shouldDowngradeSubHourSyncs && subHourSyncIds.contains(connectionId)) {
              logger.info { "Downgrading locked sub-hour connection $connectionId to an hourly schedule before entitlement recheck" }
              connectionService.writeStandardSync(setBasicHourlySchedule(connection))
              subHourSyncIds - connectionId
            } else {
              subHourSyncIds
            }

          val sourceDefinition = sourceService.getSourceDefinitionFromSource(connection.sourceId)
          val destinationDefinition = destinationService.getDestinationDefinitionFromDestination(connection.destinationId)

          val isEntitled =
            isEntitledToConnection(
              connection,
              effectiveSubHourSyncIds,
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
