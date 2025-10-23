/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.entitlements.models.FasterSyncFrequencyEntitlement
import io.airbyte.commons.entitlements.models.MappersEntitlement
import io.airbyte.commons.entitlements.models.RbacRolesEntitlement
import io.airbyte.config.ActorType
import io.airbyte.config.Permission
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_EDITOR
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_MEMBER
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_READER
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_RUNNER
import io.airbyte.config.Permission.PermissionType.WORKSPACE_ADMIN
import io.airbyte.config.Permission.PermissionType.WORKSPACE_EDITOR
import io.airbyte.config.Permission.PermissionType.WORKSPACE_READER
import io.airbyte.config.Permission.PermissionType.WORKSPACE_RUNNER
import io.airbyte.config.StatusReason
import io.airbyte.config.helpers.CronExpressionHelper
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.shared.ConnectionCronSchedule
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.EntitlementPlan.STANDARD
import io.airbyte.domain.models.EntitlementPlan.UNIFIED_TRIAL
import io.airbyte.domain.models.OrganizationId
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
internal class FeatureDegradationService(
  private val permissionService: PermissionService,
  private val entitlementClient: EntitlementClient,
  private val connectionService: ConnectionService,
  private val workspacePersistence: WorkspacePersistence,
  private val cronExpressionHelper: CronExpressionHelper,
) {
  internal fun downgradeFeaturesIfRequired(
    organizationId: OrganizationId,
    fromPlan: EntitlementPlan,
    toPlan: EntitlementPlan,
  ) {
    logger.info { "Checking for feature downgrades. organizationId=$organizationId fromPlan=$fromPlan toPlan=$toPlan" }

    if (!(fromPlan == UNIFIED_TRIAL && toPlan == STANDARD)) {
      logger.info { "Downgrade not supported from plan $fromPlan to $toPlan. Skipping downgrade." }
      return
    }

    try {
      // Get current entitlements to identify features the customer currently has
      val currentEntitlements = entitlementClient.getEntitlements(organizationId)
      val currentFeatureIds = currentEntitlements.filter { it.isEntitled }.map { it.featureId }.toSet()

      val newFeatureIds = entitlementClient.getEntitlementsForPlan(toPlan).map { it.featureId }.toSet()
      logger.info {
        "Customer currently has ${currentFeatureIds.size} entitled features. " +
          "organizationId=$organizationId features=$currentFeatureIds"
      }

      val allEntitlementsToDowngrade = currentFeatureIds - newFeatureIds

      val connectionsToDowngrade = mutableSetOf<UUID>()

      val (connectorEntitlementsToDowngrade, entitlementsToDowngrade) =
        allEntitlementsToDowngrade.partition { Entitlements.isEnterpriseConnectorEntitlementId(it) }

      val (sourceDefinitionsToDowngrade, destinationDefinitionsToDowngrade) =
        connectorEntitlementsToDowngrade.partition { Entitlements.isEnterpriseSourceConnectorEntitlementId(it) }

      logger.info {
        "Updating entitlement plan from $fromPlan to $toPlan. Revoking access to entitlements=$entitlementsToDowngrade and connections=$connectorEntitlementsToDowngrade for organizationId=$organizationId"
      }

      for (entitlementToDowngrade in entitlementsToDowngrade) {
        logger.debug {
          "Downgrading access to featureId=$entitlementToDowngrade organizationId=$organizationId"
        }
        when (entitlementToDowngrade) {
          RbacRolesEntitlement.featureId -> downgradeRBACRoles(organizationId)
          MappersEntitlement.featureId ->
            connectionsToDowngrade.addAll(connectionService.listConnectionIdsForOrganizationWithMappers(organizationId.value))
          FasterSyncFrequencyEntitlement.featureId ->
            connectionsToDowngrade.addAll(findSubHourSyncIds(organizationId))
          else -> logger.debug { "No downgrade flow defined for $entitlementToDowngrade" }
        }
      }

      logger.info {
        "Revoking access to enterprise connector featureIds=$connectorEntitlementsToDowngrade for organizationId=$organizationId"
      }

      connectionsToDowngrade.addAll(
        connectionService.listConnectionIdsForOrganizationAndActorDefinitions(
          organizationId.value,
          sourceDefinitionsToDowngrade.mapNotNull { Entitlements.actorDefinitionIdFromFeatureId(it) },
          ActorType.SOURCE,
        ),
      )
      connectionsToDowngrade.addAll(
        connectionService.listConnectionIdsForOrganizationAndActorDefinitions(
          organizationId.value,
          destinationDefinitionsToDowngrade.mapNotNull { Entitlements.actorDefinitionIdFromFeatureId(it) },
          ActorType.DESTINATION,
        ),
      )

      if (connectionsToDowngrade.isNotEmpty()) {
        logger.debug {
          "Locking the following connections for organizationId=$organizationId : connectionIds=$connectionsToDowngrade"
        }
        connectionService.lockConnectionsById(connectionsToDowngrade, StatusReason.SUBSCRIPTION_DOWNGRADED_ACCESS_REVOKED.value)
      }

      logger.info { "Feature downgrade check completed. organizationId=$organizationId fromPlan=$fromPlan toPlan=$toPlan" }
    } catch (e: Exception) {
      logger.error(e) { "Error checking for feature downgrades. organizationId=$organizationId fromPlan=$fromPlan toPlan=$toPlan" }
      // Don't fail the plan update if we can't check for downgrades
    }
  }

  fun downgradeRBACRoles(
    organizationId: OrganizationId,
    targetPermissionType: Permission.PermissionType = WORKSPACE_ADMIN,
  ) {
    val defaultWorkspace = workspacePersistence.getDefaultWorkspaceForOrganization(organizationId.value)

    val workspaceLevelPermissions = permissionService.getPermissionsByWorkspaceId(defaultWorkspace.workspaceId)
    workspaceLevelPermissions.forEach {
      when (it.permissionType) {
        WORKSPACE_EDITOR, WORKSPACE_RUNNER, WORKSPACE_READER -> {
          logger.debug { "Degrading permission id ${it.permissionId} from type ${it.permissionType} to $targetPermissionType" }
          it.permissionType = targetPermissionType
        }
        else -> logger.debug { "No degradation needed for permission id ${it.permissionId} of type ${it.permissionType}" }
      }
    }
    permissionService.updatePermissions(workspaceLevelPermissions)

    val orgLevelPermissions = permissionService.getPermissionsByOrganizationId(organizationId.value)
    orgLevelPermissions.forEach {
      when (it.permissionType) {
        ORGANIZATION_EDITOR, ORGANIZATION_RUNNER, ORGANIZATION_READER, ORGANIZATION_MEMBER -> {
          logger.debug {
            "Degrading permission id ${it.permissionId} from type ${it.permissionType} to $targetPermissionType for workspace ${defaultWorkspace.workspaceId}"
          }
          it.workspaceId = defaultWorkspace.workspaceId
          it.organizationId = null
          it.permissionType = targetPermissionType
        }
        else -> logger.debug { "No degradation needed for permission id ${it.permissionId} of type ${it.permissionType}" }
      }
    }
    permissionService.updatePermissions(orgLevelPermissions)
  }

  fun findSubHourSyncIds(organizationId: OrganizationId): Collection<UUID> {
    val fastBasicSyncIds = connectionService.listSubHourConnectionIdsForOrganization(organizationId.value)
    val cronSchedules = connectionService.listConnectionCronSchedulesForOrganization(organizationId.value)

    val fastCronSyncIds =
      cronSchedules
        .filter {
          try {
            cronExpressionHelper.executesMoreThanOncePerHour(it.scheduleData.cron.cronExpression)
          } catch (e: IllegalArgumentException) {
            logger.warn { "Invalid cron expression for connection id=${it.id}, ${e.message}" }
            false
          }
        }.map(ConnectionCronSchedule::id)

    return fastBasicSyncIds + fastCronSyncIds
  }
}
