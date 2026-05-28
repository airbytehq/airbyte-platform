/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.entitlements.models.FasterSyncFrequencyEntitlement
import io.airbyte.commons.entitlements.models.FifteenMinuteSyncFrequencyEntitlement
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
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.StatusReason
import io.airbyte.config.helpers.ScheduleHelpers.setBasicHourlySchedule
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.UserInvitationService
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.EntitlementPlan.PLUS
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
  private val userInvitationService: UserInvitationService,
  private val entitlementHelper: EntitlementHelper,
) {
  internal fun downgradeFeaturesIfRequired(
    organizationId: OrganizationId,
    fromPlan: EntitlementPlan,
    toPlan: EntitlementPlan,
  ) {
    logger.info { "Checking for feature downgrades. organizationId=$organizationId fromPlan=$fromPlan toPlan=$toPlan" }

    if (!isSupportedFeatureDegradation(fromPlan, toPlan)) {
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
      var shouldDowngradeSubHourSyncs = false

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
          RbacRolesEntitlement.featureId -> downgradeRBAC(organizationId)
          MappersEntitlement.featureId ->
            connectionsToDowngrade.addAll(connectionService.listConnectionIdsForOrganizationWithMappers(organizationId.value))
          FasterSyncFrequencyEntitlement.featureId -> shouldDowngradeSubHourSyncs = true
          FifteenMinuteSyncFrequencyEntitlement.featureId -> shouldDowngradeSubHourSyncs = true
          else -> logger.debug { "No downgrade flow defined for $entitlementToDowngrade" }
        }
      }

      if (shouldDowngradeSubHourSyncs) {
        connectionsToDowngrade.addAll(downgradeSubHourSyncSchedulesToHourly(organizationId))
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

  private fun downgradeSubHourSyncSchedulesToHourly(organizationId: OrganizationId): Set<UUID> {
    val subHourSyncIds = entitlementHelper.findSubHourSyncIds(organizationId)
    if (subHourSyncIds.isEmpty()) {
      return emptySet()
    }

    logger.info {
      "Downgrading ${subHourSyncIds.size} sub-hour connection schedules to hourly for organizationId=$organizationId"
    }

    val failedScheduleDowngrades = mutableSetOf<UUID>()
    subHourSyncIds.forEach { connectionId ->
      try {
        val connection = connectionService.getStandardSync(connectionId)
        connectionService.writeStandardSync(setBasicHourlySchedule(connection))
      } catch (e: Exception) {
        logger.error(e) { "Failed to downgrade sub-hour connection schedule to hourly. organizationId=$organizationId connectionId=$connectionId" }
        failedScheduleDowngrades.add(connectionId)
      }
    }
    return failedScheduleDowngrades
  }

  private fun isSupportedFeatureDegradation(
    fromPlan: EntitlementPlan,
    toPlan: EntitlementPlan,
  ): Boolean = toPlan == STANDARD && fromPlan in setOf(UNIFIED_TRIAL, PLUS)

  fun downgradeRBAC(
    organizationId: OrganizationId,
    targetPermissionType: Permission.PermissionType = WORKSPACE_ADMIN,
  ) {
    val defaultWorkspace = workspacePersistence.getDefaultWorkspaceForOrganization(organizationId.value)

    downgradePermissions(organizationId, defaultWorkspace, targetPermissionType)
    downgradeUserInvites(organizationId, defaultWorkspace, targetPermissionType)
  }

  fun downgradePermissions(
    organizationId: OrganizationId,
    defaultWorkspace: StandardWorkspace,
    targetPermissionType: Permission.PermissionType = WORKSPACE_ADMIN,
  ) {
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

  fun downgradeUserInvites(
    organizationId: OrganizationId,
    defaultWorkspace: StandardWorkspace,
    targetPermissionType: Permission.PermissionType = WORKSPACE_ADMIN,
  ) {
    val workspaceLevelInvitations = userInvitationService.getPendingInvitations(ScopeType.WORKSPACE, defaultWorkspace.workspaceId)
    workspaceLevelInvitations.forEach {
      when (it.permissionType) {
        WORKSPACE_EDITOR, WORKSPACE_RUNNER, WORKSPACE_READER -> {
          logger.debug { "Degrading user invitation id ${it.id} from permission type ${it.permissionType} to $targetPermissionType" }
          it.permissionType = targetPermissionType
        }
        else -> logger.debug { "No degradation needed for user invitation id ${it.id} with permission type ${it.permissionType}" }
      }
    }
    userInvitationService.updateUserInvitations(workspaceLevelInvitations)

    val orgLevelInvitations = userInvitationService.getPendingInvitations(ScopeType.ORGANIZATION, organizationId.value)
    orgLevelInvitations.forEach {
      when (it.permissionType) {
        ORGANIZATION_EDITOR, ORGANIZATION_RUNNER, ORGANIZATION_READER, ORGANIZATION_MEMBER -> {
          logger.debug {
            "Degrading user invitation id ${it.id} from permission type ${it.permissionType} to $targetPermissionType for workspace ${defaultWorkspace.workspaceId}"
          }
          it.scopeType = ScopeType.WORKSPACE
          it.scopeId = defaultWorkspace.workspaceId
          it.permissionType = targetPermissionType
        }
        else -> logger.debug { "No degradation needed for user invitation id ${it.id} with permission type ${it.permissionType}" }
      }
    }
    userInvitationService.updateUserInvitations(orgLevelInvitations)
  }
}
