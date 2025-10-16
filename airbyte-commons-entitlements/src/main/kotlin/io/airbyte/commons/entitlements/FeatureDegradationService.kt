/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.config.Permission
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_EDITOR
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_MEMBER
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_READER
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_RUNNER
import io.airbyte.config.Permission.PermissionType.WORKSPACE_ADMIN
import io.airbyte.config.Permission.PermissionType.WORKSPACE_EDITOR
import io.airbyte.config.Permission.PermissionType.WORKSPACE_READER
import io.airbyte.config.Permission.PermissionType.WORKSPACE_RUNNER
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.EntitlementPlan.STANDARD
import io.airbyte.domain.models.EntitlementPlan.STANDARD_TRIAL
import io.airbyte.domain.models.EntitlementPlan.UNIFIED_TRIAL
import io.airbyte.domain.models.OrganizationId
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
open class FeatureDegradationService(
  private val permissionService: PermissionService,
  private val workspacePersistence: WorkspacePersistence,
) {
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
}
