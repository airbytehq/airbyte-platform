/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_ADMIN
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_EDITOR
import io.airbyte.config.Permission.PermissionType.ORGANIZATION_MEMBER
import io.airbyte.config.Permission.PermissionType.WORKSPACE_ADMIN
import io.airbyte.config.Permission.PermissionType.WORKSPACE_EDITOR
import io.airbyte.config.Permission.PermissionType.WORKSPACE_READER
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.EntitlementPlan.STANDARD
import io.airbyte.domain.models.EntitlementPlan.STANDARD_TRIAL
import io.airbyte.domain.models.EntitlementPlan.UNIFIED_TRIAL
import io.airbyte.domain.models.OrganizationId
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID

class FeatureDegradationServiceTest {
  private val permissionService = mockk<PermissionService>()
  private val workspacePersistence = mockk<WorkspacePersistence>()
  private val featureDegradationService = FeatureDegradationService(permissionService, workspacePersistence)
  private val featureDegradationServiceStubbed = spyk(featureDegradationService)

  private val orgId = UUID.randomUUID()
  private val organizationId = OrganizationId(orgId)
  private val workspace = StandardWorkspace().withWorkspaceId(UUID.randomUUID()).withOrganizationId(orgId)

  @Test
  fun downgradeRBACRoles() {
    every { workspacePersistence.getDefaultWorkspaceForOrganization(organizationId.value) } returns workspace
    every { permissionService.getPermissionsByWorkspaceId(workspace.workspaceId) } returns
      listOf(
        Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
        Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_EDITOR),
        Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_READER),
      )
    every { permissionService.getPermissionsByOrganizationId(organizationId.value) } returns
      listOf(
        Permission().withOrganizationId(organizationId.value).withPermissionType(ORGANIZATION_ADMIN),
        Permission().withOrganizationId(organizationId.value).withPermissionType(ORGANIZATION_EDITOR),
        Permission().withOrganizationId(organizationId.value).withPermissionType(ORGANIZATION_MEMBER),
      )
    every { permissionService.updatePermissions(any()) } just Runs

    featureDegradationService.downgradeRBACRoles(organizationId)
    verify { workspacePersistence.getDefaultWorkspaceForOrganization(organizationId.value) }
    verify {
      permissionService.updatePermissions(
        listOf(
          Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
          Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
          Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
        ),
      )
    }
    verify {
      permissionService.updatePermissions(
        listOf(
          Permission().withOrganizationId(organizationId.value).withPermissionType(ORGANIZATION_ADMIN),
          Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
          Permission().withWorkspaceId(workspace.workspaceId).withPermissionType(WORKSPACE_ADMIN),
        ),
      )
    }
  }
}
