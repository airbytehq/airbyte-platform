/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationReadList
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.ConfigTemplateEntitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.config.Permission.PermissionType
import io.airbyte.domain.models.OrganizationId
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.publicApi.server.generated.models.EmbeddedOrganizationsList
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class EmbeddedControllerTest {
  @Test
  fun listEmbeddedOrganizations_includesAllOrgsWithPermissions() {
    val userIdString = UUID.randomUUID()
    val orgId1 = UUID.randomUUID()
    val orgId2 = UUID.randomUUID()
    val orgId3 = UUID.randomUUID()

    val controller =
      buildController(
        userIdString,
        listOf(
          OrganizationRead().organizationId(orgId1).organizationName("Org 1"),
          OrganizationRead().organizationId(orgId2).organizationName("Org 2"),
          OrganizationRead().organizationId(orgId3).organizationName("Org 3"),
        ),
        mockk {
          every { listPermissionsForUser(userIdString) } returns
            listOf(
              Permission()
                .withOrganizationId(orgId1)
                .withPermissionType(PermissionType.ORGANIZATION_ADMIN),
              Permission()
                .withOrganizationId(orgId2)
                .withPermissionType(PermissionType.ORGANIZATION_READER),
            )
          every { isUserInstanceAdmin(userIdString) } returns false
        },
        mockk {
          every { checkEntitlement(any(), ConfigTemplateEntitlement) } returns
            EntitlementResult(
              featureId = ConfigTemplateEntitlement.featureId,
              true,
              null,
              ConfigTemplateEntitlement.featureId,
            )
        },
      )
    val response = controller.listEmbeddedOrganizationsByUser()
    val orgs = response.entity as EmbeddedOrganizationsList

    // Should return all organizations for which the user has permissions
    assertEquals(2, orgs.organizations.size)

    val orgIds = orgs.organizations.map { it.organizationId }.toSet()
    assertEquals(setOf(orgId1, orgId2), orgIds)

    // Verify correct permission types are assigned
    val org1 = orgs.organizations.find { it.organizationId == orgId1 }
    val org2 = orgs.organizations.find { it.organizationId == orgId2 }

    assertEquals(io.airbyte.publicApi.server.generated.models.PermissionType.ORGANIZATION_ADMIN, org1?.permission)
    assertEquals(io.airbyte.publicApi.server.generated.models.PermissionType.ORGANIZATION_READER, org2?.permission)
  }

  @Test
  fun listEmbeddedOrganizations_filtersOutOrgsWithNoPermissions() {
    val userId = UUID.randomUUID()
    val orgId1 = UUID.randomUUID()
    val orgId2 = UUID.randomUUID()

    val controller =
      buildController(
        userId,
        listOf(
          OrganizationRead().organizationId(orgId1).organizationName("Org 1"),
          OrganizationRead().organizationId(orgId2).organizationName("Org 2"),
        ),
        mockk {
          every { listPermissionsForUser(userId) } returns
            listOf(
              Permission()
                .withOrganizationId(orgId1)
                .withPermissionType(PermissionType.ORGANIZATION_EDITOR),
            )
          every { isUserInstanceAdmin(userId) } returns false
        },
        mockk {
          every { checkEntitlement(any(), ConfigTemplateEntitlement) } returns
            EntitlementResult(
              featureId = ConfigTemplateEntitlement.featureId,
              true,
              null,
              ConfigTemplateEntitlement.featureId,
            )
        },
      )

    val response = controller.listEmbeddedOrganizationsByUser()
    val orgs = response.entity as EmbeddedOrganizationsList

    // Should only return organizations where the user has permissions
    assertEquals(1, orgs.organizations.size)
    assertEquals(orgId1, orgs.organizations[0].organizationId)
    assertEquals("Org 1", orgs.organizations[0].organizationName)
    assertEquals(io.airbyte.publicApi.server.generated.models.PermissionType.ORGANIZATION_EDITOR, orgs.organizations[0].permission)
  }

  @Test
  fun listEmbeddedOrganizations_filtersOutNonEntitledOrgs() {
    val userId = UUID.randomUUID()
    val entitledOrgId = UUID.randomUUID()
    val nonEntitledOrgId = UUID.randomUUID()

    val controller =
      buildController(
        userId,
        listOf(
          OrganizationRead().organizationId(entitledOrgId).organizationName("Entitled Org"),
          OrganizationRead().organizationId(nonEntitledOrgId).organizationName("Non-Entitled Org"),
        ),
        mockk {
          every { listPermissionsForUser(userId) } returns
            listOf(
              Permission()
                .withOrganizationId(entitledOrgId)
                .withPermissionType(PermissionType.ORGANIZATION_ADMIN),
              Permission()
                .withOrganizationId(nonEntitledOrgId)
                .withPermissionType(PermissionType.ORGANIZATION_ADMIN),
            )
          every { isUserInstanceAdmin(userId) } returns false
        },
        mockk {
          every { checkEntitlement(OrganizationId(entitledOrgId), ConfigTemplateEntitlement) } returns
            EntitlementResult(
              featureId = ConfigTemplateEntitlement.featureId,
              true,
              null,
              ConfigTemplateEntitlement.featureId,
            )
          every { checkEntitlement(OrganizationId(nonEntitledOrgId), ConfigTemplateEntitlement) } returns
            EntitlementResult(
              featureId = ConfigTemplateEntitlement.featureId,
              false,
              null,
              ConfigTemplateEntitlement.featureId,
            )
        },
      )

    val response = controller.listEmbeddedOrganizationsByUser()
    val orgs = response.entity as EmbeddedOrganizationsList

    // Should only return organizations that are entitled
    assertEquals(1, orgs.organizations.size)
    assertEquals(entitledOrgId, orgs.organizations[0].organizationId)
  }

  @Test
  fun listEmbeddedOrganizations_handlesNullOrganizationId() {
    val userId = UUID.randomUUID()
    val orgId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val controller =
      buildController(
        userId,
        listOf(
          OrganizationRead().organizationId(orgId).organizationName("Org 1"),
        ),
        mockk {
          every { listPermissionsForUser(userId) } returns
            listOf(
              // Workspace permission with null organizationId appears first
              Permission()
                .withWorkspaceId(workspaceId)
                .withPermissionType(PermissionType.WORKSPACE_ADMIN),
              Permission()
                .withOrganizationId(orgId)
                .withPermissionType(PermissionType.ORGANIZATION_ADMIN),
            )
          every { isUserInstanceAdmin(userId) } returns false
        },
        mockk {
          every { checkEntitlement(any(), ConfigTemplateEntitlement) } returns
            EntitlementResult(
              featureId = ConfigTemplateEntitlement.featureId,
              true,
              null,
              ConfigTemplateEntitlement.featureId,
            )
        },
      )

    val response = controller.listEmbeddedOrganizationsByUser()
    val orgs = response.entity as EmbeddedOrganizationsList

    // Should handle null organizationId properly and still return org with permission
    assertEquals(1, orgs.organizations.size)
    assertEquals(orgId, orgs.organizations[0].organizationId)
  }

  @Test
  fun listEmbeddedOrganizations_returnsEmptyListWhenNoMatchingPermissions() {
    val userId = UUID.randomUUID()
    val orgId = UUID.randomUUID()
    val otherOrgId = UUID.randomUUID()

    val controller =
      buildController(
        userId,
        listOf(
          OrganizationRead().organizationId(orgId).organizationName("Org 1"),
        ),
        mockk {
          every { listPermissionsForUser(userId) } returns
            listOf(
              Permission()
                .withOrganizationId(otherOrgId) // Permission for a different org
                .withPermissionType(PermissionType.ORGANIZATION_ADMIN),
            )
          every { isUserInstanceAdmin(userId) } returns false
        },
        mockk {
          every { checkEntitlement(any(), ConfigTemplateEntitlement) } returns
            EntitlementResult(
              featureId = ConfigTemplateEntitlement.featureId,
              true,
              null,
              ConfigTemplateEntitlement.featureId,
            )
        },
      )

    val response = controller.listEmbeddedOrganizationsByUser()
    val orgs = response.entity as EmbeddedOrganizationsList

    // Should return empty list when no matching permissions
    assertEquals(0, orgs.organizations.size)
  }

  // Helper method to build controller with mocks for testing
  private fun buildController(
    userId: UUID,
    organizations: List<OrganizationRead>,
    permissionHandler: PermissionHandler,
    entitlementService: EntitlementService,
  ): EmbeddedController =
    EmbeddedController(
      mockk<JwtTokenGenerator>(),
      mockk<AirbyteAuthConfig>(),
      mockk<CurrentUserService> {
        every { getCurrentUser() } returns
          AuthenticatedUser()
            .withUserId(userId)
            .withAuthUserId(userId.toString())
      },
      mockk<OrganizationsHandler> {
        every {
          listOrganizationsByUser(
            match<ListOrganizationsByUserRequestBody> { it.userId == userId },
          )
        } returns OrganizationReadList().organizations(organizations)
      },
      permissionHandler,
      entitlementService,
    )
}
