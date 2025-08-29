/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationReadList
import io.airbyte.commons.auth.config.TokenExpirationConfig
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.config.Permission.PermissionType
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.publicApi.server.generated.models.EmbeddedOrganizationsList
import io.airbyte.publicApi.server.generated.models.EmbeddedScopedTokenRequest
import io.airbyte.publicApi.server.generated.models.EmbeddedWidgetRequest
import io.airbyte.server.auth.TokenScopeClaim
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.util.Base64
import java.util.Optional
import java.util.UUID

class EmbeddedControllerTest {
  @Test
  fun basic() {
    val organizationId = UUID.randomUUID()
    val workspaceId = WorkspaceId(UUID.randomUUID())
    val externalId = "cool customer"
    val claims = slot<Map<String, Any>>()

    val ctrl =
      EmbeddedController(
        jwtTokenGenerator =
          mockk {
            every { generateToken(capture(claims)) } returns Optional.of("mock-token")
          },
        tokenExpirationConfig = TokenExpirationConfig(),
        currentUserService =
          mockk {
            every { getCurrentUser() } returns AuthenticatedUser().withAuthUserId("user-id-1")
          },
        licenseEntitlementChecker =
          mockk {
            every {
              ensureEntitled(organizationId, Entitlement.CONFIG_TEMPLATE_ENDPOINTS)
            } returns Unit
          },
        airbyteUrl = "http://my.airbyte.com/",
        tokenIssuer = "test-token-issuer",
        embeddedWorkspacesHandler =
          mockk {
            every {
              getOrCreate(any(), externalId)
            } returns workspaceId
          },
        organizationsHandler =
          mockk {
            every {
              listOrganizationsByUser(ListOrganizationsByUserRequestBody())
            } returns
              OrganizationReadList().organizations(
                mutableListOf(
                  OrganizationRead()
                    .organizationId(organizationId)
                    .organizationName("Test Organization")
                    .email("test-email@airbyte.io")
                    .ssoRealm("test-realm"),
                ),
              )
          },
        permissionHandler =
          mockk {
            every {
              listPermissionsForUser(any())
            } returns
              listOf(
                Permission()
                  .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
                  .withOrganizationId(organizationId),
              )
          },
      )

    ctrl.clock = Clock.fixed(Instant.now(), ZoneId.of("UTC"))

    val res =
      ctrl.getEmbeddedWidget(
        EmbeddedWidgetRequest(
          organizationId = organizationId,
          allowedOrigin = "http://my.operator.com/",
          externalUserId = externalId,
        ),
      )

    assertEquals(
      mapOf(
        "iss" to "test-token-issuer",
        "aud" to "airbyte-server",
        "sub" to "user-id-1",
        "typ" to "io.airbyte.auth.embedded_v1",
        "act" to
          mapOf(
            "sub" to externalId,
          ),
        "io.airbyte.auth.workspace_scope" to TokenScopeClaim(workspaceId = workspaceId.value.toString()),
        "roles" to listOf(AuthRoleConstants.EMBEDDED_END_USER),
        "exp" to
          ctrl.clock
            .instant()
            .plusSeconds(60 * 15)
            .epochSecond,
      ),
      claims.captured,
    )

    assertEquals(200, res.status)

    val responseJson: Map<String, Any> = res.entity as Map<String, Any>
    val json = Base64.getDecoder().decode(responseJson["token"] as String).decodeToString()
    val data = Jsons.deserializeToMap(Jsons.deserialize(json))

    assertEquals(
      mapOf(
        // This is a dummy value because the JwtTokenGenerator is mocked above,
        // because the JwtTokenGenerator depends on micronaut security being enabled,
        // which is currently difficult to do in our test suite. But, the token claims
        // are verified above by capturing the call to jwtTokenGenerator.generateToken().
        "token" to "mock-token",
        "widgetUrl" to "http://my.airbyte.com/embedded-widget?workspaceId=${workspaceId.value}&allowedOrigin=http%3A%2F%2Fmy.operator.com%2F",
      ),
      data,
    )
  }

  @Test
  fun `scoped token`() {
    val organizationId = UUID.randomUUID()
    val workspaceId = WorkspaceId(UUID.randomUUID())
    val claims = slot<Map<String, Any>>()

    val ctrl =
      EmbeddedController(
        jwtTokenGenerator =
          mockk {
            every { generateToken(capture(claims)) } returns Optional.of("mock-token")
          },
        tokenExpirationConfig = TokenExpirationConfig(),
        currentUserService =
          mockk {
            every { getCurrentUser() } returns AuthenticatedUser().withAuthUserId("user-id-1")
          },
        licenseEntitlementChecker =
          mockk {
            every {
              ensureEntitled(organizationId, Entitlement.CONFIG_TEMPLATE_ENDPOINTS)
            } returns Unit
          },
        airbyteUrl = "http://my.airbyte.com/",
        tokenIssuer = "test-token-issuer",
        embeddedWorkspacesHandler =
          mockk {
            every {
              getOrCreate(any(), workspaceId.value.toString())
            } returns workspaceId
          },
        organizationsHandler =
          mockk {
            every {
              listOrganizationsByUser(ListOrganizationsByUserRequestBody())
            } returns
              OrganizationReadList().organizations(
                mutableListOf(
                  OrganizationRead()
                    .organizationId(organizationId)
                    .organizationName("Test Organization")
                    .email("test-email@airbyte.io")
                    .ssoRealm("test-realm"),
                ),
              )
          },
        permissionHandler =
          mockk {
            every {
              listPermissionsForUser(any())
            } returns
              listOf(
                Permission()
                  .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
                  .withOrganizationId(organizationId),
              )
          },
      )

    ctrl.clock = Clock.fixed(Instant.now(), ZoneId.of("UTC"))

    val res =
      ctrl.generateEmbeddedScopedToken(
        EmbeddedScopedTokenRequest(
          workspaceId = workspaceId.value,
        ),
      )

    assertEquals(
      mapOf(
        "iss" to "test-token-issuer",
        "aud" to "airbyte-server",
        "sub" to "user-id-1",
        "typ" to "io.airbyte.auth.embedded_v1",
        "act" to
          mapOf(
            "sub" to workspaceId.value.toString(),
          ),
        "io.airbyte.auth.workspace_scope" to TokenScopeClaim(workspaceId = workspaceId.value.toString()),
        "roles" to listOf(AuthRoleConstants.EMBEDDED_END_USER),
        "exp" to
          ctrl.clock
            .instant()
            .plusSeconds(60 * 15)
            .epochSecond,
      ),
      claims.captured,
    )

    assertEquals(200, res.status)

    val responseJson: Map<String, Any> = res.entity as Map<String, Any>

    assertEquals(
      mapOf(
        // This is a dummy value because the JwtTokenGenerator is mocked above,
        // because the JwtTokenGenerator depends on micronaut security being enabled,
        // which is currently difficult to do in our test suite. But, the token claims
        // are verified above by capturing the call to jwtTokenGenerator.generateToken().
        "token" to "mock-token",
      ),
      responseJson,
    )
  }

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
        },
        mockk {
          every { checkEntitlements(any(), any()) } returns true
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
        },
        mockk {
          every { checkEntitlements(any(), any()) } returns true
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
        },
        mockk {
          every { checkEntitlements(entitledOrgId, any()) } returns true
          every { checkEntitlements(nonEntitledOrgId, any()) } returns false
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
        },
        mockk {
          every { checkEntitlements(any(), any()) } returns true
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
        },
        mockk {
          every { checkEntitlements(any(), any()) } returns true
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
    licenseEntitlementChecker: LicenseEntitlementChecker,
  ): EmbeddedController =
    EmbeddedController(
      mockk(),
      mockk(),
      mockk {
        every { getCurrentUser() } returns
          AuthenticatedUser()
            .withUserId(userId)
            .withAuthUserId(userId.toString())
      },
      mockk {
        every {
          listOrganizationsByUser(
            match<ListOrganizationsByUserRequestBody> { it.userId == userId },
          )
        } returns OrganizationReadList().organizations(organizations)
      },
      permissionHandler,
      mockk(),
      licenseEntitlementChecker,
      "http://localhost:8000",
      "test-issuer",
    )
}
