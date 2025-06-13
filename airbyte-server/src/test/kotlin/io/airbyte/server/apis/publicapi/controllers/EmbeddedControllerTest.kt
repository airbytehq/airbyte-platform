/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationReadList
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.auth.config.TokenExpirationConfig
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.config.Permission.PermissionType
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.publicApi.server.generated.models.EmbeddedOrganizationsList
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
            every { currentUser } returns AuthenticatedUser().withAuthUserId("user-id-1")
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
  fun listEmbeddedOrganizations_filtersByEntitlement_andPermission() {
    val entitledOrgId = UUID.randomUUID()
    val notEntitledOrgId = UUID.randomUUID()
    val userId = UUID.randomUUID()

    val currentUser = AuthenticatedUser().withUserId(userId)

    val ctrl =
      EmbeddedController(
        jwtTokenGenerator = mockk(),
        tokenExpirationConfig = TokenExpirationConfig(),
        currentUserService =
          mockk {
            every { getCurrentUser() } returns currentUser
          },
        organizationsHandler =
          mockk {
            every {
              listOrganizationsByUser(any())
            } returns
              OrganizationReadList().organizations(
                mutableListOf(
                  OrganizationRead()
                    .organizationId(entitledOrgId)
                    .organizationName("Entitled Org")
                    .email("entitled@airbyte.io")
                    .ssoRealm("entitled"),
                  OrganizationRead()
                    .organizationId(notEntitledOrgId)
                    .organizationName("Not Entitled Org")
                    .email("not-entitled@airbyte.io")
                    .ssoRealm("notentitled"),
                ),
              )
          },
        permissionHandler =
          mockk {
            every { listPermissionsForUser(userId) } returns
              listOf(
                Permission()
                  .withOrganizationId(entitledOrgId)
                  .withPermissionType(PermissionType.ORGANIZATION_ADMIN),
              )
          },
        embeddedWorkspacesHandler = mockk(),
        licenseEntitlementChecker =
          mockk {
            every { checkEntitlements(entitledOrgId, Entitlement.CONFIG_TEMPLATE_ENDPOINTS) } returns true
            every { checkEntitlements(notEntitledOrgId, Entitlement.CONFIG_TEMPLATE_ENDPOINTS) } returns false
          },
        airbyteUrl = "http://my.airbyte.com/",
        tokenIssuer = "test-issuer",
      )

    val res = ctrl.listEmbeddedOrganizationsByUser()

    val data = res.entity as EmbeddedOrganizationsList
    val result = data.organizations

    assertEquals(1, result.size)
    val item = result.first()
    assertEquals(entitledOrgId, item.organizationId)
    assertEquals("Entitled Org", item.organizationName)
    assertEquals(PermissionType.ORGANIZATION_ADMIN.toString(), item.permission.toString())
  }
}
