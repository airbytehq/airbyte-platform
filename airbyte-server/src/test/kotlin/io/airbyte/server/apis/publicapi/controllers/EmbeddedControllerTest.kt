/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.auth.OrganizationAuthRole
import io.airbyte.commons.auth.config.TokenExpirationConfig
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.Scope
import io.airbyte.config.AuthenticatedUser
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
    val requestedWorkspaceId = UUID.randomUUID()
    val claims = slot<Map<String, Any>>()

    val ctrl =
      EmbeddedController(
        jwtTokenGenerator =
          mockk {
            every { generateToken(capture(claims)) } returns Optional.of("mock-token")
          },
        apiAuthorizationHelper =
          mockk {
            every {
              ensureUserHasAnyRequiredRoleOrThrow(
                Scope.ORGANIZATION,
                listOf(organizationId.toString()),
                setOf(OrganizationAuthRole.ORGANIZATION_ADMIN),
              )
            } returns Unit
          },
        tokenExpirationConfig = TokenExpirationConfig(),
        currentUserService =
          mockk {
            every { currentUser } returns AuthenticatedUser().withAuthUserId("user-id-1")
          },
        workspaceHelper =
          mockk {
            every {
              getOrganizationForWorkspace(requestedWorkspaceId)
            } returns organizationId
          },
        licenseEntitlementChecker =
          mockk {
            every {
              ensureEntitled(organizationId, Entitlement.CONFIG_TEMPLATE_ENDPOINTS)
            } returns Unit
          },
        airbyteUrl = "http://my.airbyte.com/",
        tokenIssuer = "test-token-issuer",
      )
    ctrl.clock = Clock.fixed(Instant.now(), ZoneId.of("UTC"))

    val res =
      ctrl.getEmbeddedWidget(
        EmbeddedWidgetRequest(
          workspaceId = requestedWorkspaceId,
          allowedOrigin = "http://my.operator.com/",
          externalUserId = "bob",
        ),
      )

    assertEquals(
      mapOf(
        "iss" to "test-token-issuer",
        "aud" to "airbyte-server",
        "sub" to "user-id-1",
        "typ" to "io.airbyte.embedded.v1",
        "act" to
          mapOf(
            "sub" to "bob",
          ),
        "io.airbyte.auth.workspace_scope" to TokenScopeClaim(workspaceId = requestedWorkspaceId.toString()),
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
        "widgetUrl" to "http://my.airbyte.com/embedded-widget?workspaceId=$requestedWorkspaceId&allowedOrigin=http%3A%2F%2Fmy.operator.com%2F",
      ),
      data,
    )
  }
}
