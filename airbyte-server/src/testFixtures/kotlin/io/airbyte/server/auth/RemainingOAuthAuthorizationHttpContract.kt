/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.CompleteOAuthResponse
import io.airbyte.api.server.generated.models.OAuthConsentRead
import io.airbyte.api.server.generated.models.SourceOAuthScopesRead
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.OAuthHandler
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.DestinationOauthApiController
import io.airbyte.server.apis.controllers.SourceOauthApiController
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.UUID

abstract class RemainingOAuthAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `OAuth operations reject unsupported callers before reading or writing OAuth credentials`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `OAuth existing actor override must belong to supplied workspace and definition before credential work`() {
    assertAll(scenarios().filter { it.actor }.map { scenario -> Executable { exercise(scenario, 400, foreignActor = true, declaration = false) } })
  }

  @Test
  fun `all nine OAuth operations declare exact per kind flow or global instance policy and execute normal responses`() {
    for (scenario in scenarios()) {
      exercise(scenario, 200)
      exercise(scenario, 403, granted = false)
      exercise(scenario, 403, grant = Permission.PermissionType.WORKSPACE_READER)
      exercise(scenario, 403, token = "scoped:${UUID(0, 99)}", grant = Permission.PermissionType.INSTANCE_ADMIN)
      exercise(scenario, 404, missing = true)
      exercise(scenario, 500, lookupError = true)
      exercise(scenario, 200, granted = false, security = false)
      if (!scenario.instance) {
        exercise(scenario, 200, token = "scoped:$WORKSPACE")
        exercise(scenario, 403, usable = false)
        exercise(scenario, if (scenario.source) 200 else 403, token = "embedded-scoped:$WORKSPACE", granted = false)
        exercise(scenario, 403, token = "embedded-scoped:${UUID(0, 99)}", granted = false)
        exercise(
          scenario,
          403,
          grant =
            if (scenario.source) {
              Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR
            } else {
              Permission.PermissionType.WORKSPACE_SOURCE_EDITOR
            },
        )
      }
    }
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    grant: Permission.PermissionType =
      if (scenario.instance) {
        Permission.PermissionType.INSTANCE_ADMIN
      } else if (scenario.source) {
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR
      } else {
        Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR
      },
    granted: Boolean = true,
    foreignActor: Boolean = false,
    missing: Boolean = false,
    lookupError: Boolean = false,
    usable: Boolean = true,
    security: Boolean = true,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(
      cloud,
      listOf(
        SourceOauthApiController::class.java,
        DestinationOauthApiController::class.java,
      ),
      scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet(),
      security,
    ).run(
      configure = { context, business ->
        val handler = business.filterIsInstance<OAuthHandler>().single()
        val completed = CompleteOAuthResponse(requestSucceeded = true, authPayload = mapOf("fixture" to "credential"))
        every { handler.completeSourceOAuthHandleReturnSecret(match { it.workspaceId == WORKSPACE && it.sourceDefinitionId == DEFINITION }) } returns
          completed
        every { handler.completeDestinationOAuth(match { it.workspaceId == WORKSPACE && it.destinationDefinitionId == DEFINITION }) } returns
          completed
        every { handler.getSourceOAuthConsent(match { it.workspaceId == WORKSPACE && it.sourceDefinitionId == DEFINITION }) } returns
          OAuthConsentRead(consentUrl = "https://example.com/consent")
        every { handler.getDestinationOAuthConsent(match { it.workspaceId == WORKSPACE && it.destinationDefinitionId == DEFINITION }) } returns
          OAuthConsentRead(consentUrl = "https://example.com/consent")
        every {
          handler.revokeSourceOauthTokens(match { it.sourceId == ACTOR && it.workspaceId == WORKSPACE && it.sourceDefinitionId == DEFINITION })
        } just Runs
        every { handler.getSourceOAuthScopes(match { it.workspaceId == WORKSPACE && it.sourceDefinitionId == DEFINITION }) } returns
          SourceOAuthScopesRead(scopes = emptyList(), optionalScopes = emptyList())
        every { handler.setSourceInstancewideOauthParams(match { it.sourceDefinitionId == DEFINITION }) } just Runs
        every { handler.setDestinationInstancewideOauthParams(match { it.destinationDefinitionId == DEFINITION }) } just Runs
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        every { data.requireActorDefinitionType(DEFINITION, scenario.source) } just Runs
        every { data.canUseActorDefinition(DEFINITION, owner) } returns usable
        every { data.savedSourceWorkspaceAndOrganization(ACTOR) } returns
          if (foreignActor) {
            WorkspaceAndOrganization(
              UUID(
                0,
                99,
              ),
              ORGANIZATION,
            )
          } else {
            owner
          }
        every { data.savedDestinationWorkspaceAndOrganization(ACTOR) } returns
          if (foreignActor) {
            WorkspaceAndOrganization(
              UUID(
                0,
                99,
              ),
              ORGANIZATION,
            )
          } else {
            owner
          }
        every { data.savedActorDefinitionId(ACTOR, WORKSPACE) } returns DEFINITION
        if (missing) every { data.requireActorDefinitionType(DEFINITION, scenario.source) } throws AuthorizationResourceNotFoundException()
        if (lookupError) {
          every {
            data.requireActorDefinitionType(
              DEFINITION,
              scenario.source,
            )
          } throws java.io.IOException("OAuth definition unavailable")
        }
        val permission = Permission().withPermissionType(grant)
        if (grant.name.startsWith("WORKSPACE")) permission.workspaceId = WORKSPACE
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(
            grants,
            RoleResolver(
              mockk(),
              mockk(),
              mockk(),
              mockk(),
            ),
            security,
          ),
        )
      },
      check = { context, server, business ->
        if (declaration) RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, "io.airbyte.server.auth.${scenario.rule}")
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${server.url}${scenario.path}: ${response.headers().map()} ${response.body()}")
        if (expected != 200) RemainingOperationHttpSupport.requireNoBusiness(business)
      },
    )
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: String,
    val source: Boolean = true,
    val actor: Boolean = true,
    val instance: Boolean = false,
  )

  private fun scenarios(): List<Scenario> {
    val source =
      """{"workspaceId":"$WORKSPACE","sourceDefinitionId":"$DEFINITION","sourceId":"$ACTOR",""" +
        """"redirectUrl":"https://example.com/redirect","queryParams":{},"oAuthInputConfiguration":{}}"""
    val dest = source.replace("source", "destination")
    return listOf(
      Scenario("/api/v1/source_oauths/complete_oauth", source, "SourceOAuthFlowRule"),
      Scenario("/api/v1/source_oauths/get_consent_url", source, "SourceOAuthFlowRule"),
      Scenario(
        "/api/v1/source_oauths/get_embedded_consent_url",
        """{"workspaceId":"$WORKSPACE","sourceDefinitionId":"$DEFINITION","sourceId":"$ACTOR","redirectUrl":"https://example.com/redirect"}""",
        "SourceOAuthFlowRule",
      ),
      Scenario(
        "/api/v1/source_oauths/revoke",
        """{"workspaceId":"$WORKSPACE","sourceDefinitionId":"$DEFINITION","sourceId":"$ACTOR"}""",
        "SourceOAuthFlowRule",
      ),
      Scenario(
        "/api/v1/source_oauths/get_scopes",
        """{"workspaceId":"$WORKSPACE","sourceDefinitionId":"$DEFINITION"}""",
        "SourceOAuthFlowRule",
        actor = false,
      ),
      Scenario(
        "/api/v1/source_oauths/oauth_params/create",
        """{"sourceDefinitionId":"$DEFINITION","params":{}}""",
        "SourceOAuthInstanceRule",
        actor = false,
        instance = true,
      ),
      Scenario("/api/v1/destination_oauths/complete_oauth", dest, "DestinationOAuthFlowRule", source = false),
      Scenario("/api/v1/destination_oauths/get_consent_url", dest, "DestinationOAuthFlowRule", source = false),
      Scenario(
        "/api/v1/destination_oauths/oauth_params/create",
        """{"destinationDefinitionId":"$DEFINITION","params":{}}""",
        "DestinationOAuthInstanceRule",
        source = false,
        actor = false,
        instance = true,
      ),
    )
  }

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val DEFINITION: UUID = UUID(0, 3)
    val ACTOR: UUID = UUID(0, 4)
  }
}
