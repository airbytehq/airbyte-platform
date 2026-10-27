/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.Tag
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.data.services.OrganizationDomainVerificationService
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.DomainVerificationMethod
import io.airbyte.domain.models.DomainVerificationStatus
import io.airbyte.domain.models.OrganizationDomainVerification
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.SsoConfigRetrieval
import io.airbyte.domain.models.SsoConfigStatus
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.SsoAuthorizationDataService
import io.airbyte.domain.services.sso.SsoConfigDomainService
import io.airbyte.server.apis.controllers.DomainVerificationApiController
import io.airbyte.server.apis.controllers.SSOConfigApiController
import io.airbyte.server.apis.controllers.TagController
import io.airbyte.server.handlers.TagHandler
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

abstract class RemainingSecurityMetadataAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `SSO and domain metadata reject unsupported action and tag update rejects a different saved workspace before business`() {
    val cases =
      listOf(
        Triple(
          "/api/v1/sso_config/create",
          """{"organizationId":"$ORGANIZATION","companyIdentifier":"fixture","clientId":"fixture","clientSecret":"fixture","discoveryUrl":"https://example.com","status":"draft"}""",
          403,
        ),
        Triple("/api/v1/domain_verifications/list_pending", "{}", 403),
        Triple("/api/v1/sso_config/delete", """{"organizationId":"$ORGANIZATION","companyIdentifier":"other-realm"}""", 500),
        Triple("/api/v1/tags/update", """{"tagId":"$TAG","workspaceId":"$OTHER_WORKSPACE","name":"fixture","color":"123456"}""", 404),
      )
    assertAll(
      cases.map { (path, body, status) ->
        Executable {
          RemainingOperationHttpSupport(
            cloud,
            listOf(SSOConfigApiController::class.java, DomainVerificationApiController::class.java, TagController::class.java),
            rules(),
          ).run(
            configure = { context, business ->
              business.forEach { instance ->
                when (instance) {
                  is EntitlementService -> every { instance.ensureEntitled(any(), any()) } just Runs
                  is SsoConfigDomainService -> {
                    every { instance.createAndStoreSsoConfig(any()) } just Runs
                    every { instance.deleteSsoConfig(ORGANIZATION, "other-realm") } just Runs
                  }
                  is OrganizationDomainVerificationService -> every { instance.findByStatus(DomainVerificationStatus.PENDING) } returns emptyList()
                  is TagHandler ->
                    every { instance.updateTag(match { it.tagId == TAG && it.workspaceId == OTHER_WORKSPACE }) } returns
                      Tag(tagId = TAG, workspaceId = OTHER_WORKSPACE, name = "fixture", color = "123456")
                }
              }
              val data = mockk<AuthorizationDataService>()
              every { data.organization(ORGANIZATION) } returns ORGANIZATION
              every { data.tagWorkspaceAndOrganization(TAG) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
              every { data.workspaceAndOrganization(OTHER_WORKSPACE) } returns WorkspaceAndOrganization(OTHER_WORKSPACE, UUID(0, 99))
              val grants = mockk<PermissionService>()
              every { grants.getPermissionsByAuthUserId("caller") } returns emptyList()
              context.registerSingleton(AuthorizationDataService::class.java, data)
              val sso = mockk<SsoAuthorizationDataService>()
              every { sso.savedRealm(ORGANIZATION) } returns Optional.of("fixture")
              context.registerSingleton(SsoAuthorizationDataService::class.java, sso)
              context.registerSingleton(
                OperationPermissions::class.java,
                OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
              )
            },
            check = { _, server, business ->
              val response =
                RemainingOperationHttpSupport.request(
                  server,
                  "POST",
                  path,
                  body,
                  if (path.contains("/tags/") ||
                    path.endsWith("/delete")
                  ) {
                    "internal"
                  } else {
                    "user"
                  },
                )
              assertEquals(status, response.statusCode(), "$path: ${response.body()}")
              RemainingOperationHttpSupport.requireNoBusiness(business)
            },
          )
        }
      },
    )
  }

  @Test
  fun `all sixteen security metadata operations execute expected rules and normal business responses`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, scenario.status) } })
  }

  @Test
  fun `all sixteen security metadata operations deny before entitlement DNS credential or tag work`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, granted = false) } })
  }

  @Test
  fun `security metadata rejects unsupported and scoped organization callers despite administrator labels`() {
    for (scenario in scenarios()) {
      exercise(scenario, 403, token = "workload", grant = Permission.PermissionType.INSTANCE_ADMIN)
      exercise(scenario, 403, token = "embedded", grant = Permission.PermissionType.INSTANCE_ADMIN)
    }
    for (scenario in scenarios().filter {
      !it.path.contains("/tags/")
    }) {
      exercise(scenario, 403, token = "scoped:$WORKSPACE", grant = Permission.PermissionType.INSTANCE_ADMIN)
    }
    val tag = scenarios().single { it.path == "/api/v1/tags/update" }
    exercise(tag, 200, token = "scoped:$WORKSPACE")
    exercise(tag, 403, token = "scoped:$OTHER_WORKSPACE", grant = Permission.PermissionType.INSTANCE_ADMIN)
  }

  @Test
  fun `saved verification tag and realm failures stop all business and preserve supported error behavior`() {
    exercise(scenarios().single { it.path.endsWith("/domain_verifications/check") }.copy(body = """{"domainVerificationId":"${UUID(0, 99)}"}"""), 404)
    exercise(
      scenarios()
        .single {
          it.path.endsWith("/tags/update")
        }.copy(body = """{"tagId":"${UUID(0, 99)}","workspaceId":"$WORKSPACE","name":"fixture","color":"123456"}"""),
      404,
    )
    val deletion = scenarios().single { it.path == "/api/v1/sso_config/delete" }
    exercise(deletion, 500, missingRealm = true)
    exercise(
      deletion.copy(body = """{"organizationId":"$ORGANIZATION","companyIdentifier":"other-realm"}"""),
      500,
      grant = Permission.PermissionType.INSTANCE_ADMIN,
    )
    exercise(deletion, 500, lookupError = true)
    exercise(
      scenarios()
        .single {
          it.path == "/api/v1/tags/update"
        }.copy(
          body = """{"tagId":"$TAG","workspaceId":"$OTHER_WORKSPACE","name":"fixture","color":"123456"}""",
        ),
      404,
      grant = Permission.PermissionType.INSTANCE_ADMIN,
    )
  }

  @Test
  fun `no auth preserves security metadata contracts and skips permission lookups`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, scenario.status, security = false, granted = false) } })
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    granted: Boolean = true,
    grant: Permission.PermissionType = scenario.grant,
    token: String = "user",
    security: Boolean = true,
    missingRealm: Boolean = false,
    lookupError: Boolean = false,
  ) {
    val data = mockk<AuthorizationDataService>()
    val sso = mockk<SsoAuthorizationDataService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(
      cloud,
      listOf(SSOConfigApiController::class.java, DomainVerificationApiController::class.java, TagController::class.java),
      rules(),
      security,
    ).run(
      configure = { context, business ->
        val verification = verification()
        business.forEach { instance ->
          when (instance) {
            is EntitlementService -> every { instance.ensureEntitled(OrganizationId(ORGANIZATION), any()) } just Runs
            is SsoConfigDomainService -> {
              every { instance.retrieveSsoConfig(ORGANIZATION) } returns
                SsoConfigRetrieval("fixture", "fixture", "fixture", emptyList(), SsoConfigStatus.DRAFT)
              every { instance.createAndStoreSsoConfig(match { it.organizationId == ORGANIZATION && it.companyIdentifier == "fixture" }) } just Runs
              every { instance.deleteSsoConfig(ORGANIZATION, "fixture") } just Runs
              every { instance.updateClientCredentials(match { it.organizationId == ORGANIZATION && it.clientId == "fixture" }) } just Runs
              every { instance.activateSsoConfig(ORGANIZATION, "fixture.example.com") } just Runs
              every { instance.validateToken(ORGANIZATION, "fixture") } just Runs
            }
            is OrganizationDomainVerificationService -> {
              every { instance.createDomainVerification(ORGANIZATION, "fixture.example.com", USER) } returns verification
              every { instance.findByOrganizationId(ORGANIZATION) } returns listOf(verification)
              every { instance.findByStatus(DomainVerificationStatus.PENDING) } returns emptyList()
              every { instance.getDomainVerification(VERIFICATION) } returns verification
              every { instance.checkAndUpdateVerification(VERIFICATION) } returns verification
              every { instance.deleteDomainVerification(VERIFICATION) } just Runs
              every { instance.resetDomainVerification(VERIFICATION) } returns verification
            }
            is CurrentUserService -> every { instance.getCurrentUser() } returns AuthenticatedUser().withUserId(USER)
            is RoleResolver -> {
              val request = mockk<RoleResolver.Request>()
              every { instance.newRequest() } returns request
              every { request.withCurrentAuthentication() } returns request
              every { request.withRef(AuthenticationId.ORGANIZATION_ID, ORGANIZATION) } returns request
              every { request.requireRole(AuthRoleConstants.ORGANIZATION_ADMIN) } just Runs
            }
            is TagHandler -> {
              val tag = Tag(tagId = TAG, workspaceId = WORKSPACE, name = "fixture", color = "123456")
              every { instance.getTagsForWorkspaces(listOf(WORKSPACE)) } returns listOf(tag)
              every { instance.createTag(match { it.workspaceId == WORKSPACE && it.name == "fixture" }) } returns tag
              every { instance.updateTag(match { it.tagId == TAG && it.workspaceId == WORKSPACE }) } returns tag
              every { instance.deleteTag(match { it.tagId == TAG && it.workspaceId == WORKSPACE }) } just Runs
            }
          }
        }
        every { data.organization(ORGANIZATION) } returns ORGANIZATION
        every { data.workspaceAndOrganization(WORKSPACE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.tagWorkspaceAndOrganization(any()) } throws AuthorizationResourceNotFoundException()
        every { data.tagWorkspaceAndOrganization(TAG) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.domainVerificationOrganization(any()) } throws AuthorizationResourceNotFoundException()
        every { data.domainVerificationOrganization(VERIFICATION) } returns ORGANIZATION
        every { sso.savedRealm(ORGANIZATION) } returns if (missingRealm) Optional.empty() else Optional.of("fixture")
        if (lookupError) every { sso.savedRealm(ORGANIZATION) } throws java.io.IOException("saved realm unavailable")
        val permissions =
          if (!granted) {
            emptyList()
          } else {
            listOf(
              Permission().withPermissionType(grant).also {
                if (grant.name.startsWith("ORGANIZATION")) it.organizationId = ORGANIZATION
                if (grant.name.startsWith("WORKSPACE")) it.workspaceId = WORKSPACE
              },
            )
          }
        every { grants.getPermissionsByAuthUserId("caller") } returns permissions
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns permissions
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(SsoAuthorizationDataService::class.java, sso)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, scenario.rule.name)
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected != scenario.status) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (expected == 200 && !scenario.emptyResponse) assertTrue(response.body().isNotBlank(), scenario.path)
        if (scenario.rule != SsoDeleteRule::class.java) verify(exactly = 0) { sso.savedRealm(any()) }
        if (!security) {
          verify(exactly = 0) { grants.getPermissionsByAuthUserId(any()) }
          verify(exactly = 0) { grants.getPermissionsByServiceAccountId(any()) }
        }
      },
    )
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: Class<*>,
    val grant: Permission.PermissionType = Permission.PermissionType.ORGANIZATION_ADMIN,
    val status: Int = 200,
    val emptyResponse: Boolean = false,
  )

  private fun scenarios(): List<Scenario> {
    val org = """{"organizationId":"$ORGANIZATION"}"""
    val verification = """{"domainVerificationId":"$VERIFICATION"}"""
    return listOf(
      Scenario("/api/v1/sso_config/get", org, SsoAdminRule::class.java),
      Scenario(
        "/api/v1/sso_config/create",
        """{"organizationId":"$ORGANIZATION","companyIdentifier":"fixture","clientId":"fixture","clientSecret":"fixture","discoveryUrl":"https://example.com","status":"draft"}""",
        SsoAdminRule::class.java,
        emptyResponse = true,
      ),
      Scenario(
        "/api/v1/sso_config/delete",
        """{"organizationId":"$ORGANIZATION","companyIdentifier":"fixture"}""",
        SsoDeleteRule::class.java,
        emptyResponse = true,
      ),
      Scenario(
        "/api/v1/sso_config/update_credentials",
        """{"organizationId":"$ORGANIZATION","clientId":"fixture","clientSecret":"fixture"}""",
        SsoAdminRule::class.java,
        emptyResponse = true,
      ),
      Scenario(
        "/api/v1/sso_config/activate",
        """{"organizationId":"$ORGANIZATION","emailDomain":"fixture.example.com"}""",
        SsoAdminRule::class.java,
        emptyResponse = true,
      ),
      Scenario(
        "/api/v1/sso_config/validate_token",
        """{"organizationId":"$ORGANIZATION","accessToken":"fixture"}""",
        SsoAdminRule::class.java,
        emptyResponse = true,
      ),
      Scenario(
        "/api/v1/domain_verifications/create",
        """{"organizationId":"$ORGANIZATION","domain":"fixture.example.com"}""",
        DomainVerificationOrganizationRule::class.java,
      ),
      Scenario("/api/v1/domain_verifications/list", org, DomainVerificationOrganizationRule::class.java),
      Scenario("/api/v1/domain_verifications/list_pending", "{}", GlobalAdminRule::class.java, Permission.PermissionType.INSTANCE_ADMIN),
      Scenario("/api/v1/domain_verifications/check", verification, SavedDomainVerificationRule::class.java),
      Scenario("/api/v1/domain_verifications/delete", verification, SavedDomainVerificationRule::class.java, emptyResponse = true),
      Scenario("/api/v1/domain_verifications/reset", verification, SavedDomainVerificationRule::class.java),
      Scenario("/api/v1/tags/list", """{"workspaceId":"$WORKSPACE"}""", TagReadRule::class.java, Permission.PermissionType.WORKSPACE_READER),
      Scenario(
        "/api/v1/tags/create",
        """{"workspaceId":"$WORKSPACE","name":"fixture","color":"123456"}""",
        TagWriteRule::class.java,
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
      ),
      Scenario(
        "/api/v1/tags/update",
        """{"tagId":"$TAG","workspaceId":"$WORKSPACE","name":"fixture","color":"123456"}""",
        TagWriteRule::class.java,
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
      ),
      Scenario(
        "/api/v1/tags/delete",
        """{"tagId":"$TAG","workspaceId":"$WORKSPACE"}""",
        TagWriteRule::class.java,
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
        emptyResponse = true,
      ),
    )
  }

  private fun rules() =
    setOf(
      SsoAdminRule::class.java.name,
      SsoDeleteRule::class.java.name,
      DomainVerificationOrganizationRule::class.java.name,
      SavedDomainVerificationRule::class.java.name,
      GlobalAdminRule::class.java.name,
      TagReadRule::class.java.name,
      TagWriteRule::class.java.name,
    )

  private fun verification(): OrganizationDomainVerification {
    val time = OffsetDateTime.parse("2026-10-01T00:00:00Z")
    return OrganizationDomainVerification(
      VERIFICATION,
      ORGANIZATION,
      "fixture.example.com",
      DomainVerificationMethod.DNS_TXT,
      DomainVerificationStatus.PENDING,
      "fixture",
      "_airbyte.fixture.example.com",
      "fixture",
      0,
      time,
      time.plusDays(1),
      USER,
      time,
      time,
      time,
    )
  }

  companion object {
    val ORGANIZATION: UUID = UUID(0, 4)
    val WORKSPACE: UUID = UUID(0, 3)
    val OTHER_WORKSPACE: UUID = UUID(0, 12)
    val TAG: UUID = UUID(0, 14)
    val VERIFICATION: UUID = UUID(0, 15)
    val USER: UUID = UUID(0, 8)
  }
}
