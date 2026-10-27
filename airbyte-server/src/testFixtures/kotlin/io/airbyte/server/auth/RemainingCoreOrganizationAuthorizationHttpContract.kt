/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.client.WebUrlHelper
import io.airbyte.api.server.generated.models.ListOrganizationSummariesResponse
import io.airbyte.api.server.generated.models.OrganizationInfoRead
import io.airbyte.api.server.generated.models.OrganizationRead
import io.airbyte.api.server.generated.models.OrganizationReadList
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.scim.ScimConfigurationRead
import io.airbyte.domain.models.scim.ScimConfigurationStatus
import io.airbyte.domain.models.scim.ScimIdpProvider
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.OrganizationAccessService
import io.airbyte.domain.services.scim.ScimAccessGate
import io.airbyte.domain.services.scim.ScimConfigurationService
import io.airbyte.server.apis.controllers.OrganizationApiController
import io.airbyte.server.apis.controllers.ScimConfigApiController
import io.airbyte.server.helpers.OrganizationAccessAuthorizationHelper
import io.mockk.Called
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
import java.util.UUID

abstract class RemainingCoreOrganizationAuthorizationHttpContract {
  protected open val cloud = false

  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `organization get rejects unsupported caller before legacy helper and business despite earlier allowed`() =
    exercise(scenarios().first(), "workload", 403)

  @Test
  fun `SCIM config disable rejects caller before audit identity and token changes`() =
    exercise(scenarios().single { it.path == "/api/v1/scim_config/disable" }, "workload", 403)

  @Test
  fun `global organization create requires an existing beneficiary before business`() =
    exercise(scenarios().single { it.path == "/api/v1/organizations/create" }, "internal", 404, missingBeneficiary = true)

  @Test
  fun `all core organization and Config SCIM operations execute independent rules with normal responses`() {
    assertAll(
      scenarios().map { scenario ->
        Executable {
          exercise(
            scenario,
            "user",
            if (!cloud &&
              scenario.ossUnimplemented
            ) {
              501
            } else {
              scenario.status
            },
          )
        }
      },
    )
  }

  @Test
  fun `OSS organization usage stubs preserve action checks without saved organization metadata`() {
    if (cloud) return
    assertAll(
      scenarios().filter { it.ossUnimplemented }.map { scenario ->
        Executable {
          exercise(scenario, "user", 501, missingOrganization = true, unusedOwner = true)
          exercise(scenario, "user", 501, lookupError = true, unusedOwner = true)
          exercise(scenario, "user", 501, missingOrganization = true, security = false, granted = false, unusedOwner = true)
          exercise(scenario, "user", 403, granted = false, unusedOwner = true)
          exercise(scenario, "workload", 403, unusedOwner = true)
          exercise(scenario, "scoped:${UUID(0, 1)}", 403, unusedOwner = true)
        }
      },
    )
  }

  @Test
  fun `OSS organization usage stubs preserve nullable no auth bodies`() {
    if (cloud) return
    assertAll(
      scenarios().filter { it.ossUnimplemented }.map { scenario ->
        Executable { exercise(scenario.copy(body = "null"), "user", 501, security = false, granted = false, unusedOwner = true) }
      },
    )
  }

  @Test
  fun `all core organization and Config SCIM operations reject before business after earlier allowed`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, "user", 403, granted = false) } })
  }

  @Test
  fun `both organization reads preserve accessible workspace alternative without direct organization permission`() {
    for (scenario in scenarios().filter { it.rule == OrganizationAccessibleRule::class.java }) {
      exercise(scenario, "user", 200, granted = false, workspaceAccess = true)
      exercise(scenario, "scoped:${UUID(0, 1)}", 403, workspaceAccess = true)
      exercise(scenario, "embedded", 403, workspaceAccess = true)
      exercise(scenario, "internal", 200, granted = false)
    }
  }

  @Test
  fun `user discovery uses saved authentication subjects and forbids unrelated reader grants`() {
    for (scenario in scenarios().filter { it.rule == OrganizationUserListRule::class.java }) {
      exercise(scenario, "user", 200, granted = false, self = true)
      exercise(scenario, "user", 403, grant = Permission.PermissionType.WORKSPACE_READER)
      exercise(scenario, "user", 403, grant = Permission.PermissionType.ORGANIZATION_READER)
    }
  }

  @Test
  fun `administrator discovery keeps empty results for missing or unlinked user filters`() {
    assertAll(
      scenarios().filter { it.rule == OrganizationUserListRule::class.java }.map { scenario ->
        Executable {
          exercise(scenario, "user", 200, missingBeneficiary = true)
          exercise(scenario, "internal", 200, granted = false, missingBeneficiary = true)
          exercise(scenario, "user", 200, security = false, granted = false, missingBeneficiary = true)
          exercise(scenario, "user", 500, permissionFailure = true)
          exercise(scenario, "user", 500, granted = false, userLookupFailure = true)
        }
      },
    )
  }

  @Test
  fun `organization usage needs its admin intent while SCIM needs organization admin`() {
    val usage = scenarios().single { it.path.endsWith("/get_usage") }
    exercise(usage, "user", 403, grant = Permission.PermissionType.ORGANIZATION_READER)
    for (scenario in scenarios().filter {
      it.path.contains("/scim_config/")
    }) {
      exercise(scenario, "user", 403, grant = Permission.PermissionType.ORGANIZATION_EDITOR)
    }
  }

  @Test
  fun `required saved organization and beneficiary failures never become global grants`() {
    exercise(scenarios().first(), "user", 404, missingOrganization = true)
    exercise(scenarios().first(), "user", 500, lookupError = true)
    exercise(
      scenarios().single {
        it.path.endsWith("/list_by_user_id")
      },
      "user",
      404,
      grant = Permission.PermissionType.WORKSPACE_READER,
      missingBeneficiary = true,
    )
    for (body in listOf("null", "{}", """{"organizationId":null}""")) exercise(scenarios().first().copy(body = body), "user", 400)
  }

  @Test
  fun `no auth still executes normal organization and SCIM contracts without permission queries`() {
    assertAll(
      scenarios().map { scenario ->
        Executable {
          exercise(
            scenario,
            "user",
            if (!cloud &&
              scenario.ossUnimplemented
            ) {
              501
            } else {
              scenario.status
            },
            granted = false,
            security = false,
          )
        }
      },
    )
  }

  private fun exercise(
    scenario: Scenario,
    token: String,
    expected: Int,
    grant: Permission.PermissionType = scenario.grant,
    granted: Boolean = true,
    self: Boolean = false,
    workspaceAccess: Boolean = false,
    missingOrganization: Boolean = false,
    missingBeneficiary: Boolean = false,
    lookupError: Boolean = false,
    permissionFailure: Boolean = false,
    userLookupFailure: Boolean = false,
    security: Boolean = true,
    unusedOwner: Boolean = false,
  ) {
    val controller =
      if (cloud) {
        Class.forName(
          "io.airbyte.server.wrapped.apis.controllers.OrganizationWrappedController",
        )
      } else {
        OrganizationApiController::class.java
      }
    val controllers = listOf(controller, OrganizationApiController::class.java, ScimConfigApiController::class.java).distinct()
    val grants = mockk<PermissionService>()
    val data = mockk<AuthorizationDataService>()
    val access = mockk<OrganizationAccessService>()
    RemainingOperationHttpSupport(cloud, controllers, scenarios().map { it.rule.name }.toSet(), security).run(
      configure = { context, business ->
        business.forEach { instance ->
          configureDependency(instance)
          when (instance) {
            is OrganizationAccessAuthorizationHelper -> every { instance.validateOrganizationOrWorkspaceAccess(ORGANIZATION) } just Runs
            is OrganizationsHandler -> {
              val organization = OrganizationRead(ORGANIZATION, "fixture", "fixture@example.com")
              every { instance.getOrganization(match { it.organizationId == ORGANIZATION }) } returns organization
              every { instance.getOrganizationInfo(ORGANIZATION) } returns OrganizationInfoRead(ORGANIZATION, "fixture", false, false)
              every { instance.updateOrganization(match { it.organizationId == ORGANIZATION && it.organizationName == "updated" }) } returns
                organization.copy(organizationName = "updated")
              every { instance.setOrganizationAgenticStatus(match { it.organizationId == ORGANIZATION && it.isAgentic }) } returns
                organization.copy(isAgentic = true)
              every { instance.createOrganization(match { it.userId == USER }) } returns organization
              every { instance.listOrganizationsByUser(match { it.userId == USER }) } returns OrganizationReadList(emptyList())
              every { instance.getOrganizationSummaries(match { it.userId == USER }) } returns ListOrganizationSummariesResponse(emptyList())
            }
            is ScimConfigurationService -> {
              every { instance.getConfiguration(OrganizationId(ORGANIZATION)) } returns ScimConfigurationRead(ScimConfigurationStatus.NOT_CONFIGURED)
              every { instance.enable(OrganizationId(ORGANIZATION), ScimIdpProvider.OKTA, UserId(USER)) } returns
                ScimConfigurationRead(ScimConfigurationStatus.ENABLED, ScimIdpProvider.OKTA, token = "fixture")
              every { instance.rotateToken(OrganizationId(ORGANIZATION), UserId(USER)) } returns
                ScimConfigurationRead(ScimConfigurationStatus.ENABLED, ScimIdpProvider.OKTA, token = "rotated-fixture")
              every { instance.disable(OrganizationId(ORGANIZATION), UserId(USER)) } just Runs
            }
            is ScimAccessGate -> every { instance.isAllowed(OrganizationId(ORGANIZATION)) } returns true
            is CurrentUserService -> every { instance.getCurrentUser() } returns AuthenticatedUser().withUserId(USER)
            is WebUrlHelper -> every { instance.baseUrl } returns "https://example.com"
          }
        }
        every { data.organization(ORGANIZATION) } returns ORGANIZATION
        if (missingOrganization) every { data.organization(ORGANIZATION) } throws AuthorizationResourceNotFoundException()
        if (lookupError) every { data.organization(ORGANIZATION) } throws java.io.IOException("organization ownership unavailable")
        every { data.userAuthSubjects(USER) } returns setOf(if (self) "caller" else "other-user")
        if (userLookupFailure) every { data.userAuthSubjects(USER) } throws java.io.IOException("user authentication ownership unavailable")
        every { data.requireUser(USER) } just Runs
        if (missingBeneficiary) {
          every { data.userAuthSubjects(USER) } throws AuthorizationResourceNotFoundException()
          every { data.requireUser(USER) } throws AuthorizationResourceNotFoundException()
        }
        every { access.hasAccessibleWorkspace("caller", ORGANIZATION) } returns workspaceAccess
        val permissions =
          if (granted) {
            listOf(
              Permission().withPermissionType(grant).also {
                if (grant.name.startsWith("ORGANIZATION")) it.organizationId = ORGANIZATION
                if (grant.name.startsWith("WORKSPACE")) it.workspaceId = UUID(0, 1)
              },
            )
          } else {
            emptyList()
          }
        every { grants.getPermissionsByAuthUserId("caller") } returns permissions
        if (permissionFailure) every { grants.getPermissionsByAuthUserId("caller") } throws java.io.IOException("permission lookup unavailable")
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns permissions
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(OrganizationAccessService::class.java, access)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, scenario.rule.name)
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected !in setOf(200, 204, 501)) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (expected == 200) assertTrue(response.body().isNotBlank(), scenario.path)
        if (!cloud && scenario.ossUnimplemented) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (unusedOwner) {
          verify { data wasNot Called }
          verify { access wasNot Called }
        }
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
    val grant: Permission.PermissionType,
    val status: Int = 200,
    val ossUnimplemented: Boolean = false,
  )

  private fun scenarios(): List<Scenario> {
    val id = """{"organizationId":"$ORGANIZATION"}"""
    return listOf(
      Scenario("/api/v1/organizations/get", id, OrganizationAccessibleRule::class.java, Permission.PermissionType.ORGANIZATION_MEMBER),
      Scenario(
        "/api/v1/organizations/get_organization_info",
        id,
        OrganizationAccessibleRule::class.java,
        Permission.PermissionType.ORGANIZATION_MEMBER,
      ),
      Scenario(
        "/api/v1/organizations/update",
        """{"organizationId":"$ORGANIZATION","organizationName":"updated"}""",
        OrganizationEditorRule::class.java,
        Permission.PermissionType.ORGANIZATION_EDITOR,
      ),
      Scenario(
        "/api/v1/organizations/agentic_status",
        """{"organizationId":"$ORGANIZATION","isAgentic":true}""",
        OrganizationInstanceAdminRule::class.java,
        Permission.PermissionType.INSTANCE_ADMIN,
      ),
      Scenario(
        "/api/v1/organizations/create",
        """{"userId":"$USER","organizationName":"fixture"}""",
        OrganizationCreateRule::class.java,
        Permission.PermissionType.INSTANCE_ADMIN,
      ),
      Scenario(
        "/api/v1/organizations/list_by_user_id",
        """{"userId":"$USER"}""",
        OrganizationUserListRule::class.java,
        Permission.PermissionType.INSTANCE_ADMIN,
      ),
      Scenario(
        "/api/v1/organizations/list_summaries",
        """{"userId":"$USER","nameContains":"","pagination":{"pageSize":20,"rowOffset":0}}""",
        OrganizationUserListRule::class.java,
        Permission.PermissionType.INSTANCE_ADMIN,
      ),
      Scenario(
        "/api/v1/organizations/get_usage",
        id,
        if (cloud) OrganizationUsageRule::class.java else OrganizationUsageUnsupportedRule::class.java,
        Permission.PermissionType.ORGANIZATION_ADMIN,
        ossUnimplemented = true,
      ),
      Scenario(
        "/api/v1/organizations/get_data_worker_usage",
        """{"organizationId":"$ORGANIZATION","startDate":"2026-10-01","endDate":"2026-10-02"}""",
        if (cloud) OrganizationMemberRule::class.java else OrganizationMemberUnsupportedRule::class.java,
        Permission.PermissionType.ORGANIZATION_MEMBER,
        ossUnimplemented = true,
      ),
      Scenario("/api/v1/scim_config/get", id, OrganizationAdminRule::class.java, Permission.PermissionType.ORGANIZATION_ADMIN),
      Scenario(
        "/api/v1/scim_config/enable",
        """{"organizationId":"$ORGANIZATION","idpProvider":"okta"}""",
        OrganizationAdminRule::class.java,
        Permission.PermissionType.ORGANIZATION_ADMIN,
      ),
      Scenario("/api/v1/scim_config/rotate_token", id, OrganizationAdminRule::class.java, Permission.PermissionType.ORGANIZATION_ADMIN),
      Scenario("/api/v1/scim_config/disable", id, OrganizationAdminRule::class.java, Permission.PermissionType.ORGANIZATION_ADMIN, status = 204),
    )
  }

  companion object {
    val ORGANIZATION: UUID = UUID(0, 4)
    val USER: UUID = UUID(0, 8)
  }
}
