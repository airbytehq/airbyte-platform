/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.PrivateLinkEntitlement
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.OrganizationAccessService
import io.airbyte.server.apis.controllers.EntitlementsApiController
import io.airbyte.server.apis.controllers.FusionUsageApiController
import io.airbyte.server.apis.controllers.OrganizationSemanticSearchApiController
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

abstract class RemainingAdditionalOrganizationAuthorizationHttpContract {
  protected open val cloud = false

  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `remaining organization and entitlement routes preserve workload policy before normal business`() {
    assertAll(
      scenarios().map { scenario ->
        Executable {
          exercise(scenario, if (scenario.rule.endsWith("OssUnsupportedOperationRule")) 501 else 403, token = "workload", checkDeclaration = false)
        }
      },
    )
  }

  @Test
  fun `OSS entitlement plan update keeps instance admin without saved organization reads`() {
    if (cloud) return
    val update = scenarios().single { it.path.endsWith("/update_plan") }
    assertAll(
      { exercise(update, 501, missing = true, checkDeclaration = false, unusedOwner = true) },
      { exercise(update, 501, lookupError = true, checkDeclaration = false, unusedOwner = true) },
      { exercise(update, 501, missing = true, security = false, granted = false, checkDeclaration = false, unusedOwner = true) },
      { exercise(update, 403, granted = false, checkDeclaration = false, unusedOwner = true) },
      { exercise(update.copy(grant = Permission.PermissionType.ORGANIZATION_ADMIN), 403, checkDeclaration = false, unusedOwner = true) },
      { exercise(update, 403, token = "workload", checkDeclaration = false, unusedOwner = true) },
      { exercise(update, 403, token = "scoped:${UUID(0, 1)}", checkDeclaration = false, unusedOwner = true) },
    )
  }

  @Test
  fun `all six remaining organization operations execute independent expected policies and normal responses`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, status(scenario)) } })
  }

  @Test
  fun `organization operations preserve grant checks and OSS unsupported responses before business`() {
    assertAll(
      scenarios().map { scenario ->
        Executable { exercise(scenario, if (scenario.rule.endsWith("OssUnsupportedOperationRule")) 501 else 403, granted = false) }
      },
    )
  }

  @Test
  fun `organization helper alternatives scoped tokens and required lookup failures preserve boundaries`() {
    for (scenario in scenarios().filter { it.rule.endsWith("OrganizationAccessibleRule") || it.rule.endsWith("OrganizationFusionUsageRule") }) {
      exercise(scenario, status(scenario), granted = false, workspaceAccess = true)
      exercise(scenario, 403, token = "scoped:${UUID(0, 1)}", workspaceAccess = true)
    }
    for (scenario in scenarios().filter {
      !it.rule.endsWith("GlobalAdminRule") && !it.rule.endsWith("OssUnsupportedOperationRule") &&
        !it.rule.endsWith("OssOrganizationAdminUnsupportedRule") && !it.rule.endsWith("OssInstanceAdminUnsupportedRule")
    }) {
      exercise(scenario, 404, missing = true)
      exercise(scenario, 500, lookupError = true)
    }
    for (scenario in scenarios()) exercise(scenario, status(scenario), security = false, granted = false)
  }

  @Test
  fun `OSS semantic search and fusion stubs preserve unsupported responses without tenant lookups`() {
    if (cloud) return
    assertAll(
      scenarios().filter { it.path.endsWith("/get_semantic_search_enablement") || it.path.endsWith("/tool_calls/usage") }.map { scenario ->
        Executable {
          exercise(scenario, 501, granted = false, checkDeclaration = false, unusedTenant = true)
          exercise(scenario, 401, token = "anonymous", checkDeclaration = false, unusedTenant = true)
          exercise(scenario, 501, token = "workload", checkDeclaration = false, unusedTenant = true)
          exercise(scenario, 501, token = "embedded", checkDeclaration = false, unusedTenant = true)
          exercise(scenario, 403, token = "scoped:${UUID(0, 1)}", checkDeclaration = false, unusedTenant = true)
          exercise(scenario, 501, granted = false, security = false, missing = true, checkDeclaration = false, unusedTenant = true)
        }
      },
    )
  }

  @Test
  fun `OSS semantic search update rejects missing and null selectors during request binding`() {
    if (cloud) return
    val update = scenarios().single { it.path.endsWith("/update_semantic_search_enablement") }
    assertAll(
      listOf("""{"semanticSearchEnabled":true}""", """{"organizationId":null,"semanticSearchEnabled":true}""").flatMap { body ->
        val request = update.copy(body = body)
        listOf(
          Executable { exercise(request, 400, security = false, granted = false, unusedOwner = true) },
          Executable { exercise(request.copy(grant = Permission.PermissionType.INSTANCE_ADMIN), 400, unusedOwner = true) },
        )
      },
    )
  }

  @Test
  fun `OSS semantic search update retains its explicit admin action without saved organization lookups`() {
    if (cloud) return
    val update = scenarios().single { it.path.endsWith("/update_semantic_search_enablement") }
    assertAll(
      { exercise(update, 403, token = "scoped:${UUID(0, 1)}", checkDeclaration = false, unusedOwner = true) },
      { exercise(update, 403, token = "workload", checkDeclaration = false, unusedOwner = true) },
      { exercise(update.copy(grant = Permission.PermissionType.INSTANCE_ADMIN), 501, missing = true, checkDeclaration = false, unusedOwner = true) },
      { exercise(update, 403, granted = false, checkDeclaration = false, unusedOwner = true) },
      { exercise(update, 501, missing = true, checkDeclaration = false, unusedOwner = true) },
      { exercise(update, 501, missing = true, granted = false, security = false, checkDeclaration = false, unusedOwner = true) },
    )
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    granted: Boolean = true,
    workspaceAccess: Boolean = false,
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
    checkDeclaration: Boolean = true,
    unusedTenant: Boolean = false,
    unusedOwner: Boolean = false,
  ) {
    val controllers =
      if (cloud) {
        listOf(
          Class.forName("io.airbyte.server.wrapped.apis.controllers.EntitlementsWrappedController"),
          Class.forName("io.airbyte.server.wrapped.apis.controllers.OrganizationSemanticSearchWrappedController"),
          Class.forName("io.airbyte.server.wrapped.sonar.FusionUsageController"),
        )
      } else {
        listOf(EntitlementsApiController::class.java, OrganizationSemanticSearchApiController::class.java, FusionUsageApiController::class.java)
      }
    val data = mockk<AuthorizationDataService>()
    val access = mockk<OrganizationAccessService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(cloud, controllers, scenarios().map { it.rule }.toSet(), security, earlierAllow = token != "anonymous").run(
      configure = { context, business ->
        business.forEach { instance ->
          when (instance) {
            is EntitlementService -> {
              every { instance.checkEntitlement(OrganizationId(ORGANIZATION), PrivateLinkEntitlement) } returns
                EntitlementResult(PrivateLinkEntitlement.featureId, true)
              every { instance.addOrUpdateOrganization(OrganizationId(ORGANIZATION), EntitlementPlan.STANDARD) } just Runs
            }
            is OrganizationAccessAuthorizationHelper -> every { instance.validateOrganizationOrWorkspaceAccess(ORGANIZATION) } just Runs
          }
          configureDependency(instance)
        }
        every { data.organization(ORGANIZATION) } returns ORGANIZATION
        if (missing) every { data.organization(ORGANIZATION) } throws AuthorizationResourceNotFoundException()
        if (lookupError) every { data.organization(ORGANIZATION) } throws java.io.IOException("organization unavailable")
        every { access.hasAccessibleWorkspace("caller", ORGANIZATION) } returns workspaceAccess
        val permission = Permission().withPermissionType(scenario.grant)
        if (scenario.grant != Permission.PermissionType.INSTANCE_ADMIN) permission.organizationId = ORGANIZATION
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(OrganizationAccessService::class.java, access)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (checkDeclaration) RemainingOperationHttpSupport.requireRule(context, scenario.method, scenario.template, scenario.rule)
        val response = RemainingOperationHttpSupport.request(server, scenario.method, scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected !in setOf(200, 501)) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (expected == 200) assertTrue(response.body().isNotBlank())
        if (unusedTenant || scenario.rule.endsWith("OssUnsupportedOperationRule")) {
          verify { data wasNot Called }
          verify { access wasNot Called }
          verify { grants wasNot Called }
          RemainingOperationHttpSupport.requireNoBusiness(business)
        }
        if (unusedOwner || scenario.rule.endsWith("OssOrganizationAdminUnsupportedRule")) {
          verify { data wasNot Called }
          verify { access wasNot Called }
          RemainingOperationHttpSupport.requireNoBusiness(business)
        }
        if (!security) verify(exactly = 0) { grants.getPermissionsByAuthUserId(any()) }
      },
    )
  }

  private data class Scenario(
    val path: String,
    val body: String,
    val rule: String,
    val grant: Permission.PermissionType,
    val cloudOnly: Boolean = false,
    val method: String = "POST",
    val template: String = path,
  )

  private fun status(scenario: Scenario) = if (scenario.cloudOnly && !cloud) 501 else 200

  private fun scenarios(): List<Scenario> =
    listOf(
      Scenario(
        "/api/v1/entitlements/is_entitled",
        """{"organization_id":"$ORGANIZATION","feature_id":"feature-privatelink"}""",
        "io.airbyte.server.auth.OrganizationMemberRule",
        Permission.PermissionType.ORGANIZATION_MEMBER,
      ),
      Scenario(
        "/api/v1/entitlements/list_plans",
        "{}",
        "io.airbyte.server.auth.GlobalAdminRule",
        Permission.PermissionType.INSTANCE_ADMIN,
        cloudOnly = true,
      ),
      Scenario(
        "/api/v1/entitlements/update_plan",
        """{"organization_id":"$ORGANIZATION","plan_name":"STANDARD"}""",
        if (cloud) "io.airbyte.server.auth.OrganizationInstanceAdminRule" else "io.airbyte.server.auth.OssInstanceAdminUnsupportedRule",
        Permission.PermissionType.INSTANCE_ADMIN,
        cloudOnly = true,
      ),
      Scenario(
        "/api/v1/organizations/get_semantic_search_enablement",
        """{"organizationId":"$ORGANIZATION"}""",
        if (cloud) "io.airbyte.server.auth.OrganizationAccessibleRule" else "io.airbyte.server.auth.OssUnsupportedOperationRule",
        Permission.PermissionType.ORGANIZATION_MEMBER,
        cloudOnly = true,
      ),
      Scenario(
        "/api/v1/organizations/update_semantic_search_enablement",
        """{"organizationId":"$ORGANIZATION","semanticSearchEnabled":true}""",
        if (cloud) "io.airbyte.server.auth.OrganizationAdminRule" else "io.airbyte.server.auth.OssOrganizationAdminUnsupportedRule",
        Permission.PermissionType.ORGANIZATION_ADMIN,
        cloudOnly = true,
      ),
      Scenario(
        "/api/v1/organizations/$ORGANIZATION/tool_calls/usage",
        "",
        if (cloud) "io.airbyte.server.wrapped.auth.OrganizationFusionUsageRule" else "io.airbyte.server.auth.OssUnsupportedOperationRule",
        Permission.PermissionType.ORGANIZATION_MEMBER,
        cloudOnly = true,
        method = "GET",
        template = "/api/v1/organizations/{organizationId}/tool_calls/usage",
      ),
    )

  companion object {
    val ORGANIZATION: UUID = UUID(0, 2)
  }
}
