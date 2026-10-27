/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.NumericEntitlementResult
import io.airbyte.commons.entitlements.models.PrivateLinkEntitlement
import io.airbyte.commons.entitlements.models.PrivateLinkLimitEntitlement
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.services.DataplaneNetworkConfigService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.PrivateLinkService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ScopedConfigurationMetadata
import io.airbyte.domain.models.CloudProvider
import io.airbyte.domain.models.DataplaneNetworkConfig
import io.airbyte.domain.models.NetworkConfig
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.PrivateLink
import io.airbyte.domain.models.PrivateLinkServiceConfig
import io.airbyte.domain.models.PrivateLinkServiceType
import io.airbyte.domain.models.PrivateLinkStatus
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.PrivateLinkController
import io.mockk.Called
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

abstract class RemainingPrivateLinkAuthorizationHttpContract {
  protected open val cloud = false

  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `private link routes reject unsupported callers before provider token or provisioning business`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `OSS private link workspace stubs keep no auth unsupported response without saved metadata`() {
    if (cloud) return
    assertAll(
      scenarios().filter { it.suffix in setOf("create", "list") }.map { scenario ->
        Executable {
          exercise(scenario, 501, security = false, granted = false, missing = true, unusedOwner = true)
          exercise(scenario, 501, security = false, granted = false, lookupError = true, unusedOwner = true)
        }
      },
    )
  }

  @Test
  fun `OSS private link delete preserves its workspace editor action and static admin alternative`() {
    if (cloud) return
    val delete = scenarios().single { it.suffix == "delete" }.copy(body = """{"privateLinkId":"$LINK","workspaceId":"$WORKSPACE"}""")
    assertAll(
      { exercise(delete, 501, grant = Permission.PermissionType.WORKSPACE_SOURCE_EDITOR, declaration = false) },
      { exercise(delete, 403, grant = Permission.PermissionType.WORKSPACE_READER, declaration = false) },
      { exercise(delete, 403, granted = false, declaration = false) },
      { exercise(delete, 403, token = "workload", declaration = false) },
      { exercise(delete, 501, security = false, granted = false, missing = true, declaration = false, unusedOwner = true) },
      { exercise(delete, 501, grant = Permission.PermissionType.INSTANCE_ADMIN, missing = true, declaration = false, unusedOwner = true) },
    )
  }

  @Test
  fun `OSS private link workspace stubs preserve static instance admin access without saved workspace reads`() {
    if (cloud) return
    assertAll(
      scenarios().filter { it.suffix in setOf("create", "list") }.map { scenario ->
        Executable { exercise(scenario, 501, grant = Permission.PermissionType.INSTANCE_ADMIN, missing = true, unusedOwner = true) }
      },
    )
  }

  @Test
  fun `OSS private link stubs reject omitted and null selectors before business`() {
    if (cloud) return
    assertAll(
      scenarios().flatMap { scenario ->
        listOf("{}", """{"workspaceId":null,"privateLinkId":null,"status":null}""").flatMap { body ->
          val request = scenario.copy(body = body)
          listOf(
            Executable { exercise(request, 400, security = false, granted = false, unusedOwner = true) },
            Executable { exercise(request, 400, grant = Permission.PermissionType.INSTANCE_ADMIN, unusedOwner = true) },
            Executable { exercise(request, 400, granted = false, unusedOwner = true) },
            Executable { exercise(request, 400, grant = Permission.PermissionType.ORGANIZATION_MEMBER, unusedOwner = true) },
            Executable { exercise(request, 400, token = "workload", grant = Permission.PermissionType.INSTANCE_ADMIN, unusedOwner = true) },
            Executable {
              exercise(
                request,
                400,
                token = "scoped:${UUID(0, 99)}",
                grant = Permission.PermissionType.INSTANCE_ADMIN,
                unusedOwner = true,
              )
            },
          )
        }
      },
    )
  }

  @Test
  fun `active private link configuration references require both workspace and exact link origin`() {
    if (!cloud) return
    val update = scenarios().single { it.suffix == "update" }
    assertAll(
      scenarios().filter { it.suffix in setOf("delete", "update") }.flatMap { scenario ->
        listOf(
          Executable { exercise(scenario, 400, wrongConfig = true, declaration = false) },
          Executable { exercise(scenario, 400, wrongConfigOrigin = true, declaration = false) },
        )
      } +
        listOf(
          Executable { exercise(update, 403, token = "user", declaration = false) },
          Executable { exercise(update, 403, token = "application-owner", declaration = false) },
        ),
    )
  }

  @Test
  fun `all six private link operations declare edition specific stub workspace saved and worker policies`() {
    for (scenario in scenarios()) {
      exercise(scenario, success())
      exercise(scenario, 403, token = "service", granted = false)
      exercise(scenario, 403, token = "user", grant = Permission.PermissionType.ORGANIZATION_MEMBER)
      exercise(scenario, 403, token = "embedded")
      exercise(scenario, 403, token = "scoped:${UUID(0, 99)}")
      exercise(scenario, success(), granted = false, security = false)
      if (cloud && scenario.suffix != "list_by_status") {
        exercise(scenario, 404, missing = true)
        exercise(scenario, 500, lookupError = true)
      }
      if (scenario.suffix == "create" || scenario.suffix == "delete") exercise(scenario, 403, grant = Permission.PermissionType.WORKSPACE_READER)
    }
  }

  @Test
  fun `private link clear and status updates ignore unconsumed config IDs while inconsistent new references fail early`() {
    if (!cloud) return
    val update = scenarios().single { it.suffix == "update" }
    exercise(update, 200, deletedWorkspace = true)
    exercise(update.copy(body = """{"privateLinkId":"$LINK","clearScopedConfigurationId":true}"""), 200, configUnavailable = true)
    exercise(update.copy(body = """{"privateLinkId":"$LINK","status":"available"}"""), 200, configUnavailable = true)
    exercise(
      update.copy(body = """{"privateLinkId":"$LINK","scopedConfigurationId":"$CONFIG","clearScopedConfigurationId":true}"""),
      400,
      configUnavailable = true,
    )
  }

  @Test
  fun `private link missing token metadata and unusable saved group fail before provider or workflow writes`() {
    if (!cloud) return
    for (scenario in scenarios().filter { it.suffix in setOf("delete", "update") }) {
      exercise(scenario, 404, missingConfig = true)
      exercise(scenario, 500, configFailure = true)
    }
    for (scenario in scenarios().filter { it.suffix in setOf("create", "delete") }) exercise(scenario, 403, groupUsable = false)
  }

  private fun success() = if (cloud) 200 else 501

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String =
      if (cloud &&
        scenario.suffix == "update"
      ) {
        "internal"
      } else {
        "user"
      },
    declaration: Boolean = true,
    granted: Boolean = true,
    grant: Permission.PermissionType =
      if (scenario.suffix ==
        "list"
      ) {
        Permission.PermissionType.WORKSPACE_READER
      } else if (scenario.suffix == "create" || scenario.suffix == "delete") {
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR
      } else {
        Permission.PermissionType.INSTANCE_ADMIN
      },
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
    wrongConfig: Boolean = false,
    wrongConfigOrigin: Boolean = false,
    configUnavailable: Boolean = false,
    missingConfig: Boolean = false,
    configFailure: Boolean = false,
    groupUsable: Boolean = true,
    unusedOwner: Boolean = false,
    deletedWorkspace: Boolean = false,
  ) {
    val controller =
      if (cloud) {
        Class.forName(
          "io.airbyte.server.wrapped.apis.controllers.PrivateLinkWrappedController",
        )
      } else {
        PrivateLinkController::class.java
      }
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    val configs = mockk<ScopedConfigurationService>()
    val rules =
      scenarios().map { it.rule }.toSet() + if (cloud) setOf("io.airbyte.domain.services.auth.PrivateLinkAuthorizationDataService") else emptySet()
    RemainingOperationHttpSupport(cloud, listOf(controller), rules, security).run(
      configure = { context, business ->
        business.forEach(::configureDependency)
        val links = business.filterIsInstance<PrivateLinkService>().singleOrNull() ?: mockk()
        val workspaces = business.filterIsInstance<WorkspaceService>().singleOrNull() ?: mockk()
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        val saved =
          PrivateLink(
            LINK,
            WORKSPACE,
            GROUP,
            "fixture",
            PrivateLinkStatus.AVAILABLE,
            PrivateLinkServiceType.STORAGE,
            PrivateLinkServiceConfig.Storage("us-east-1", "fixture-bucket"),
            scopedConfigurationId = CONFIG,
            createdAt = OffsetDateTime.parse("2026-10-22T00:00:00Z"),
            updatedAt = OffsetDateTime.parse("2026-10-22T00:00:00Z"),
          )
        every { links.getPrivateLink(LINK) } returns saved
        every { links.listByWorkspaceId(WORKSPACE) } returns listOf(saved)
        every { links.listByStatus(PrivateLinkStatus.AVAILABLE) } returns listOf(saved)
        every { links.createPrivateLink(WORKSPACE, GROUP, "fixture", match { it is PrivateLinkServiceConfig.Storage }) } returns saved
        every { links.updatePrivateLink(id = LINK, status = PrivateLinkStatus.DELETING) } returns saved
        every { links.updatePrivateLink(id = LINK, scopedConfigurationId = CONFIG) } returns saved
        every { links.updatePrivateLink(id = LINK, status = PrivateLinkStatus.AVAILABLE) } returns saved
        every { links.updatePrivateLink(id = LINK) } returns saved
        every { links.clearScopedConfigurationId(LINK) } returns saved
        every { workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } returns
          StandardWorkspace().withWorkspaceId(WORKSPACE).withOrganizationId(ORGANIZATION).withDataplaneGroupId(GROUP)
        for (instance in business) {
          when (instance) {
            is EntitlementService -> {
              every { instance.checkEntitlement(OrganizationId(ORGANIZATION), PrivateLinkEntitlement) } returns
                EntitlementResult(PrivateLinkEntitlement.featureId, true, "fixture")
              every { instance.getNumericEntitlement(OrganizationId(ORGANIZATION), PrivateLinkLimitEntitlement) } returns
                NumericEntitlementResult(PrivateLinkLimitEntitlement.featureId, true, 0, isUnlimited = true)
            }
            is OrganizationService ->
              every { instance.getOrganization(ORGANIZATION) } returns
                Optional.of(Organization().withOrganizationId(ORGANIZATION).withName("fixture"))
            is DataplaneNetworkConfigService ->
              every { instance.getByDataplaneGroupId(GROUP) } returns
                DataplaneNetworkConfig(
                  dataplaneGroupId = GROUP,
                  provider = CloudProvider.AWS,
                  config = NetworkConfig.Aws("us-east-1", "vpc-fixture", listOf("subnet-fixture"), listOf("sg-fixture")),
                )
          }
        }
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        every { data.savedWorkspaceAndOrganization(WORKSPACE, false) } returns owner
        every { data.savedWorkspaceAndOrganization(WORKSPACE, true) } returns owner
        if (deletedWorkspace) every { data.savedWorkspaceAndOrganization(WORKSPACE, false) } throws AuthorizationResourceNotFoundException()
        every { data.canUseDataplaneGroup(GROUP, ORGANIZATION) } returns groupUsable
        every { configs.getScopedConfigurationMetadata(CONFIG) } returns
          ScopedConfigurationMetadata(
            CONFIG,
            "network_security_token",
            ConfigScopeType.WORKSPACE,
            if (wrongConfig) UUID(0, 99) else WORKSPACE,
            Optional.empty(),
            Optional.empty(),
            ConfigOriginType.PRIVATE_LINK,
            if (wrongConfigOrigin) UUID(0, 99).toString() else LINK.toString(),
          )
        if (missing) {
          every { data.workspaceAndOrganization(WORKSPACE) } throws AuthorizationResourceNotFoundException()
          every { data.savedWorkspaceAndOrganization(WORKSPACE, false) } throws AuthorizationResourceNotFoundException()
          every { data.savedWorkspaceAndOrganization(WORKSPACE, true) } throws AuthorizationResourceNotFoundException()
          every { links.getPrivateLink(LINK) } throws NoSuchElementException("missing link")
        }
        if (lookupError) {
          every { data.workspaceAndOrganization(WORKSPACE) } throws java.io.IOException("workspace metadata unavailable")
          every { data.savedWorkspaceAndOrganization(WORKSPACE, false) } throws java.io.IOException("workspace metadata unavailable")
          every { data.savedWorkspaceAndOrganization(WORKSPACE, true) } throws java.io.IOException("workspace metadata unavailable")
          every { links.getPrivateLink(LINK) } throws java.io.IOException("private link metadata unavailable")
        }
        if (configUnavailable) every { configs.getScopedConfigurationMetadata(CONFIG) } throws AssertionError("unconsumed config must not be read")
        if (missingConfig) {
          every { configs.getScopedConfigurationMetadata(CONFIG) } throws
            io.airbyte.data.ConfigNotFoundException(io.airbyte.config.ConfigNotFoundType.SCOPED_CONFIGURATION, CONFIG)
        }
        if (configFailure) every { configs.getScopedConfigurationMetadata(CONFIG) } throws java.io.IOException("token metadata unavailable")
        val permission = Permission().withPermissionType(grant)
        if (grant.name.startsWith("WORKSPACE")) {
          permission.workspaceId = WORKSPACE
        } else if (grant !=
          Permission.PermissionType.INSTANCE_ADMIN
        ) {
          permission.organizationId = ORGANIZATION
        }
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(ScopedConfigurationService::class.java, configs)
        if (!cloud) {
          context.registerSingleton(PrivateLinkService::class.java, links)
          context.registerSingleton(WorkspaceService::class.java, workspaces)
        }
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (declaration) RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, scenario.rule)
        val response = RemainingOperationHttpSupport.request(server, "POST", scenario.path, scenario.body, token)
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected != success()) {
          RemainingOperationHttpSupport.requireNoBusiness(business.filterNot { it is PrivateLinkService || it is WorkspaceService })
          business.filterIsInstance<PrivateLinkService>().forEach { links ->
            verify(exactly = 0) { links.createPrivateLink(any(), any(), any(), any()) }
            verify(exactly = 0) { links.updatePrivateLink(any(), any(), any(), any(), any()) }
            verify(exactly = 0) { links.clearScopedConfigurationId(any()) }
            verify(exactly = 0) { links.listByWorkspaceId(any()) }
            verify(exactly = 0) { links.listByStatus(any()) }
          }
        }
        verify(exactly = 0) { configs.getScopedConfiguration(any()) }
        verify(exactly = 0) { configs.getConnectorVersionSelector(any()) }
        if (!cloud) {
          RemainingOperationHttpSupport.requireNoBusiness(business)
          verify { configs wasNot Called }
        }
        if (unusedOwner) verify { data wasNot Called }
      },
    )
  }

  private data class Scenario(
    val suffix: String,
    val body: String,
    val rule: String,
  ) {
    val path = "/api/v1/private_link/$suffix"
  }

  private fun scenarios(): List<Scenario> {
    fun rule(
      active: String,
      stub: String,
    ) = if (cloud) "io.airbyte.server.wrapped.auth.$active" else "io.airbyte.server.auth.$stub"
    val id = """{"privateLinkId":"$LINK"}"""
    return listOf(
      Scenario(
        "create",
        """{"workspaceId":"$WORKSPACE","name":"fixture","serviceConfig":{"type":"storage","region":"us-east-1","bucket":"fixture-bucket"}}""",
        rule("PrivateLinkCreateRule", "PrivateLinkWorkspaceStubRule"),
      ),
      Scenario("list", """{"workspaceId":"$WORKSPACE"}""", rule("PrivateLinkListRule", "PrivateLinkWorkspaceStubRule")),
      Scenario(
        "delete",
        """{"privateLinkId":"$LINK","workspaceId":"$WORKSPACE"}""",
        rule("PrivateLinkDeleteRule", "PrivateLinkWorkspaceStubRule"),
      ),
      Scenario("get", id, rule("PrivateLinkInternalReadRule", "PrivateLinkGlobalStubRule")),
      Scenario(
        "update",
        """{"privateLinkId":"$LINK","scopedConfigurationId":"$CONFIG"}""",
        rule("PrivateLinkWorkerUpdateRule", "PrivateLinkGlobalStubRule"),
      ),
      Scenario("list_by_status", """{"status":"available"}""", rule("PrivateLinkStatusFilterRule", "PrivateLinkGlobalStubRule")),
    )
  }

  companion object {
    val LINK: UUID = UUID(0, 1)
    val WORKSPACE: UUID = UUID(0, 2)
    val ORGANIZATION: UUID = UUID(0, 3)
    val GROUP: UUID = UUID(0, 4)
    val CONFIG: UUID = UUID(0, 5)
  }
}
