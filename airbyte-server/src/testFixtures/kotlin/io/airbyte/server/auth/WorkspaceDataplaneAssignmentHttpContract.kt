/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem
import io.airbyte.api.server.generated.models.WorkspaceCreate
import io.airbyte.api.server.generated.models.WorkspaceUpdate
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.commons.PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.MaximumWorkspacesEntitlement
import io.airbyte.commons.entitlements.models.NumericEntitlementResult
import io.airbyte.commons.entitlements.models.PrivateLinkEntitlement
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.CommunityCurrentUserService
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.commons.server.support.SecurityAwareCurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.WorkspaceApiController
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.token.DefaultRolesFinder
import io.micronaut.security.token.config.TokenConfigurationProperties
import io.micronaut.security.utils.DefaultSecurityService
import io.micronaut.security.utils.SecurityService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.function.Executable
import java.util.Optional
import java.util.UUID

/** Real route rules, caller resolution and handler; only storage and external services are replaced. */
abstract class WorkspaceDataplaneAssignmentHttpContract {
  protected open val cloud = false

  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `internal callers without a human record can assign an allowed dataplane group`() {
    assertAll(paths.map { path -> Executable { exercise(path, GROUP, 200, human = false) } })
  }

  @Test
  fun `internal callers cannot assign a foreign organization's dataplane group`() {
    assertAll(paths.map { path -> Executable { exercise(path, FOREIGN_GROUP, 403, human = false) } })
  }

  @Test
  fun `service callers use their own organization grants without a human record`() {
    assertAll(
      paths.flatMap { path ->
        listOf("service", "dataplane").flatMap { token ->
          listOf(
            Executable { exercise(path, GROUP, 200, token = token, human = false) },
            Executable { exercise(path, GROUP, 403, token = token, human = false, granted = false) },
          )
        }
      },
    )
  }

  @Test
  fun `internal callers can assign default groups and entitled PrivateLink groups`() {
    assertAll(
      paths.flatMap { path ->
        listOf(DEFAULT_GROUP, PRIVATE_GROUP).map { group -> Executable { exercise(path, group, 200, human = false) } }
      },
    )
  }

  @Test
  fun `PrivateLink assignment still requires the target organization's entitlement`() {
    assertAll(paths.map { path -> Executable { exercise(path, PRIVATE_GROUP, 403, human = false, entitled = false) } })
  }

  @Test
  fun `user and no auth requests preserve the organization permission check for explicit assignments`() {
    assertAll(
      paths.flatMap { path ->
        listOf(true, false).flatMap { security ->
          listOf(
            Executable { exercise(path, GROUP, 200, token = "user-without-roles", security = security) },
            Executable { exercise(path, GROUP, 403, token = "user-without-roles", security = security, granted = false) },
          )
        }
      },
    )
  }

  @Test
  fun `direct handler callers retain organization permission and dataplane ownership checks`() {
    val authentication = mockk<SecurityService>()
    every { authentication.authentication } returns Optional.of(Authentication.build("caller"))
    for (update in listOf(false, true)) {
      val allowed = Fixture(authentication)
      assertEquals(GROUP, allowed.assignDirectly(GROUP, update).dataplaneGroupId)
      val denied = Fixture(authentication, granted = false)
      assertThrows<ForbiddenProblem> { denied.assignDirectly(GROUP, update) }
      assertThrows<ForbiddenProblem> { allowed.assignDirectly(FOREIGN_GROUP, update) }
      assertThrows<LicenseEntitlementProblem> { Fixture(authentication, entitled = false).assignDirectly(PRIVATE_GROUP, update) }
    }
  }

  @Test
  fun `direct handler calls without authentication retain the current user's permission check`() {
    val authentication = mockk<SecurityService>()
    every { authentication.authentication } returns Optional.empty()
    for (update in listOf(false, true)) {
      val allowed = Fixture(authentication, security = false)
      assertEquals(GROUP, allowed.assignDirectly(GROUP, update).dataplaneGroupId)
      assertThrows<ForbiddenProblem> { Fixture(authentication, granted = false, security = false).assignDirectly(GROUP, update) }
    }
  }

  private fun exercise(
    path: String,
    group: UUID,
    expected: Int,
    token: String = "internal-without-roles",
    security: Boolean = true,
    human: Boolean = true,
    granted: Boolean = true,
    entitled: Boolean = true,
  ) {
    val controller = if (cloud) Class.forName("io.airbyte.server.wrapped.apis.WorkspaceApiWrappedController") else WorkspaceApiController::class.java
    val authentication = DefaultSecurityService(DefaultRolesFinder(TokenConfigurationProperties()))
    val fixture = Fixture(authentication, human, granted, entitled, security)
    RemainingOperationHttpSupport(
      cloud,
      listOf(controller),
      setOf(WorkspaceCreateRule::class.java.name, WorkspaceWriteRule::class.java.name),
      security,
    ).run(
      configure = { context, business ->
        business.forEach(::configureDependency)
        context.registerSingleton(WorkspacesHandler::class.java, fixture.handler)
        context.registerSingleton(CurrentUserService::class.java, fixture.users)
        context.registerSingleton(PermissionHandler::class.java, fixture.permissionHandler)
        context.registerSingleton(SecurityService::class.java, authentication)
        context.registerSingleton(AuthorizationDataService::class.java, fixture.data)
        context.registerSingleton(OperationPermissions::class.java, OperationPermissions(fixture.grants, fixture.roles, security))
      },
      check = { _, server, _ ->
        val selector = if (path == "/update") "workspaceId" else "id"
        val body = """{"$selector":"$WORKSPACE","organizationId":"$ORGANIZATION","name":"fixture","dataplaneGroupId":"$group"}"""
        val response = RemainingOperationHttpSupport.request(server, "POST", "/api/v1/workspaces$path", body, if (security) token else "")
        assertEquals(expected, response.statusCode(), "$path: ${response.body()}")
        if (expected == 200) {
          assertEquals(group.toString(), Jsons.deserialize(response.body()).path("dataplaneGroupId").asText())
          assertEquals(group, fixture.saved.dataplaneGroupId)
        } else {
          assertEquals(DEFAULT_GROUP, fixture.saved.dataplaneGroupId)
          verify(exactly = 0) { fixture.workspaces.writeWorkspaceWithSecrets(any()) }
          verify(exactly = 0) { fixture.workspaces.writeStandardWorkspaceNoSecrets(any()) }
          verify(exactly = 0) { fixture.workspaces.writeWorkspacePreservingWebhookConfigs(any(), any()) }
        }
        if (!human) {
          verify(exactly = 0) { fixture.userPersistence.getDefaultUser() }
          verify(exactly = 0) { fixture.userPersistence.getUserByAuthId(any()) }
        }
      },
    )
  }

  private inner class Fixture(
    authentication: SecurityService,
    human: Boolean = true,
    granted: Boolean = true,
    entitled: Boolean = true,
    security: Boolean = true,
  ) {
    val userPersistence = mockk<UserPersistence>()
    val users: CurrentUserService =
      if (security) {
        SecurityAwareCurrentUserService(userPersistence, authentication, mockk { every { hasConnection() } returns false })
      } else {
        CommunityCurrentUserService(userPersistence)
      }
    val grants = mockk<PermissionService>()
    val permissionHandler = PermissionHandler(mockk(), mockk(), { UUID.randomUUID() }, grants)
    val groups = mockk<DataplaneGroupService>()
    val workspaces = mockk<WorkspaceService>()
    val organizations = mockk<OrganizationService>()
    val entitlements = mockk<EntitlementService>()
    val persistence = mockk<WorkspacePersistence>()
    var saved =
      StandardWorkspace()
        .withWorkspaceId(WORKSPACE)
        .withCustomerId(UUID(0, 3))
        .withName("fixture")
        .withSlug("fixture")
        .withInitialSetupComplete(false)
        .withOrganizationId(ORGANIZATION)
        .withDataplaneGroupId(DEFAULT_GROUP)
    val roles =
      RoleResolver(
        AuthenticationHeaderResolver(mockk(), permissionHandler, userPersistence, groups, mockk(), mockk()),
        users,
        authentication,
        permissionHandler,
      )

    init {
      every { userPersistence.getDefaultUser() } returns
        if (human) Optional.of(AuthenticatedUser().withUserId(USER).withAuthUserId(DEFAULT_USER_ID.toString())) else Optional.empty()
      every { userPersistence.getUserByAuthId("caller") } returns
        if (human) Optional.of(AuthenticatedUser().withUserId(USER).withAuthUserId("caller")) else Optional.empty()
      val permissions =
        if (granted) {
          listOf(
            Permission()
              .withPermissionId(UUID(0, 19))
              .withUserId(USER)
              .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
              .withOrganizationId(ORGANIZATION),
          )
        } else {
          emptyList()
        }
      every { grants.getPermissionsByAuthUserId("caller") } returns permissions
      every { grants.getPermissionsByAuthUserId(DEFAULT_USER_ID.toString()) } returns permissions
      every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns permissions
      every { grants.getEffectivePermissionsForUser(USER) } returns permissions
      for (organization in listOf(ORGANIZATION, UUID(0, 90), UUID(0, 91), PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID)) {
        every { organizations.getOrganization(organization) } returns Optional.of(Organization().withOrganizationId(organization))
      }
      every { groups.getDataplaneGroup(GROUP) } returns DataplaneGroup().withId(GROUP).withOrganizationId(ORGANIZATION)
      every { groups.getDataplaneGroup(DEFAULT_GROUP) } returns DataplaneGroup().withId(DEFAULT_GROUP).withOrganizationId(UUID(0, 90))
      every { groups.getDataplaneGroup(FOREIGN_GROUP) } returns DataplaneGroup().withId(FOREIGN_GROUP).withOrganizationId(UUID(0, 91))
      every { groups.getDataplaneGroup(PRIVATE_GROUP) } returns
        DataplaneGroup().withId(PRIVATE_GROUP).withOrganizationId(PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID)
      every { groups.listDefaultDataplaneGroups() } returns listOf(groups.getDataplaneGroup(DEFAULT_GROUP))
      every { persistence.workspaceExists(WORKSPACE) } returns false
      every { entitlements.getNumericEntitlement(OrganizationId(ORGANIZATION), MaximumWorkspacesEntitlement) } returns
        NumericEntitlementResult(MaximumWorkspacesEntitlement.featureId, hasAccess = true, value = null, isUnlimited = true)
      every { entitlements.checkEntitlement(OrganizationId(ORGANIZATION), PrivateLinkEntitlement) } returns
        EntitlementResult(PrivateLinkEntitlement.featureId, entitled)
      every { entitlements.ensureEntitled(any(), any()) } answers { callOriginal() }
      every { workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, any()) } answers { Jsons.clone(saved) }
      every { workspaces.getWorkspaceBySlugOptional("fixture", true) } returns Optional.empty()
      every { workspaces.writeWorkspaceWithSecrets(any()) } answers { saved = firstArg() }
      every { workspaces.writeStandardWorkspaceNoSecrets(any()) } answers { saved = firstArg() }
      every { workspaces.writeWorkspacePreservingWebhookConfigs(any(), any()) } answers { saved = firstArg() }
    }

    val data =
      AuthorizationDataService(
        workspaces = workspaces,
        permissions = grants,
        actors = mockk(),
        connections = mockk(),
        operations = mockk(),
        jobs = mockk(),
        organizations = organizations,
        dataplanes = mockk(),
        dataplaneGroups = groups,
        groups = mockk(),
        verifications = mockk(),
        secretStorages = mockk(),
        tags = mockk(),
        projects = mockk(),
        commands = mockk(),
        users = userPersistence,
        invitations = mockk(),
        privateLinks = mockk(),
        connectionEvents = mockk(),
        actorRepository = mockk(),
        attempts = mockk(),
        entitlements = entitlements,
        featureFlags = mockk(),
      )
    val handler =
      WorkspacesHandler(
        workspacePersistence = persistence,
        organizationService = organizations,
        permissionHandler = permissionHandler,
        connectionsHandler = mockk(),
        destinationHandler = mockk(),
        sourceHandler = mockk(),
        uuidSupplier = { WORKSPACE },
        workspaceService = workspaces,
        workspaceHelper = mockk(),
        dataplaneGroupService = groups,
        entitlementService = entitlements,
        trackingClient = mockk(relaxUnitFun = true),
        limitsProvider = mockk(),
        consumptionService = mockk(),
        ffClient = mockk(),
        airbyteEdition = if (cloud) AirbyteEdition.CLOUD else AirbyteEdition.COMMUNITY,
        roleResolver = roles,
        scimConfigurationService = mockk(),
        scimAccessGate = mockk(),
        operationsHandler = mockk(),
      )

    fun assignDirectly(
      group: UUID,
      update: Boolean,
    ) = if (update) {
      handler.updateWorkspace(WorkspaceUpdate(workspaceId = WORKSPACE, dataplaneGroupId = group))
    } else {
      handler.createWorkspace(WorkspaceCreate(organizationId = ORGANIZATION, name = "fixture", dataplaneGroupId = group))
    }
  }

  companion object {
    private val WORKSPACE = UUID(0, 1)
    private val ORGANIZATION = UUID(0, 2)
    private val USER = UUID(0, 8)
    private val GROUP = UUID(0, 40)
    private val DEFAULT_GROUP = UUID(0, 41)
    private val FOREIGN_GROUP = UUID(0, 42)
    private val PRIVATE_GROUP = UUID(0, 43)
    private val paths = listOf("/create", "/create_if_not_exist", "/update")
  }
}
