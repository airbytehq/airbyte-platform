/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRole
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.auth.TokenType
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.metrics.MetricClient
import io.micronaut.security.utils.SecurityService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class RoleResolverTest {
  // The role resolver always returns AUTHENTICATED_USER
  val baseRoles = setOf<String>(AuthRoleConstants.AUTHENTICATED_USER)

  lateinit var workspaceHelper: WorkspaceHelper
  lateinit var userPersistence: UserPersistence
  lateinit var currentUserService: CurrentUserService
  lateinit var securityService: SecurityService
  lateinit var permissionHandler: PermissionHandler
  lateinit var dataplaneGroupService: DataplaneGroupService
  lateinit var dataplaneService: DataplaneService
  lateinit var roleResolver: RoleResolver

  @BeforeEach
  fun setUp() {
    workspaceHelper = mockk()
    // Every workspace belongs to some organization; tests that care about which one stub it specifically.
    every { workspaceHelper.getOrganizationForWorkspace(any()) } answers { UUID.randomUUID() }
    userPersistence = mockk()
    currentUserService = mockk()
    permissionHandler = mockk()
    securityService = mockk()
    dataplaneGroupService = mockk()
    dataplaneService = mockk()
    val authenticationHeaderResolver =
      AuthenticationHeaderResolver(
        workspaceHelper,
        permissionHandler,
        userPersistence,
        dataplaneGroupService,
        dataplaneService,
        mockk<MetricClient>(relaxed = true),
      )
    roleResolver = RoleResolver(authenticationHeaderResolver, currentUserService, securityService, permissionHandler)
  }

  @Test
  fun trustedWorkspaceOrganizationRejectsForeignOrganizationGrant() {
    val workspaceId = UUID.randomUUID()
    val oldOrganizationId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val workspaceOrganizations = mapOf(workspaceId to organizationId)
    val staleGrant = listOf(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_READER).withOrganizationId(oldOrganizationId))
    assertEquals(baseRoles, roleResolver.resolveRoles(staleGrant, "auth-user-1", setOf(workspaceId), workspaceOrganizations, emptySet(), emptySet()))

    val organizationGrant = listOf(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_READER).withOrganizationId(organizationId))
    val organizationRoles =
      roleResolver.resolveRoles(
        organizationGrant,
        "auth-user-1",
        setOf(workspaceId),
        workspaceOrganizations,
        emptySet(),
        emptySet(),
      )
    assertTrue(AuthRoleConstants.WORKSPACE_READER in organizationRoles, "$organizationRoles")

    val workspaceGrant = listOf(Permission().withPermissionType(Permission.PermissionType.WORKSPACE_READER).withWorkspaceId(workspaceId))
    val workspaceRoles = roleResolver.resolveRoles(workspaceGrant, "auth-user-1", setOf(workspaceId), workspaceOrganizations, emptySet(), emptySet())
    assertTrue(AuthRoleConstants.WORKSPACE_READER in workspaceRoles, "$workspaceRoles")
  }

  @Test
  fun testWithCurrentUser() {
    every { currentUserService.getCurrentUser() } returns AuthenticatedUser().withAuthUserId("auth-user-1")
    val req = roleResolver.newRequest().withCurrentUser()
    assertEquals(RoleResolver.Subject("auth-user-1", TokenType.USER), req.subject)
  }

  @Test
  fun selfRoleWithMultipleAuthUserIds() {
    val uid = UUID.randomUUID()
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.AIRBYTE_USER_ID, uid)

    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns emptyList()
    every { userPersistence.listAuthUserIdsForUser(uid) } returns listOf("auth-user-1", "alt-user-1")

    assertEquals(setOf("AUTHENTICATED_USER", "SELF"), req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
    verify(exactly = 1) { userPersistence.listAuthUserIdsForUser(uid) }
  }

  @Test
  fun noSelf() {
    val uid = UUID.randomUUID()
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.AIRBYTE_USER_ID, uid)

    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns emptyList()
    every { userPersistence.listAuthUserIdsForUser(uid) } returns listOf("auth-user-2")

    assertEquals(setOf("AUTHENTICATED_USER"), req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
    verify(exactly = 1) { userPersistence.listAuthUserIdsForUser(uid) }
  }

  @Test
  fun testWithRef() {
    val roleReq = roleResolver.newRequest().withRef(AuthenticationId.CONNECTION_ID, "conn-id-1")
    assertEquals(mapOf(AuthenticationId.CONNECTION_ID.httpHeader to "conn-id-1"), roleReq.props)
  }

  @Test
  fun noUserSpecified() {
    assertEquals(emptySet<String>(), roleResolver.newRequest().roles())
    assertEquals(emptySet<String>(), roleResolver.newRequest().withSubject("  ", TokenType.USER).roles())
  }

  @Test
  fun exceptionsAreDropped() {
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } throws Exception("foo")
    assertEquals(emptySet<String>(), roleResolver.newRequest().withSubject("auth-user-1", TokenType.USER).roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun orgRolesImplyWorkspaceRoles() {
    val org = UUID.randomUUID()
    val perms =
      listOf(
        Permission()
          .withOrganizationId(org)
          .withPermissionType(Permission.PermissionType.ORGANIZATION_EDITOR),
      )

    val roles = roleResolver.resolveRoles(perms, "user1", emptySet(), emptyMap(), setOf(org), emptySet())
    val expect =
      """
      AUTHENTICATED_USER 
      WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR WORKSPACE_SOURCE_EDITOR WORKSPACE_DESTINATION_EDITOR 
      ORGANIZATION_MEMBER ORGANIZATION_READER ORGANIZATION_RUNNER ORGANIZATION_EDITOR
    """.toRoleSet()
    assertEquals(expect, roles)
  }

  @Test
  fun noWorkspaceAccess() {
    // user does not have any permissions
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns emptyList()

    // user is requesting access to a workspace
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.WORKSPACE_ID, UUID.randomUUID())

    assertEquals(baseRoles, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun noOrgAccess() {
    // user does not have any permissions
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns emptyList()

    // user is requesting access to an org
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.ORGANIZATION_ID, UUID.randomUUID())

    assertEquals(baseRoles, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun noWorkspaceAccess2() {
    val workspace1 = UUID.randomUUID()
    val workspace2 = UUID.randomUUID()

    // user is requesting access to workspace 1
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.WORKSPACE_ID, workspace1)

    // user is workspace admin on workspace 2, but not workspace 1
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withWorkspaceId(workspace2),
      )

    assertEquals(baseRoles, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun allowWorkspaceAccess() {
    val workspace1 = UUID.randomUUID()

    // user is requesting access to workspace 1
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.WORKSPACE_ID, workspace1)

    // user is workspace admin on workspace 1
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withWorkspaceId(workspace1),
      )

    val expect =
      "AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR WORKSPACE_SOURCE_EDITOR WORKSPACE_DESTINATION_EDITOR WORKSPACE_ADMIN"
        .toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun requestMultipleWorkspaces() {
    val workspace1 = UUID.randomUUID()
    val workspace2 = UUID.randomUUID()

    val grants =
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withWorkspaceId(workspace1),
      )

    val roles = roleResolver.resolveRoles(grants, "auth-user-1", setOf(workspace1, workspace2), emptyMap(), emptySet(), emptySet())
    assertEquals(baseRoles, roles)
  }

  @Test
  fun grantedMultipleOrgs() {
    val org1 = UUID.randomUUID()
    val org2 = UUID.randomUUID()

    // user is requesting access to one org, but has access to multiple.
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.ORGANIZATION_ID, org1)

    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
          .withOrganizationId(org1),
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
          .withOrganizationId(org2),
      )

    // The user is not granted workspace access, because they need access to all requested workspaces
    val expect =
      """
      AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR WORKSPACE_SOURCE_EDITOR WORKSPACE_DESTINATION_EDITOR
      ORGANIZATION_ADMIN ORGANIZATION_EDITOR WORKSPACE_ADMIN
      ORGANIZATION_READER ORGANIZATION_RUNNER ORGANIZATION_MEMBER
    """.toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun multipleWorkspacesMinRole() {
    val workspace1 = UUID.randomUUID()
    val workspace2 = UUID.randomUUID()

    val grants =
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withWorkspaceId(workspace1),
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_RUNNER)
          .withWorkspaceId(workspace2),
      )

    // The user is granted the minimum common role across the workspaces, which is runner.
    val expect = "AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER".toRoleSet()
    val roles = roleResolver.resolveRoles(grants, "auth-user-1", setOf(workspace1, workspace2), emptyMap(), emptySet(), emptySet())
    assertEquals(expect, roles)
  }

  @Test
  fun workspaceRoleViaResourceReference() {
    val workspace1 = UUID.randomUUID()
    val source1 = UUID.randomUUID()

    // user is requesting access to source1
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.SOURCE_ID, source1)

    // source1 is owned by workspace1
    every { workspaceHelper.getWorkspaceForSourceId(source1) } returns workspace1

    // the user has reader access to workspace1
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
          .withWorkspaceId(workspace1),
      )

    val expect = "AUTHENTICATED_USER WORKSPACE_READER".toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun workspaceRoleViaResourceReferenceNoAccess() {
    val workspace1 = UUID.randomUUID()
    val workspace2 = UUID.randomUUID()
    val source1 = UUID.randomUUID()

    // user is requesting access to source1
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.SOURCE_ID, source1)

    // source1 is owned by workspace2
    every { workspaceHelper.getWorkspaceForSourceId(source1) } returns workspace2

    // the user has reader access to workspace1
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
          .withWorkspaceId(workspace1),
      )

    assertEquals(baseRoles, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun requireRole() {
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns emptyList()

    val req = roleResolver.newRequest().withSubject("auth-user-1", TokenType.USER)
    assertThrows<ForbiddenProblem> {
      req.requireRole("FOO")
    }

    req.requireRole("AUTHENTICATED_USER")
  }

  @Test
  fun organizationAdminWithWorkspaceAccess() {
    val workspace1 = UUID.randomUUID()
    val org1 = UUID.randomUUID()
    val org2 = UUID.randomUUID()

    // user is requesting access to workspace1
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.WORKSPACE_ID, workspace1)

    // user has both ORGANIZATION_MEMBER and ORGANIZATION_ADMIN permissions
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)
          .withOrganizationId(org1),
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
          .withOrganizationId(org2),
      )

    // workspace 1 belongs to org 2
    every { workspaceHelper.getOrganizationForWorkspace(workspace1) } returns org2

    // The user should get ORGANIZATION_ADMIN permission to the workspace
    val expect =
      """
      AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR WORKSPACE_SOURCE_EDITOR WORKSPACE_DESTINATION_EDITOR 
      ORGANIZATION_ADMIN ORGANIZATION_EDITOR WORKSPACE_ADMIN 
      ORGANIZATION_READER ORGANIZATION_RUNNER ORGANIZATION_MEMBER
    """.toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun workspaceAdminPermissionOnSpecificWorkspace() {
    val workspace1 = UUID.randomUUID()
    val workspace2 = UUID.randomUUID()

    // user is requesting access to workspace1
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.WORKSPACE_ID, workspace1)

    // user has workspace admin permission on workspace1 and workspace reader on workspace2
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withWorkspaceId(workspace1),
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
          .withWorkspaceId(workspace2),
      )

    // The user should get WORKSPACE_ADMIN permission for the requested workspace
    val expect =
      "AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR WORKSPACE_SOURCE_EDITOR WORKSPACE_DESTINATION_EDITOR WORKSPACE_ADMIN"
        .toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun groupWorkspacePermissionResolvesWorkspaceRoles() {
    val workspace1 = UUID.randomUUID()
    val groupId = UUID.randomUUID()
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.WORKSPACE_ID, workspace1)

    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_EDITOR)
          .withWorkspaceId(workspace1)
          .withGroupId(groupId),
      )

    val expect =
      "AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR WORKSPACE_SOURCE_EDITOR WORKSPACE_DESTINATION_EDITOR"
        .toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun workspaceSourceEditorResolvesWithoutDestinationEditor() {
    val workspace1 = UUID.randomUUID()
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.WORKSPACE_ID, workspace1)

    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_SOURCE_EDITOR)
          .withWorkspaceId(workspace1),
      )

    val expect = "AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_SOURCE_EDITOR".toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun workspaceDestinationEditorResolvesWithoutSourceEditor() {
    val workspace1 = UUID.randomUUID()
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.WORKSPACE_ID, workspace1)

    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR)
          .withWorkspaceId(workspace1),
      )

    val expect = "AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_DESTINATION_EDITOR".toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun groupWorkspaceSourceEditorPermissionResolvesWorkspaceRoles() {
    val workspace1 = UUID.randomUUID()
    val groupId = UUID.randomUUID()
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.WORKSPACE_ID, workspace1)

    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_SOURCE_EDITOR)
          .withWorkspaceId(workspace1)
          .withGroupId(groupId),
      )

    val expect = "AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_SOURCE_EDITOR".toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun workspaceEditorOutranksActorScopedEditorForSameWorkspace() {
    val workspace1 = UUID.randomUUID()
    val groupId = UUID.randomUUID()
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.WORKSPACE_ID, workspace1)

    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_SOURCE_EDITOR)
          .withWorkspaceId(workspace1),
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_EDITOR)
          .withWorkspaceId(workspace1)
          .withGroupId(groupId),
      )

    val expect =
      "AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR WORKSPACE_SOURCE_EDITOR WORKSPACE_DESTINATION_EDITOR".toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun directAndGroupWorkspacePermissionsUseHighestGrantForSameWorkspace() {
    val workspace1 = UUID.randomUUID()
    val groupId = UUID.randomUUID()
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.WORKSPACE_ID, workspace1)

    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
          .withWorkspaceId(workspace1),
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withWorkspaceId(workspace1)
          .withGroupId(groupId),
      )

    val expect =
      "AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR WORKSPACE_SOURCE_EDITOR WORKSPACE_DESTINATION_EDITOR WORKSPACE_ADMIN"
        .toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun directAndGroupOrganizationPermissionsUseHighestGrantForSameOrganization() {
    val organizationId = UUID.randomUUID()
    val groupId = UUID.randomUUID()
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withOrg(organizationId)

    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)
          .withOrganizationId(organizationId),
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
          .withOrganizationId(organizationId)
          .withGroupId(groupId),
      )

    val expect =
      """
      AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR WORKSPACE_SOURCE_EDITOR WORKSPACE_DESTINATION_EDITOR
      ORGANIZATION_ADMIN ORGANIZATION_EDITOR WORKSPACE_ADMIN
      ORGANIZATION_READER ORGANIZATION_RUNNER ORGANIZATION_MEMBER
    """.toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun overlappingWorkspacePermissionsUseHighestGrantPerWorkspaceThenLowestAcrossWorkspaces() {
    val workspace1 = UUID.randomUUID()
    val workspace2 = UUID.randomUUID()
    val groupId = UUID.randomUUID()
    val grants =
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
          .withWorkspaceId(workspace1),
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withWorkspaceId(workspace1)
          .withGroupId(groupId),
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_EDITOR)
          .withWorkspaceId(workspace2),
      )

    val expect =
      "AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR WORKSPACE_SOURCE_EDITOR WORKSPACE_DESTINATION_EDITOR"
        .toRoleSet()
    val roles = roleResolver.resolveRoles(grants, "auth-user-1", setOf(workspace1, workspace2), emptyMap(), emptySet(), emptySet())
    assertEquals(expect, roles)
  }

  @Test
  fun organizationAndWorkspaceGrantsAreCombinedPerWorkspaceBeforeMultiWorkspaceReduction() {
    val workspace1 = UUID.randomUUID()
    val workspace2 = UUID.randomUUID()
    val organization1 = UUID.randomUUID()
    val organization2 = UUID.randomUUID()
    val group1 = UUID.randomUUID()
    val group2 = UUID.randomUUID()
    val grants =
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)
          .withOrganizationId(organization1),
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
          .withOrganizationId(organization1)
          .withGroupId(group1),
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)
          .withOrganizationId(organization2),
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
          .withWorkspaceId(workspace2)
          .withGroupId(group2),
      )

    val expect = "AUTHENTICATED_USER ORGANIZATION_MEMBER WORKSPACE_READER".toRoleSet()
    val workspaceOrganizations = mapOf(workspace1 to organization1, workspace2 to organization2)
    val roles =
      roleResolver.resolveRoles(
        grants,
        "auth-user-1",
        setOf(workspace1, workspace2),
        workspaceOrganizations,
        setOf(organization1, organization2),
        emptySet(),
      )
    assertEquals(expect, roles)
  }

  @Test
  fun organizationGrantDoesNotCoverWorkspaceInAnotherOrganization() {
    val workspace = UUID.randomUUID()
    val workspaceOrganization = UUID.randomUUID()
    val otherOrganization = UUID.randomUUID()
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withRef(AuthenticationId.WORKSPACE_ID, workspace)

    every { workspaceHelper.getOrganizationForWorkspace(workspace) } returns workspaceOrganization
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
          .withOrganizationId(otherOrganization),
      )

    assertEquals(baseRoles, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun overlappingOrganizationPermissionsUseHighestGrantPerOrganizationThenLowestAcrossOrganizations() {
    val organization1 = UUID.randomUUID()
    val organization2 = UUID.randomUUID()
    val groupId = UUID.randomUUID()
    val req =
      roleResolver
        .newRequest()
        .withSubject("auth-user-1", TokenType.USER)
        .withOrg(organization1)
        .withOrg(organization2)

    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)
          .withOrganizationId(organization1),
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
          .withOrganizationId(organization1)
          .withGroupId(groupId),
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_EDITOR)
          .withOrganizationId(organization2),
      )

    val expect =
      """
      AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR WORKSPACE_SOURCE_EDITOR WORKSPACE_DESTINATION_EDITOR
      ORGANIZATION_EDITOR ORGANIZATION_READER ORGANIZATION_RUNNER ORGANIZATION_MEMBER
    """.toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun hardCodedNonUserTokenRolesAreUnchanged() {
    val roles = roleResolver.newRequest().withSubject("workload-api-token", TokenType.WORKLOAD_API).roles()

    assertEquals(setOf(AuthRoleConstants.DATAPLANE), roles)
    verify(exactly = 0) { permissionHandler.getPermissionsByAuthUserId("workload-api-token") }
  }

  @Test
  fun serviceAccountPermissionResolutionIsUnchanged() {
    val serviceAccountId = UUID.randomUUID()
    every { permissionHandler.getPermissionsByServiceAccountId(serviceAccountId) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.INSTANCE_ADMIN))

    val roles = roleResolver.newRequest().withSubject(serviceAccountId.toString(), TokenType.SERVICE_ACCOUNT).roles()

    assertEquals(AuthRole.getInstanceAdminRoles(), roles)
    verify(exactly = 1) { permissionHandler.getPermissionsByServiceAccountId(serviceAccountId) }
    verify(exactly = 0) { permissionHandler.getPermissionsByAuthUserId(serviceAccountId.toString()) }
  }

  @Test
  fun nonUserPermissionResolutionPreservesLegacyCrossWorkspaceReduction() {
    val workspace1 = UUID.randomUUID()
    val workspace2 = UUID.randomUUID()
    val organization1 = UUID.randomUUID()
    val organization2 = UUID.randomUUID()
    val grants =
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
          .withWorkspaceId(workspace1),
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
          .withOrganizationId(organization1),
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withWorkspaceId(workspace2),
        Permission()
          .withPermissionType(Permission.PermissionType.ORGANIZATION_READER)
          .withOrganizationId(organization2),
      )

    val workspaceOrganizations = mapOf(workspace1 to organization1, workspace2 to organization2)
    val roles =
      roleResolver.resolveRoles(
        grants,
        "service-account",
        setOf(workspace1, workspace2),
        workspaceOrganizations,
        setOf(organization1, organization2),
        emptySet(),
        false,
      )
    val expect = "AUTHENTICATED_USER ORGANIZATION_READER ORGANIZATION_MEMBER WORKSPACE_READER".toRoleSet()
    assertEquals(expect, roles)
  }
}

private fun String.toRoleSet() = this.trim().split("\\s+".toRegex()).toSet()
