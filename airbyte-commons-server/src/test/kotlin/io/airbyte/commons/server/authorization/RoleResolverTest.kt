/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.persistence.job.WorkspaceHelper
import io.micronaut.http.HttpRequest
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
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
  lateinit var permissionHandler: PermissionHandler
  lateinit var roleResolver: RoleResolver

  @BeforeEach
  fun setUp() {
    workspaceHelper = mockk()
    userPersistence = mockk()
    currentUserService = mockk()
    permissionHandler = mockk()
    val authenticationHeaderResolver = AuthenticationHeaderResolver(workspaceHelper, permissionHandler, userPersistence)
    roleResolver = RoleResolver(authenticationHeaderResolver, currentUserService, permissionHandler)
  }

  @Test
  fun testWithCurrentUser() {
    every { currentUserService.currentUser } returns AuthenticatedUser().withAuthUserId("auth-user-1")
    val req = roleResolver.Request().withCurrentUser()
    assertEquals("auth-user-1", req.authUserId)
  }

  @Test
  fun selfRoleWithMultipleAuthUserIds() {
    val uid = UUID.randomUUID()
    val req =
      roleResolver
        .Request()
        .withAuthUserId("auth-user-1")
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
        .Request()
        .withAuthUserId("auth-user-1")
        .withRef(AuthenticationId.AIRBYTE_USER_ID, uid)

    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns emptyList()
    every { userPersistence.listAuthUserIdsForUser(uid) } returns listOf("auth-user-2")

    assertEquals(setOf("AUTHENTICATED_USER"), req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
    verify(exactly = 1) { userPersistence.listAuthUserIdsForUser(uid) }
  }

  @Test
  fun testWithHttpRequest() {
    val httpReq = HttpRequest.GET<Any>("/")
    httpReq.headers.add(AuthenticationId.CONNECTION_ID.httpHeader, "conn-id-1")
    val roleReq = roleResolver.Request().withHttpRequest(httpReq)
    assertEquals(mapOf(AuthenticationId.CONNECTION_ID.httpHeader to "conn-id-1"), roleReq.props)
  }

  @Test
  fun testWithRef() {
    val roleReq = roleResolver.Request().withRef(AuthenticationId.CONNECTION_ID, "conn-id-1")
    assertEquals(mapOf(AuthenticationId.CONNECTION_ID.httpHeader to "conn-id-1"), roleReq.props)
  }

  @Test
  fun testWithWorkspaces() {
    val workspaces = listOf(UUID.randomUUID(), UUID.randomUUID())
    val req = roleResolver.Request().withWorkspaces(workspaces)
    assertEquals(mapOf(AuthenticationId.WORKSPACE_IDS.httpHeader to """["${workspaces[0]}","${workspaces[1]}"]"""), req.props)
  }

  @Test
  fun noUserSpecified() {
    assertEquals(emptySet<String>(), roleResolver.Request().roles())
    assertEquals(emptySet<String>(), roleResolver.Request().withAuthUserId("  ").roles())
  }

  @Test
  fun exceptionsAreDropped() {
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } throws Exception("foo")
    assertEquals(emptySet<String>(), roleResolver.Request().withAuthUserId("auth-user-1").roles())
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

    val roles = roleResolver.resolveRoles(perms, "user1", emptySet(), setOf(org), emptySet())
    val expect =
      """
      AUTHENTICATED_USER 
      WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR 
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
        .Request()
        .withAuthUserId("auth-user-1")
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
        .Request()
        .withAuthUserId("auth-user-1")
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
        .Request()
        .withAuthUserId("auth-user-1")
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
        .Request()
        .withAuthUserId("auth-user-1")
        .withRef(AuthenticationId.WORKSPACE_ID, workspace1)

    // user is workspace admin on workspace 1
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withWorkspaceId(workspace1),
      )

    val expect = "AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR WORKSPACE_ADMIN".toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun requestMultipleWorkspaces() {
    val workspace1 = UUID.randomUUID()
    val workspace2 = UUID.randomUUID()

    // user is requesting access to multiple workspaces, but only has access to one.
    val req =
      roleResolver
        .Request()
        .withAuthUserId("auth-user-1")
        .withWorkspaces(listOf(workspace1, workspace2))

    // user is workspace admin on workspace 1
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withWorkspaceId(workspace1),
      )

    // The user is not granted workspace access, because they need access to all requested workspaces
    assertEquals(baseRoles, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun grantedMultipleOrgs() {
    val org1 = UUID.randomUUID()
    val org2 = UUID.randomUUID()

    // user is requesting access to one org, but has access to multiple.
    val req =
      roleResolver
        .Request()
        .withAuthUserId("auth-user-1")
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
      AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER WORKSPACE_EDITOR 
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

    // user is requesting access to multiple workspaces.
    val req =
      roleResolver
        .Request()
        .withAuthUserId("auth-user-1")
        .withWorkspaces(listOf(workspace1, workspace2))

    // user is workspace admin on workspace 1,
    // and workspace runner on workspace 2.
    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns
      listOf(
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)
          .withWorkspaceId(workspace1),
        Permission()
          .withPermissionType(Permission.PermissionType.WORKSPACE_RUNNER)
          .withWorkspaceId(workspace2),
      )

    // The user is granted the minimum common role across the workspaces, which is reader.
    val expect = "AUTHENTICATED_USER WORKSPACE_READER WORKSPACE_RUNNER".toRoleSet()
    assertEquals(expect, req.roles())
    verify(exactly = 1) { permissionHandler.getPermissionsByAuthUserId("auth-user-1") }
  }

  @Test
  fun workspaceRoleViaResourceReference() {
    val workspace1 = UUID.randomUUID()
    val source1 = UUID.randomUUID()

    // user is requesting access to source1
    val req =
      roleResolver
        .Request()
        .withAuthUserId("auth-user-1")
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
        .Request()
        .withAuthUserId("auth-user-1")
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
    val req = roleResolver.Request().withAuthUserId("auth-user-1")
    assertThrows<ForbiddenProblem> {
      req.requireRole("FOO")
    }

    every { permissionHandler.getPermissionsByAuthUserId("auth-user-1") } returns emptyList()

    req.requireRole("AUTHENTICATED_USER")
  }
}

private fun String.toRoleSet() = this.trim().split("\\s+".toRegex()).toSet()
