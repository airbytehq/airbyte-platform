/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.helpers

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.WorkspacePersistence
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID

class OrganizationAccessAuthorizationHelperTest {
  private val roleResolver = mockk<RoleResolver>()
  private val workspacePersistence = mockk<WorkspacePersistence>()
  private val currentUserService = mockk<CurrentUserService>()

  private lateinit var authorizationHelper: OrganizationAccessAuthorizationHelper
  private val organizationId = UUID.randomUUID()
  private val userId = UUID.randomUUID()
  private val workspaceId1 = UUID.randomUUID()
  private val workspaceId2 = UUID.randomUUID()
  private val authenticatedUser = AuthenticatedUser().withUserId(userId)

  @BeforeEach
  fun setUp() {
    authorizationHelper = OrganizationAccessAuthorizationHelper(roleResolver, workspacePersistence, currentUserService)

    // Default: current user service returns our test user
    every { currentUserService.getCurrentUser() } returns authenticatedUser
  }

  @Test
  fun `validateOrganizationOrWorkspaceAccess succeeds with organization member role`() {
    val orgRequest = mockk<RoleResolver.Request>(relaxed = true)
    every { roleResolver.newRequest() } returns orgRequest
    every { orgRequest.withCurrentAuthentication() } returns orgRequest
    every { orgRequest.withOrg(organizationId) } returns orgRequest
    every { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) } returns Unit

    assertDoesNotThrow { authorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) }

    verify(exactly = 1) { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) }
    verify(exactly = 0) { workspacePersistence.listWorkspacesInOrganizationByUserId(any(), any(), any()) }
  }

  @Test
  fun `validateOrganizationOrWorkspaceAccess succeeds with workspace access when org role fails`() {
    val orgRequest = mockk<RoleResolver.Request>(relaxed = true)
    every { roleResolver.newRequest() } returns orgRequest
    every { orgRequest.withCurrentAuthentication() } returns orgRequest
    every { orgRequest.withOrg(organizationId) } returns orgRequest
    val forbiddenException = ForbiddenProblem()
    every { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) } throws forbiddenException

    // Mock workspace access
    val workspace1 = StandardWorkspace().apply { workspaceId = workspaceId1 }
    val workspace2 = StandardWorkspace().apply { workspaceId = workspaceId2 }
    every { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) } returns
      listOf(workspace1, workspace2)

    assertDoesNotThrow { authorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) }

    verify(exactly = 1) { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) }
    verify(exactly = 1) { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) }
  }

  @Test
  fun `validateOrganizationOrWorkspaceAccess succeeds with single workspace access`() {
    val orgRequest = mockk<RoleResolver.Request>(relaxed = true)
    every { roleResolver.newRequest() } returns orgRequest
    every { orgRequest.withCurrentAuthentication() } returns orgRequest
    every { orgRequest.withOrg(organizationId) } returns orgRequest
    val forbiddenException = ForbiddenProblem()
    every { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) } throws forbiddenException

    // Mock single workspace access
    val workspace1 = StandardWorkspace().apply { workspaceId = workspaceId1 }
    every { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) } returns listOf(workspace1)

    assertDoesNotThrow { authorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId) }

    verify(exactly = 1) { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) }
    verify(exactly = 1) { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) }
  }

  @Test
  fun `validateOrganizationOrWorkspaceAccess fails when no organization or workspace access`() {
    val orgRequest = mockk<RoleResolver.Request>(relaxed = true)
    every { roleResolver.newRequest() } returns orgRequest
    every { orgRequest.withCurrentAuthentication() } returns orgRequest
    every { orgRequest.withOrg(organizationId) } returns orgRequest
    val forbiddenException = ForbiddenProblem()
    every { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) } throws forbiddenException

    // Mock no workspace access
    every { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) } returns emptyList()

    val exception =
      assertThrows<ForbiddenProblem> {
        authorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId)
      }

    // Should re-throw the original organization permission exception
    assert(exception === forbiddenException)
    verify(exactly = 1) { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) }
    verify(exactly = 1) { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) }
  }

  @Test
  fun `validateOrganizationOrWorkspaceAccess handles currentUserService exceptions`() {
    val orgRequest = mockk<RoleResolver.Request>(relaxed = true)
    every { roleResolver.newRequest() } returns orgRequest
    every { orgRequest.withCurrentAuthentication() } returns orgRequest
    every { orgRequest.withOrg(organizationId) } returns orgRequest
    val forbiddenException = ForbiddenProblem()
    every { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) } throws forbiddenException

    // Mock currentUserService to throw an exception
    every { currentUserService.getCurrentUser() } throws RuntimeException("User service error")

    val exception =
      assertThrows<RuntimeException> {
        authorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId)
      }

    assert(exception.message == "User service error")
    verify(exactly = 1) { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) }
    verify(exactly = 1) { currentUserService.getCurrentUser() }
    verify(exactly = 0) { workspacePersistence.listWorkspacesInOrganizationByUserId(any(), any(), any()) }
  }

  @Test
  fun `validateOrganizationOrWorkspaceAccess handles workspacePersistence exceptions`() {
    val orgRequest = mockk<RoleResolver.Request>(relaxed = true)
    every { roleResolver.newRequest() } returns orgRequest
    every { orgRequest.withCurrentAuthentication() } returns orgRequest
    every { orgRequest.withOrg(organizationId) } returns orgRequest
    val forbiddenException = ForbiddenProblem()
    every { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) } throws forbiddenException

    // Mock workspacePersistence to throw an exception
    every { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) } throws
      RuntimeException("Workspace service error")

    val exception =
      assertThrows<RuntimeException> {
        authorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId)
      }

    assert(exception.message == "Workspace service error")
    verify(exactly = 1) { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) }
    verify(exactly = 1) { currentUserService.getCurrentUser() }
    verify(exactly = 1) { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) }
  }

  @Test
  fun `validateOrganizationOrWorkspaceAccess handles roleResolver exceptions`() {
    every { roleResolver.newRequest() } throws RuntimeException("Role resolver error")

    val exception =
      assertThrows<RuntimeException> {
        authorizationHelper.validateOrganizationOrWorkspaceAccess(organizationId)
      }

    assert(exception.message == "Role resolver error")
    verify(exactly = 1) { roleResolver.newRequest() }
    verify(exactly = 0) { currentUserService.getCurrentUser() }
    verify(exactly = 0) { workspacePersistence.listWorkspacesInOrganizationByUserId(any(), any(), any()) }
  }
}
