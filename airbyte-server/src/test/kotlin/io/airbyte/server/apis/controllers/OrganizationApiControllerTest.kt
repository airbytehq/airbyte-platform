/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.OrganizationCreateRequestBody
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationInfoRead
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.WorkspacePersistence
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID

class OrganizationApiControllerTest {
  private lateinit var organizationsHandler: OrganizationsHandler
  private lateinit var roleResolver: RoleResolver
  private lateinit var workspacePersistence: WorkspacePersistence
  private lateinit var currentUserService: CurrentUserService
  private lateinit var organizationApiController: OrganizationApiController

  private val organizationId = UUID.randomUUID()
  private val workspaceId1 = UUID.randomUUID()
  private val workspaceId2 = UUID.randomUUID()
  private val userId = UUID.randomUUID()
  private val organizationInfoRead =
    OrganizationInfoRead()
      .organizationId(organizationId)
      .organizationName("Test Organization")
      .sso(false)

  @BeforeEach
  fun setup() {
    organizationsHandler = mockk()
    roleResolver = mockk(relaxed = true)
    workspacePersistence = mockk()
    currentUserService = mockk()
    organizationApiController = OrganizationApiController(organizationsHandler, roleResolver, workspacePersistence, currentUserService)

    // Default behavior: organizationsHandler returns organization info
    every { organizationsHandler.getOrganizationInfo(organizationId) } returns organizationInfoRead

    // Default behavior: currentUserService returns a mock user
    val user = AuthenticatedUser()
    user.userId = userId
    user.email = "test@example.com"
    every { currentUserService.getCurrentUser() } returns user
  }

  @Test
  fun testGetOrganization() {
    every { organizationsHandler.getOrganization(any()) } returns OrganizationRead()
    val body = OrganizationIdRequestBody().organizationId(UUID.randomUUID())
    assertNotNull(organizationApiController.getOrganization(body))
  }

  @Test
  fun testUpdateOrganization() {
    every { organizationsHandler.updateOrganization(any()) } returns OrganizationRead()
    assertNotNull(organizationApiController.updateOrganization(OrganizationUpdateRequestBody()))
  }

  @Test
  fun testCreateOrganization() {
    every { organizationsHandler.createOrganization(any()) } returns OrganizationRead()
    assertNotNull(organizationApiController.createOrganization(OrganizationCreateRequestBody()))
  }

  @Test
  fun `getOrgInfo succeeds with organization-level permission`() {
    val request = mockk<RoleResolver.Request>(relaxed = true)
    every { roleResolver.newRequest() } returns request
    every { request.withCurrentAuthentication() } returns request
    every { request.withOrg(organizationId) } returns request
    every { request.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) } returns Unit // No exception = success

    val requestBody = OrganizationIdRequestBody().organizationId(organizationId)

    val result = organizationApiController.getOrgInfo(requestBody)

    assertNotNull(result)
    assertEquals(organizationId, result!!.organizationId)
    verify(exactly = 1) { organizationsHandler.getOrganizationInfo(organizationId) }
    verify(exactly = 0) { workspacePersistence.listWorkspacesByOrganizationId(any(), any(), any()) } // Should not check workspaces
  }

  @Test
  fun `getOrgInfo succeeds with workspace-level permission when org permission fails`() {
    val orgRequest = mockk<RoleResolver.Request>(relaxed = true)

    every { roleResolver.newRequest() } returns orgRequest

    // First request (org-level) - setup and failure
    every { orgRequest.withCurrentAuthentication() } returns orgRequest
    every { orgRequest.withOrg(organizationId) } returns orgRequest
    every { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) } throws ForbiddenProblem()

    // Mock workspace persistence returning workspaces user has access to
    val workspace1 = StandardWorkspace().apply { workspaceId = workspaceId1 }
    val workspace2 = StandardWorkspace().apply { workspaceId = workspaceId2 }
    every { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) } returns
      listOf(workspace1, workspace2)

    val requestBody = OrganizationIdRequestBody().organizationId(organizationId)

    val result = organizationApiController.getOrgInfo(requestBody)

    assertNotNull(result)
    assertEquals(organizationId, result!!.organizationId)
    verify(exactly = 1) { organizationsHandler.getOrganizationInfo(organizationId) }
    verify(exactly = 1) { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) }
    verify(exactly = 1) { currentUserService.getCurrentUser() }
  }

  @Test
  fun `getOrgInfo fails when user has no organization or workspace permissions`() {
    val orgRequest = mockk<RoleResolver.Request>(relaxed = true)

    every { roleResolver.newRequest() } returns orgRequest

    // First request (org-level) - failure
    every { orgRequest.withCurrentAuthentication() } returns orgRequest
    every { orgRequest.withOrg(organizationId) } returns orgRequest
    val orgForbiddenException = ForbiddenProblem()
    every { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) } throws orgForbiddenException

    // Mock workspace persistence returning empty list (user has no workspace access)
    every { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) } returns emptyList()

    val requestBody = OrganizationIdRequestBody().organizationId(organizationId)

    val exception =
      assertThrows<ForbiddenProblem> {
        organizationApiController.getOrgInfo(requestBody)
      }

    // Should re-throw the original org permission error
    assertEquals(orgForbiddenException, exception)
    verify(exactly = 0) { organizationsHandler.getOrganizationInfo(organizationId) } // Should not call handler
    verify(exactly = 1) { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) }
    verify(exactly = 1) { currentUserService.getCurrentUser() }
  }

  @Test
  fun `getOrgInfo fails when organization has no workspaces and user lacks org permission`() {
    val orgRequest = mockk<RoleResolver.Request>(relaxed = true)

    every { roleResolver.newRequest() } returns orgRequest

    // Organization-level permission failure
    every { orgRequest.withCurrentAuthentication() } returns orgRequest
    every { orgRequest.withOrg(organizationId) } returns orgRequest
    val forbiddenException = ForbiddenProblem()
    every { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) } throws forbiddenException

    // Empty workspace list
    every { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) } returns emptyList()

    val requestBody = OrganizationIdRequestBody().organizationId(organizationId)

    val exception =
      assertThrows<ForbiddenProblem> {
        organizationApiController.getOrgInfo(requestBody)
      }

    // Should re-throw the original org permission error
    assertEquals(forbiddenException, exception)
    verify(exactly = 0) { organizationsHandler.getOrganizationInfo(organizationId) }
    verify(exactly = 1) { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) }
    verify(exactly = 1) { currentUserService.getCurrentUser() }
  }

  @Test
  fun `getOrgInfo should succeed when user has access to only some workspaces in organization`() {
    val orgRequest = mockk<RoleResolver.Request>(relaxed = true)

    every { roleResolver.newRequest() } returns orgRequest

    // First request (org-level) - failure
    every { orgRequest.withCurrentAuthentication() } returns orgRequest
    every { orgRequest.withOrg(organizationId) } returns orgRequest
    every { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) } throws ForbiddenProblem()

    // Mock workspace persistence returning workspaces that user has access to (only workspace1, not workspace2)
    val workspace1 = StandardWorkspace().apply { workspaceId = workspaceId1 }
    every { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) } returns listOf(workspace1)

    val requestBody = OrganizationIdRequestBody().organizationId(organizationId)

    // Should succeed because user has access to at least one workspace (workspace1)
    val result = organizationApiController.getOrgInfo(requestBody)

    assertNotNull(result)
    assertEquals(organizationId, result!!.organizationId)
    verify(exactly = 1) { organizationsHandler.getOrganizationInfo(organizationId) }
    verify(exactly = 1) { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) }
    verify(exactly = 1) { currentUserService.getCurrentUser() }
  }

  @Test
  fun `getOrgInfo should fail when user has no workspace access in organization`() {
    val orgRequest = mockk<RoleResolver.Request>(relaxed = true)

    every { roleResolver.newRequest() } returns orgRequest

    // First request (org-level) - failure
    every { orgRequest.withCurrentAuthentication() } returns orgRequest
    every { orgRequest.withOrg(organizationId) } returns orgRequest
    val forbiddenException = ForbiddenProblem()
    every { orgRequest.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER) } throws forbiddenException

    // Mock workspace persistence returning empty list (user has no workspace access)
    every { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) } returns emptyList()

    val requestBody = OrganizationIdRequestBody().organizationId(organizationId)

    // Should fail because user has no access to any workspace in the organization
    val exception =
      assertThrows<ForbiddenProblem> {
        organizationApiController.getOrgInfo(requestBody)
      }

    // Should re-throw the original org permission error
    assertEquals(forbiddenException, exception)
    verify(exactly = 0) { organizationsHandler.getOrganizationInfo(organizationId) }
    verify(exactly = 1) { workspacePersistence.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) }
    verify(exactly = 1) { currentUserService.getCurrentUser() }
  }
}
