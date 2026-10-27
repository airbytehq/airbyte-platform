/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.config.ScopeType
import io.airbyte.config.UserInvitation
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.UserInvitationService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class MembershipAuthorizationDataServiceTest {
  private val data = mockk<AuthorizationDataService>()
  private val permissions = mockk<PermissionService>()
  private val users = mockk<UserPersistence>()
  private val invitations = mockk<UserInvitationService>()
  private val service = MembershipAuthorizationDataService(data, permissions, users, invitations)
  private val workspace = UUID(0, 1)
  private val organization = UUID(0, 2)
  private val id = UUID(0, 3)
  private val user = UUID(0, 8)

  @Test
  fun `group permission parent projection does not resolve its possibly deleted workspace or require a user email`() {
    every { permissions.getPermission(id) } returns Permission().withPermissionId(id).withWorkspaceId(workspace).withGroupId(UUID(0, 9))
    assertEquals(Optional.of(UUID(0, 9)), service.permissionGroup(id))
    verify(exactly = 0) { data.workspaceAndOrganization(any()) }
    every { users.getUserByAuthId("subject") } returns Optional.of(AuthenticatedUser().withUserId(user).withAuthUserId("subject"))
    assertTrue(service.currentIdentity("subject").email.isEmpty)
  }

  @Test
  fun `permission metadata copies saved beneficiary group kind and actual sole scope without treating malformed rows as global`() {
    every { data.workspaceAndOrganization(workspace) } returns WorkspaceAndOrganization(workspace, organization)
    val saved =
      Permission()
        .withPermissionId(
          id,
        ).withUserId(user)
        .withWorkspaceId(workspace)
        .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
    every { permissions.getPermission(id) } returns saved
    val snapshot = service.permission(id)
    saved.workspaceId = UUID(0, 99)
    assertEquals(AuthorizationTarget.Workspace(WorkspaceAndOrganization(workspace, organization)), snapshot.target)
    assertEquals(Optional.of(user), snapshot.userId)
    assertTrue(snapshot.groupId.isEmpty)
    saved.withWorkspaceId(workspace).withOrganizationId(organization)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.permission(id) }
    saved.withWorkspaceId(null).withOrganizationId(null)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.permission(id) }
    saved.withPermissionType(Permission.PermissionType.INSTANCE_ADMIN)
    assertEquals(AuthorizationTarget.Global, service.permission(id).target)
    saved.permissionId = UUID(0, 99)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.permission(id) }
  }

  @Test
  fun `group member candidates need their direct organization row rather than workspace access or another user row`() {
    every { data.requireUser(user) } just Runs
    every { permissions.getPermissionsForUser(user) } returns listOf(Permission().withUserId(user).withWorkspaceId(workspace))
    assertFalse(service.isDirectOrganizationMember(user, organization))
    every { permissions.getPermissionsForUser(user) } returns listOf(Permission().withUserId(UUID(0, 99)).withOrganizationId(organization))
    assertFalse(service.isDirectOrganizationMember(user, organization))
    every { permissions.getPermissionsForUser(user) } returns listOf(Permission().withUserId(user).withOrganizationId(organization))
    assertTrue(service.isDirectOrganizationMember(user, organization))
    val failure = IOException("membership metadata unavailable")
    every { permissions.getPermissionsForUser(user) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.isDirectOrganizationMember(user, organization) })
  }

  @Test
  fun `invitations preserve exact saved selector recipient case and scope while current identity uses saved or community user`() {
    every { data.workspaceAndOrganization(workspace) } returns WorkspaceAndOrganization(workspace, organization)
    val invitation =
      UserInvitation()
        .withId(
          id,
        ).withInviteCode(
          "exact",
        ).withScopeId(
          workspace,
        ).withScopeType(ScopeType.WORKSPACE)
        .withInvitedEmail("Exact@example.com")
        .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
    every { invitations.getUserInvitationByInviteCode("exact") } returns invitation
    assertEquals("Exact@example.com", service.invitationByCode("exact").email)
    invitation.inviteCode = "other"
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.invitationByCode("exact") }
    every { users.getUserByAuthId("subject") } returns
      Optional.of(AuthenticatedUser().withUserId(user).withAuthUserId("subject").withEmail("exact@example.com"))
    assertEquals(AuthorizationUserIdentity(user, Optional.of("exact@example.com")), service.currentIdentity("subject"))
    every { users.getDefaultUser() } returns
      Optional.of(AuthenticatedUser().withUserId(user).withAuthUserId("community").withEmail("exact@example.com"))
    assertEquals(AuthorizationUserIdentity(user, Optional.of("exact@example.com")), service.defaultIdentity())
    val failure = IOException("invitation metadata unavailable")
    every { invitations.getUserInvitationById(id) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.invitationById(id) })
  }
}
