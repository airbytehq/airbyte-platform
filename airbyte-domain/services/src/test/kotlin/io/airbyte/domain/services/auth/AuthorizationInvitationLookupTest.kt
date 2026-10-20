/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Organization
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.UserInvitation
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.UserInvitationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class AuthorizationInvitationLookupTest {
  private val id = UUID.randomUUID()
  private val workspace = UUID.randomUUID()
  private val org = UUID.randomUUID()
  private val invitations = mockk<UserInvitationService>()
  private val workspaces = mockk<WorkspaceService>()
  private val organizations = mockk<OrganizationService>()
  private val service =
    AuthorizationDataService(
      workspaces,
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      organizations,
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      mockk(),
      invitations,
      privateLinks = mockk(),
      connectionEvents = mockk(),
    )

  @Test
  fun `invitation UUID and code resolve saved workspace or organization`() {
    every { workspaces.getStandardWorkspaceNoSecrets(workspace, false) } returns
      StandardWorkspace().withWorkspaceId(workspace).withOrganizationId(org)
    every { organizations.getOrganization(org) } returns Optional.of(Organization().withOrganizationId(org))
    every { invitations.getUserInvitationById(id) } returns UserInvitation().withId(id).withScopeType(ScopeType.WORKSPACE).withScopeId(workspace)
    every { invitations.getUserInvitationByInviteCode("code") } returns
      UserInvitation().withInviteCode("code").withScopeType(ScopeType.ORGANIZATION).withScopeId(org)
    assertEquals(AuthorizationTarget.Workspace(WorkspaceAndOrganization(workspace, org)), service.invitationTarget(id))
    assertEquals(AuthorizationTarget.Organization(org), service.invitationTargetByCode("code"))
  }

  @Test
  fun `missing invitation and missing scope stay not found`() {
    every { invitations.getUserInvitationById(id) } throws ConfigNotFoundException(ConfigNotFoundType.USER_INVITATION, id)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.invitationTarget(id) }
    every { invitations.getUserInvitationById(id) } returns UserInvitation().withId(id).withScopeType(ScopeType.WORKSPACE)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.invitationTarget(id) }
    every { invitations.getUserInvitationByInviteCode("code") } returns UserInvitation().withInviteCode("code").withScopeId(org)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.invitationTargetByCode("code") }
  }

  @Test
  fun `wrong saved invitation identity fails closed and storage errors propagate`() {
    every { invitations.getUserInvitationById(id) } returns
      UserInvitation().withId(UUID.randomUUID()).withScopeType(ScopeType.ORGANIZATION).withScopeId(org)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.invitationTarget(id) }
    every { invitations.getUserInvitationByInviteCode("code") } returns
      UserInvitation().withInviteCode("wrong").withScopeType(ScopeType.ORGANIZATION).withScopeId(org)
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.invitationTargetByCode("code") }
    val failure = IOException("invitations unavailable")
    every { invitations.getUserInvitationById(id) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.invitationTarget(id) })
  }
}
