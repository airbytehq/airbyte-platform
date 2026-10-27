/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class OrganizationAccessServiceTest {
  private val userId = UUID(0, 1)
  private val organizationId = UUID(0, 2)
  private val users = mockk<UserPersistence>()
  private val workspaces = mockk<WorkspacePersistence>()
  private val service = OrganizationAccessService(users, workspaces)

  @Test
  fun `authentication subject is converted to the unique internal user before querying the requested organization`() {
    every { users.getUserByAuthId("external-subject") } returns Optional.of(AuthenticatedUser().withUserId(userId))
    every { workspaces.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) } returns listOf(StandardWorkspace())

    assertTrue(service.hasAccessibleWorkspace("external-subject", organizationId))
    verify(exactly = 1) { workspaces.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) }
  }

  @Test
  fun `empty accessible workspace result does not grant organization access`() {
    every { users.getUserByAuthId("external-subject") } returns Optional.of(AuthenticatedUser().withUserId(userId))
    every { workspaces.listWorkspacesInOrganizationByUserId(organizationId, userId, Optional.empty()) } returns emptyList()

    assertFalse(service.hasAccessibleWorkspace("external-subject", organizationId))
  }

  @Test
  fun `missing user denies access while dependency errors propagate`() {
    every { users.getUserByAuthId("external-subject") } returns Optional.empty()
    assertFalse(service.hasAccessibleWorkspace("external-subject", organizationId))
    verify(exactly = 0) { workspaces.listWorkspacesInOrganizationByUserId(any(), any(), any()) }

    val failure = IOException("identity unavailable")
    every { users.getUserByAuthId("external-subject") } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.hasAccessibleWorkspace("external-subject", organizationId) })
  }
}
