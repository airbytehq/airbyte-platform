/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.PermissionService
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class PublicWorkspaceAuthorizationServiceTest {
  private val users = mockk<UserPersistence>()
  private val permissions = mockk<PermissionService>()
  private val service = PublicWorkspaceAuthorizationService(users, permissions)
  private val user = UUID(0, 1)
  private val organization = UUID(0, 2)

  @Test
  fun `implicit organization preserves the single existing organization and the new user branch`() {
    every { users.getUserByAuthId("caller") } returns Optional.of(AuthenticatedUser().withUserId(user).withAuthUserId("caller"))
    every { permissions.getPermissionsForUser(user) } returns listOf(Permission().withOrganizationId(organization))
    assertEquals(Optional.of(organization), service.implicitOrganization("caller", true))
    every { permissions.getPermissionsForUser(user) } returns emptyList()
    assertEquals(Optional.empty<UUID>(), service.implicitOrganization("caller", true))
    every { permissions.getPermissionsForUser(user) } returns listOf(Permission().withWorkspaceId(UUID(0, 4)))
    assertEquals(Optional.empty<UUID>(), service.implicitOrganization("caller", true))
  }

  @Test
  fun `implicit organization preserves ambiguity and lookup errors instead of selecting an arbitrary tenant`() {
    every { users.getUserByAuthId("caller") } returns Optional.of(AuthenticatedUser().withUserId(user).withAuthUserId("caller"))
    every { permissions.getPermissionsForUser(user) } returns
      listOf(Permission().withOrganizationId(organization), Permission().withOrganizationId(UUID(0, 3)))
    assertThrows(IllegalStateException::class.java) { service.implicitOrganization("caller", true) }
    val failure = IOException("permissions unavailable")
    every { permissions.getPermissionsForUser(user) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.implicitOrganization("caller", true) })
  }

  @Test
  fun `no auth resolves the saved default user while a mismatched authenticated subject fails`() {
    every { users.getDefaultUser() } returns Optional.of(AuthenticatedUser().withUserId(user).withAuthUserId("community"))
    every { permissions.getPermissionsForUser(user) } returns listOf(Permission().withOrganizationId(organization))
    assertEquals(Optional.of(organization), service.implicitOrganization("ignored", false))
    every { users.getUserByAuthId("caller") } returns Optional.of(AuthenticatedUser().withUserId(user).withAuthUserId("different"))
    assertThrows(IllegalStateException::class.java) { service.implicitOrganization("caller", true) }
    every { users.getUserByAuthId("caller") } returns Optional.empty()
    assertThrows(NoSuchElementException::class.java) { service.implicitOrganization("caller", true) }
  }
}
