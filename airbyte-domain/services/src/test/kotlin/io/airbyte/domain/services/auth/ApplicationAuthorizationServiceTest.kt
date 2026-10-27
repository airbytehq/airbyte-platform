/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.ApplicationService
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class ApplicationAuthorizationServiceTest {
  private val users = mockk<UserPersistence>()
  private val applications = mockk<ApplicationService>()
  private val service = ApplicationAuthorizationService(users, applications)

  @Test
  fun `application current owner uses unique auth subject while no auth uses the real default user's subject`() {
    every { users.getUserByAuthId("caller") } returns Optional.of(AuthenticatedUser().withUserId(UUID(0, 8)).withAuthUserId("caller"))
    every { users.getDefaultUser() } returns Optional.of(AuthenticatedUser().withUserId(UUID(0, 9)).withAuthUserId("community-subject"))
    assertEquals("caller", service.currentUserSubject("caller"))
    assertEquals("community-subject", service.defaultUserSubject())
    every { applications.requireApplicationOwner("community-subject", UUID(0, 16)) } just Runs
    service.requireApplicationOwner(service.defaultUserSubject(), UUID(0, 16))
    verify(exactly = 1) { applications.requireApplicationOwner("community-subject", UUID(0, 16)) }
    verify(exactly = 0) { applications.listApplicationsByUser(any()) }
    verify(exactly = 0) { applications.getToken(any(), any()) }
  }

  @Test
  fun `missing ambiguous wrong saved subjects and lookup errors cannot approve an application owner`() {
    every { users.getUserByAuthId("caller") } returns Optional.empty()
    assertThrows(NoSuchElementException::class.java) { service.currentUserSubject("caller") }
    every { users.getUserByAuthId("caller") } returns Optional.of(AuthenticatedUser().withUserId(UUID(0, 8)).withAuthUserId("different"))
    assertThrows(IllegalStateException::class.java) { service.currentUserSubject("caller") }
    val failure = IOException("current owner unavailable")
    every { users.getUserByAuthId("caller") } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.currentUserSubject("caller") })
    verify(exactly = 0) { applications.requireApplicationOwner(any(), any()) }
  }
}
