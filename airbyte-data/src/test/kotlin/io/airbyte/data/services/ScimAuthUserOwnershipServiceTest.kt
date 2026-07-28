/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.ScimAuthUserRepository
import io.airbyte.data.repositories.entities.ScimAuthUser
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.UUID
import io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider as DbAuthProvider

class ScimAuthUserOwnershipServiceTest {
  private val repository = mockk<ScimAuthUserRepository>()
  private val service = ScimAuthUserOwnershipService(repository)

  @Test
  fun `ambiguous raw subject fails before the guarded operation`() {
    val firstOwner = UUID.randomUUID()
    val secondOwner = UUID.randomUUID()
    every { repository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every { repository.findByAuthUserIdForUpdate(AUTH_USER_ID) } returns
      listOf(
        identity(firstOwner, DbAuthProvider.keycloak),
        identity(secondOwner, DbAuthProvider.google_identity_platform),
      )
    var operationCalled = false

    assertThatThrownBy {
      service.withUniqueOwner(AUTH_USER_ID, firstOwner) {
        operationCalled = true
      }
    }.isInstanceOf(IllegalStateException::class.java)

    assertThat(operationCalled).isFalse()
  }

  @Test
  fun `unique expected owner executes the guarded operation`() {
    val owner = UUID.randomUUID()
    every { repository.acquireIdentityLock(AUTH_USER_ID) } returns true
    every { repository.findByAuthUserIdForUpdate(AUTH_USER_ID) } returns
      listOf(identity(owner, DbAuthProvider.keycloak))

    val result = service.withUniqueOwner(AUTH_USER_ID, owner) { "allowed" }

    assertThat(result).isEqualTo("allowed")
  }

  private fun identity(
    userId: UUID,
    provider: DbAuthProvider,
  ): ScimAuthUser =
    ScimAuthUser(
      userId = userId,
      authUserId = AUTH_USER_ID,
      authProvider = provider,
    )

  companion object {
    private const val AUTH_USER_ID = "shared-subject"
  }
}
