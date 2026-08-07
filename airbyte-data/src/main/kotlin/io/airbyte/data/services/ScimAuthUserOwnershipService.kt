/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.ScimAuthUserRepository
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Serializes operations keyed by a raw authentication subject and fails closed unless that subject
 * has exactly one Airbyte user owner.
 */
@Singleton
open class ScimAuthUserOwnershipService(
  private val authUserRepository: ScimAuthUserRepository,
) {
  @Transactional("config")
  open fun <T> withUniqueOwner(
    authUserId: String,
    expectedUserId: UUID? = null,
    operation: () -> T,
  ): T {
    val owner = uniqueOwner(authUserId)
    check(expectedUserId == null || owner == expectedUserId) {
      "Authentication identity $authUserId is not uniquely owned by the expected user."
    }
    return operation()
  }

  @Transactional("config")
  open fun uniqueOwner(authUserId: String): UUID {
    authUserRepository.acquireIdentityLock(authUserId)
    val owners =
      authUserRepository
        .findByAuthUserIdForUpdate(authUserId)
        .map { it.userId }
        .distinct()
    check(owners.size == 1) {
      "Authentication identity $authUserId is not uniquely owned by the expected user."
    }
    return owners.single()
  }
}
