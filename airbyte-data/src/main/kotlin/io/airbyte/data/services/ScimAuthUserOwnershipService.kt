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
    authUserRepository.acquireIdentityLock(authUserId)
    val owners =
      authUserRepository
        .findByAuthUserIdForUpdate(authUserId)
        .map { it.userId }
        .distinct()
    check(owners.size == 1 && (expectedUserId == null || owners.single() == expectedUserId)) {
      "Authentication identity $authUserId is not uniquely owned by the expected user."
    }
    return operation()
  }
}
