/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.ApplicationService
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

/** Nonsecret current-user and provider ownership metadata; application credentials stay in business services. */
@Singleton
@Requires(bean = ApplicationService::class)
class ApplicationAuthorizationService(
  private val users: UserPersistence,
  private val applications: ApplicationService,
) {
  fun currentUserSubject(authUserId: String): String {
    val subject = subject(users.getUserByAuthId(authUserId))
    check(subject == authUserId) { "Authentication identity does not match the saved user" }
    return subject
  }

  fun defaultUserSubject(): String = subject(users.getDefaultUser())

  fun requireApplicationOwner(
    authUserId: String,
    applicationId: UUID,
  ) {
    applications.requireApplicationOwner(authUserId, applicationId)
  }

  private fun subject(user: Optional<AuthenticatedUser>): String {
    val subject: String? = user.orElseThrow().authUserId
    check(!subject.isNullOrBlank()) { "Saved user has no authentication identity" }
    return subject
  }
}
