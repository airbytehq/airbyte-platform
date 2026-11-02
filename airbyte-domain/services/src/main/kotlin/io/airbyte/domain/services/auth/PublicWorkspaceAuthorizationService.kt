/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.services.PermissionService
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

/** Reads saved direct memberships without creating an organization or permission. */
@Singleton
class PublicWorkspaceAuthorizationService(
  private val users: UserPersistence,
  private val permissions: PermissionService,
) {
  fun implicitOrganization(
    subject: String,
    authenticated: Boolean,
  ): Optional<UUID> {
    val organizations = organizationMemberships(subject, authenticated)
    check(organizations.size <= 1) { "User is associated with more than one organization. Please specify an organization id." }
    return Optional.ofNullable(organizations.singleOrNull())
  }

  fun organizationMemberships(
    subject: String,
    authenticated: Boolean,
  ): List<UUID> {
    val user: AuthenticatedUser = (if (authenticated) users.getUserByAuthId(subject) else users.getDefaultUser()).orElseThrow()
    if (authenticated) check(user.authUserId == subject) { "Authentication identity does not match the saved user" }
    return permissions.getPermissionsForUser(user.userId).mapNotNull { it.organizationId }
  }
}
