/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

/** Uses the same effective workspace membership query as organization discovery. */
@Singleton
class OrganizationAccessService(
  private val users: UserPersistence,
  private val workspaces: WorkspacePersistence,
) {
  fun hasAccessibleWorkspace(
    authUserId: String,
    organizationId: UUID,
  ): Boolean {
    val user = users.getUserByAuthId(authUserId)
    if (user.isEmpty) return false

    return workspaces.listWorkspacesInOrganizationByUserId(organizationId, user.get().userId, Optional.empty()).isNotEmpty()
  }
}
