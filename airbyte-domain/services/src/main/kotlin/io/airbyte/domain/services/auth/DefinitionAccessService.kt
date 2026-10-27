/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.persistence.UserPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.services.WorkspaceService
import jakarta.inject.Singleton
import java.util.UUID

/** Existing custom-definition availability through an active readable workspace, using metadata only. */
@Singleton
class DefinitionAccessService(
  private val users: UserPersistence,
  private val workspaces: WorkspacePersistence,
  private val definitions: WorkspaceService,
) {
  fun currentUserId(subject: String): UUID {
    val user = users.getUserByAuthId(subject).orElseThrow()
    check(user.authUserId == subject) { "Saved user does not match the authenticated subject" }
    return user.userId ?: throw NoSuchElementException("Saved user has no ID")
  }

  fun hasAccessibleCustomDefinition(
    userId: UUID,
    definitionId: UUID,
  ): Boolean = workspaces.listActiveWorkspaceIdsByUserId(userId).any { definitions.workspaceCanUseCustomDefinition(definitionId, it) }
}
