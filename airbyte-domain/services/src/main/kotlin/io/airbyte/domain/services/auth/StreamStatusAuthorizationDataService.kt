/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.data.repositories.StreamStatusAuthorizationRepository
import io.airbyte.data.repositories.entities.StreamStatusAuthorization
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class StreamStatusAuthorizationDataService(
  private val statuses: StreamStatusAuthorizationRepository,
) {
  fun status(
    id: UUID,
    workspaceId: UUID,
  ): StreamStatusAuthorization {
    val saved = statuses.findByIdAndWorkspaceId(id, workspaceId).orElseThrow { AuthorizationResourceNotFoundException() }
    if (saved.id != id || saved.workspaceId != workspaceId) throw AuthorizationResourceNotFoundException()
    return saved
  }
}
