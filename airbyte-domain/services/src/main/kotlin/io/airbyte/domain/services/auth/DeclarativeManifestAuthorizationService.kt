/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.data.services.WorkspaceService
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class DeclarativeManifestAuthorizationService(
  private val data: AuthorizationDataService,
  private val workspaces: WorkspaceService,
) {
  fun canUseSource(
    definition: UUID,
    workspace: UUID,
  ): Boolean {
    data.requireActorDefinitionType(definition, true)
    return workspaces.workspaceCanUseCustomDefinition(definition, workspace)
  }
}
