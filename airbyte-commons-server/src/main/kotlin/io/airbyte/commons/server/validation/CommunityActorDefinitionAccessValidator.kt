/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.validation

import jakarta.inject.Singleton
import java.util.UUID

/**
 * Default Community edition implementation of [ActorDefinitionAccessValidator]. Does nothing,
 * because Community edition does not have any access restrictions/auth.
 */
@Singleton
class CommunityActorDefinitionAccessValidator : ActorDefinitionAccessValidator {
  override fun validateWriteAccess(actorDefinitionId: UUID) {
    // do nothing
  }
}
