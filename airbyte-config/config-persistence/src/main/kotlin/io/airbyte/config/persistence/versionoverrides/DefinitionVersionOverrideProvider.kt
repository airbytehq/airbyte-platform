/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.versionoverrides

import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus
import jakarta.annotation.Nullable
import java.util.Optional
import java.util.UUID

/**
 * Defines the interface for a class that can provide overrides for actor definition versions. This
 * interface is used to allow for a different implementation of the FF override provider in Cloud
 * and OSS, namely for adding additional contexts that are only available in Cloud.
 */
interface DefinitionVersionOverrideProvider {
  fun getOverride(
    actorDefinitionId: UUID,
    workspaceId: UUID,
    @Nullable actorId: UUID?,
  ): Optional<ActorDefinitionVersionWithOverrideStatus>

  fun getOverrides(
    actorDefinitionIds: List<UUID>,
    workspaceId: UUID,
  ): List<ActorDefinitionVersionWithOverrideStatus>
}
