/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader.helpers

import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus
import io.airbyte.config.persistence.versionoverrides.DefinitionVersionOverrideProvider
import java.util.Optional
import java.util.UUID

/**
 * Implementation of [DefinitionVersionOverrideProvider] that does not override any versions.
 * Used for testing.
 */
class NoOpDefinitionVersionOverrideProvider : DefinitionVersionOverrideProvider {
  override fun getOverride(
    actorDefinitionId: UUID,
    workspaceId: UUID,
    actorId: UUID?,
  ): Optional<ActorDefinitionVersionWithOverrideStatus> = Optional.empty()

  override fun getOverrides(
    actorDefinitionIds: List<UUID>,
    workspaceId: UUID,
  ): List<ActorDefinitionVersionWithOverrideStatus> = listOf()
}
