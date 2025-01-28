/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader.helpers;

import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.persistence.version_overrides.DefinitionVersionOverrideProvider;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link DefinitionVersionOverrideProvider} that does not override any versions.
 * Used for testing.
 */
public class NoOpDefinitionVersionOverrideProvider implements DefinitionVersionOverrideProvider {

  @Override
  public Optional<ActorDefinitionVersionWithOverrideStatus> getOverride(final UUID actorDefinitionId,
                                                                        final UUID workspaceId,
                                                                        @Nullable final UUID actorId) {
    return Optional.empty();
  }

  @Override
  public List<ActorDefinitionVersionWithOverrideStatus> getOverrides(List<UUID> actorDefinitionIds,
                                                                     UUID workspaceId) {
    return List.of();
  }

}
