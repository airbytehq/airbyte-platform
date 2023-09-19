/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.persistence.version_overrides.DefinitionVersionOverrideProvider;
import java.util.Optional;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link DefinitionVersionOverrideProvider} that does not override any versions.
 * Used for testing.
 */
class NoOpDefinitionVersionOverrideProvider implements DefinitionVersionOverrideProvider {

  @Override
  public Optional<ActorDefinitionVersion> getOverride(final ActorType actorType,
                                                      final UUID actorDefinitionId,
                                                      final UUID workspaceId,
                                                      @Nullable final UUID actorId,
                                                      final ActorDefinitionVersion defaultVersion) {
    return Optional.empty();
  }

}
