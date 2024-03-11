/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader.helpers;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.persistence.version_overrides.DefinitionVersionOverrideProvider;
import java.util.Optional;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link DefinitionVersionOverrideProvider} that does not override any versions.
 * Used for testing.
 */
public class NoOpDefinitionVersionOverrideProvider implements DefinitionVersionOverrideProvider {

  @Override
  public Optional<ActorDefinitionVersionWithOverrideStatus> getOverride(final ActorType actorType,
                                                                        final UUID actorDefinitionId,
                                                                        final UUID workspaceId,
                                                                        @Nullable final UUID actorId,
                                                                        final ActorDefinitionVersion defaultVersion) {
    return Optional.empty();
  }

}
