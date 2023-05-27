/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Defines the interface for a class that can provide overrides for actor definition versions.
 */
public interface DefinitionVersionOverrideProvider {

  Optional<ActorDefinitionVersion> getOverride(final ActorType actorType,
                                               final UUID actorDefinitionId,
                                               final UUID workspaceId,
                                               @Nullable final UUID actorId,
                                               final ActorDefinitionVersion defaultVersion);

}
