/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import io.airbyte.config.ActorDefinitionVersion;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Defines the interface for a class that can provide static overrides for actor definition
 * versions. This is used to allow for a different implementation of the override provider in Cloud
 * and OSS.
 */
public interface LocalDefinitionVersionOverrideProvider {

  Optional<ActorDefinitionVersion> getOverride(final UUID actorDefinitionId,
                                               final UUID workspaceId,
                                               @Nullable final UUID actorId,
                                               final ActorDefinitionVersion defaultVersion);

}
