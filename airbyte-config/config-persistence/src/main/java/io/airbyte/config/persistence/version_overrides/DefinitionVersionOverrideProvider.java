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
 * Defines the interface for a class that can provide overrides for actor definition versions. This
 * interface is used to allow for a different implementation of the FF override provider in Cloud
 * and OSS, namely for adding additional contexts that are only available in Cloud.
 */
public interface DefinitionVersionOverrideProvider {

  Optional<ActorDefinitionVersion> getOverride(final ActorType actorType,
                                               final UUID actorDefinitionId,
                                               final UUID workspaceId,
                                               @Nullable final UUID actorId,
                                               final ActorDefinitionVersion defaultVersion);

}
