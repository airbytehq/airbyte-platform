/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Defines the interface for a class that can provide overrides for actor definition versions. This
 * interface is used to allow for a different implementation of the FF override provider in Cloud
 * and OSS, namely for adding additional contexts that are only available in Cloud.
 */
public interface DefinitionVersionOverrideProvider {

  Optional<ActorDefinitionVersionWithOverrideStatus> getOverride(final UUID actorDefinitionId,
                                                                 final UUID workspaceId,
                                                                 @Nullable final UUID actorId);

  List<ActorDefinitionVersionWithOverrideStatus> getOverrides(final List<UUID> actorDefinitionIds,
                                                              final UUID workspaceId);

}
