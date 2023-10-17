/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * This service is responsible for managing the lifecycle of actor definitions.
 */
public interface ActorDefinitionService {

  Set<UUID> getActorDefinitionIdsInUse() throws IOException;

  Map<UUID, Map.Entry<io.airbyte.config.ActorType, Version>> getActorDefinitionToProtocolVersionMap() throws IOException;

  Map<UUID, ActorDefinitionVersion> getActorDefinitionIdsToDefaultVersionsMap() throws IOException;

  int updateActorDefinitionsDockerImageTag(List<UUID> actorDefinitionIds, String targetImageTag) throws IOException;

  void writeActorDefinitionWorkspaceGrant(UUID actorDefinitionId, UUID scopeId, io.airbyte.config.ScopeType scopeType) throws IOException;

  boolean actorDefinitionWorkspaceGrantExists(UUID actorDefinitionId, UUID scopeId, io.airbyte.config.ScopeType scopeType) throws IOException;

  void deleteActorDefinitionWorkspaceGrant(UUID actorDefinitionId, UUID scopeId, io.airbyte.config.ScopeType scopeType) throws IOException;

  ActorDefinitionVersion writeActorDefinitionVersion(ActorDefinitionVersion actorDefinitionVersion) throws IOException;

  Optional<ActorDefinitionVersion> getActorDefinitionVersion(UUID actorDefinitionId, String dockerImageTag) throws IOException;

  ActorDefinitionVersion getActorDefinitionVersion(UUID actorDefinitionVersionId) throws IOException, ConfigNotFoundException;

  List<ActorDefinitionVersion> listActorDefinitionVersionsForDefinition(UUID actorDefinitionId) throws IOException;

  List<ActorDefinitionVersion> getActorDefinitionVersions(List<UUID> actorDefinitionVersionIds) throws IOException;

  void setActorDefaultVersion(UUID actorId, UUID actorDefinitionVersionId) throws IOException;

  List<ActorDefinitionBreakingChange> listBreakingChangesForActorDefinition(UUID actorDefinitionId) throws IOException;

  void setActorDefinitionVersionSupportStates(List<UUID> actorDefinitionVersionIds, ActorDefinitionVersion.SupportState supportState)
      throws IOException;

  List<ActorDefinitionBreakingChange> listBreakingChangesForActorDefinitionVersion(ActorDefinitionVersion actorDefinitionVersion) throws IOException;

  List<ActorDefinitionBreakingChange> listBreakingChanges() throws IOException;

  boolean scopeCanUseDefinition(final UUID actorDefinitionId, final UUID scopeId, final String scopeType) throws IOException;

}
