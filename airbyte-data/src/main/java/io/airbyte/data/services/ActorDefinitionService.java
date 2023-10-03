/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.ConnectorSpecification;
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

  void updateDeclarativeActorDefinition(ActorDefinitionConfigInjection configInjection, ConnectorSpecification spec);

  /**
   * Set the ActorDefinitionVersion for a given tag as the default version for the associated actor
   * definition. Check docker image tag on the new ADV; if an ADV exists for that tag, set the
   * existing ADV for the tag as the default. Otherwise, insert the new ADV and set it as the default.
   *
   * @param actorDefinitionVersion new actor definition version
   * @throws IOException - you never know when you IO
   */
  void setActorDefinitionVersionForTagAsDefault(ActorDefinitionVersion actorDefinitionVersion,
                                                List<ActorDefinitionBreakingChange> breakingChangesForDefinition)
      throws IOException;

  void setActorDefinitionVersionAsDefaultVersion(ActorDefinitionVersion actorDefinitionVersion,
                                                 List<ActorDefinitionBreakingChange> breakingChangesForDefinition)
      throws IOException;

  void updateDefaultVersionIdForActorsOnVersion(UUID previousDefaultVersionId, UUID newDefaultVersionId);

  void updateActorDefinitionDefaultVersionId(UUID actorDefinitionId, UUID versionId);

  void writeActorDefinitionWorkspaceGrant(UUID actorDefinitionId, UUID scopeId, io.airbyte.config.ScopeType scopeType) throws IOException;

  // TODO: The scopeType was an enum but it had jooq imports, so gotta figure out what to do there.
  int writeActorDefinitionWorkspaceGrant(UUID actorDefinitionId, UUID scopeId, String scopeType);

  boolean actorDefinitionWorkspaceGrantExists(UUID actorDefinitionId, UUID scopeId, io.airbyte.config.ScopeType scopeType) throws IOException;

  void deleteActorDefinitionWorkspaceGrant(UUID actorDefinitionId, UUID scopeId, io.airbyte.config.ScopeType scopeType) throws IOException;

  ActorDefinitionVersion writeActorDefinitionVersion(ActorDefinitionVersion actorDefinitionVersion) throws IOException;

  Optional<ActorDefinitionVersion> getActorDefinitionVersion(UUID actorDefinitionId, String dockerImageTag) throws IOException;

  ActorDefinitionVersion getActorDefinitionVersion(UUID actorDefinitionVersionId) throws IOException, ConfigNotFoundException;

  List<ActorDefinitionVersion> listActorDefinitionVersionsForDefinition(UUID actorDefinitionId) throws IOException;

  List<ActorDefinitionVersion> getActorDefinitionVersions(List<UUID> actorDefinitionVersionIds) throws IOException;

  void writeActorDefinitionBreakingChanges(List<ActorDefinitionBreakingChange> breakingChanges) throws IOException;

  void setActorDefaultVersion(UUID actorId, UUID actorDefinitionVersionId) throws IOException;

  ActorDefinitionVersion getDefaultVersionForActorDefinitionId(UUID actorDefinitionId);

  /**
   * Get an optional ADV for an actor definition's default version. The optional will be empty if the
   * defaultVersionId of the actor definition is set to null in the DB. The only time this should be
   * the case is if we are in the process of inserting and have already written the source definition,
   * but not yet set its default version.
   */
  Optional<ActorDefinitionVersion> getDefaultVersionForActorDefinitionIdOptional(UUID actorDefinitionId);

  List<ActorDefinitionBreakingChange> listBreakingChangesForActorDefinition(UUID actorDefinitionId) throws IOException;

  void setActorDefinitionVersionSupportStates(List<UUID> actorDefinitionVersionIds, ActorDefinitionVersion.SupportState supportState)
      throws IOException;

  List<ActorDefinitionBreakingChange> listBreakingChangesForActorDefinitionVersion(ActorDefinitionVersion actorDefinitionVersion) throws IOException;

  List<ActorDefinitionBreakingChange> listBreakingChanges() throws IOException;

  /**
   * This function generates a hash for the given AirbyteCatalog.
   *
   * @param airbyteCatalog the catalog to be hashed.
   * @return the hash of the catalog.
   */
  default String generateCanonicalHash(AirbyteCatalog airbyteCatalog) {
    final HashFunction hashFunction = Hashing.murmur3_32_fixed();
    try {
      return hashFunction.hashBytes(Jsons.canonicalJsonSerialize(airbyteCatalog).getBytes(Charsets.UTF_8)).toString();
    } catch (IOException e) {
      // TODO: Setup a logger here
      // LOGGER.error(
      // "Failed to serialize AirbyteCatalog to canonical JSON", e);
      return null;
    }
  }

}
