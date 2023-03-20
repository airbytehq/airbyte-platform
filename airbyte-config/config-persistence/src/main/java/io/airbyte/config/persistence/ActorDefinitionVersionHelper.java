/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import jakarta.inject.Singleton;
import java.util.UUID;

/**
 * Helper class for retrieving the actor definition version to use when running a connector. This
 * should be used when a specific actor or workspace is present, rather than accessing the fields
 * directly on the definitions.
 */
@Singleton
public class ActorDefinitionVersionHelper {

  private ActorDefinitionVersion getDefaultSourceVersion(final StandardSourceDefinition sourceDefinition) {
    return new ActorDefinitionVersion()
        .withDockerRepository(sourceDefinition.getDockerRepository())
        .withDockerImageTag(sourceDefinition.getDockerImageTag())
        .withSpec(sourceDefinition.getSpec());
  }

  private ActorDefinitionVersion getDefaultDestinationVersion(final StandardDestinationDefinition destinationDefinition) {
    return new ActorDefinitionVersion()
        .withDockerRepository(destinationDefinition.getDockerRepository())
        .withDockerImageTag(destinationDefinition.getDockerImageTag())
        .withSpec(destinationDefinition.getSpec());
  }

  /**
   * Get the actor definition version to use for a source.
   *
   * @param sourceDefinition source definition
   * @param actorId source id
   * @return actor definition version
   */
  public ActorDefinitionVersion getSourceVersion(final StandardSourceDefinition sourceDefinition, final UUID actorId) {
    // TODO: Apply overrides based on actorId here. Fallback to workspace overrides if none exist.
    return getDefaultSourceVersion(sourceDefinition);
  }

  /**
   * Get the actor definition version to use for a source in a workspace.
   *
   * @param sourceDefinition source definition
   * @param workspaceId workspace id
   * @return actor definition version
   */
  public ActorDefinitionVersion getSourceVersionForWorkspace(final StandardSourceDefinition sourceDefinition, final UUID workspaceId) {
    // TODO: Apply overrides based on workspaceId here
    return getDefaultSourceVersion(sourceDefinition);
  }

  /**
   * Get the actor definition version to use for a destination.
   *
   * @param destinationDefinition destination definition
   * @param actorId destination id
   * @return actor definition version
   */
  public ActorDefinitionVersion getDestinationVersion(final StandardDestinationDefinition destinationDefinition, final UUID actorId) {
    // TODO: Apply overrides based on actorId here. Fallback to workspace overrides if none exist.
    return getDefaultDestinationVersion(destinationDefinition);
  }

  /**
   * Get the actor definition version to use for a destination in a workspace.
   *
   * @param destinationDefinition destination definition
   * @param workspaceId workspace id
   * @return actor definition version
   */
  public ActorDefinitionVersion getDestinationVersionForWorkspace(final StandardDestinationDefinition destinationDefinition,
                                                                  final UUID workspaceId) {
    // TODO: Apply overrides based on workspaceId here
    return getDefaultDestinationVersion(destinationDefinition);
  }

}
