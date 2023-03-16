/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorDefinitionVersionHelperTest {

  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;

  private static final UUID actorId = UUID.randomUUID();
  private static final UUID workspaceId = UUID.randomUUID();
  private static final String DOCKER_IMAGE_TAG = "0.1.0";
  private static final ConnectorSpecification SPEC = new ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of("key", "value")));

  @BeforeEach
  void setup() {
    actorDefinitionVersionHelper = new ActorDefinitionVersionHelper();
  }

  @Test
  void testGetSourceVersion() {
    final ActorDefinitionVersion expected = new ActorDefinitionVersion()
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, actorId);
    assertEquals(expected, actual);
  }

  @Test
  void testGetSourceVersionForWorkspace() {
    final ActorDefinitionVersion expected = new ActorDefinitionVersion()
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersionForWorkspace(sourceDefinition, workspaceId);
    assertEquals(expected, actual);
  }

  @Test
  void testGetDestinationVersion() {
    final ActorDefinitionVersion expected = new ActorDefinitionVersion()
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, actorId);
    assertEquals(expected, actual);
  }

  @Test
  void testGetDestinationVersionForWorkspace() {
    final ActorDefinitionVersion expected = new ActorDefinitionVersion()
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersionForWorkspace(destinationDefinition, workspaceId);
    assertEquals(expected, actual);
  }

}
