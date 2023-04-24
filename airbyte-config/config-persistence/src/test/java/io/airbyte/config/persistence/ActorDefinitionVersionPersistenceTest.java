/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for interacting with the actor_definition_version table.
 */
class ActorDefinitionVersionPersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID ID = UUID.randomUUID();
  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final String SOURCE_NAME = "Test Source";
  private static final String DOCKER_REPOSITORY = "airbyte/source-test";
  private static final String DOCKER_IMAGE_TAG = "0.1.0";
  private static final String UNPERSISTED_DOCKER_IMAGE_TAG = "0.1.1";
  private static final ConnectorSpecification SPEC = new ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of("key", "value")));

  private static final StandardSourceDefinition SOURCE_DEFINITION = new StandardSourceDefinition()
      .withName(SOURCE_NAME)
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
      .withSourceDefinitionId(ACTOR_DEFINITION_ID);
  private static final ActorDefinitionVersion ACTOR_DEFINITION_VERSION = new ActorDefinitionVersion()
      .withId(ID)
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
      .withSpec(SPEC);

  private ConfigRepository configRepository;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    configRepository = new ConfigRepository(database, MockData.MAX_SECONDS_BETWEEN_MESSAGE_SUPPLIER);

    // Create correlated entry in actor_definition table to satisfy foreign key requirement
    configRepository.writeStandardSourceDefinition(SOURCE_DEFINITION);
  }

  @Test
  void testReadAndWriteActorDefinitionVersion() throws IOException {
    configRepository.writeActorDefinitionVersion(ACTOR_DEFINITION_VERSION);
    assertTrue(configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG).isPresent());
    assertEquals(configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG).get(), ACTOR_DEFINITION_VERSION);
  }

  @Test
  void testGetForNonExistentTagReturnsEmptyOptional() throws IOException {
    assertTrue(configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, UNPERSISTED_DOCKER_IMAGE_TAG).isEmpty());
  }

  @Test
  void testGetActorDefinitionVersionById() throws IOException {
    configRepository.writeActorDefinitionVersion(ACTOR_DEFINITION_VERSION);
    assertTrue(configRepository.getActorDefinitionVersion(ID).isPresent());
    assertEquals(configRepository.getActorDefinitionVersion(ID).get(), ACTOR_DEFINITION_VERSION);
  }

  @Test
  void testGetActorDefinitionVersionByIdNotExistentReturnsEmptyOptional() throws IOException {
    assertTrue(configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID).isEmpty());
  }

}
