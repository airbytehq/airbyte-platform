/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for interacting with the actor_definition_version table.
 */
class ActorDefinitionVersionPersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final String SOURCE_NAME = "Test Source";
  private static final String DOCKER_REPOSITORY = "airbyte/source-test";
  private static final String DOCKER_IMAGE_TAG = "0.1.0";
  private static final String DOCKER_IMAGE_TAG_2 = "0.2.0";
  private static final String UNPERSISTED_DOCKER_IMAGE_TAG = "0.1.1";
  private static final String PROTOCOL_VERSION = "1.0.0";
  private static final ConnectorSpecification SPEC = new ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of("key", "value"))).withProtocolVersion(PROTOCOL_VERSION);
  private static final ConnectorSpecification SPEC_2 = new ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of("key", "value2"))).withProtocolVersion(PROTOCOL_VERSION);

  private static final StandardSourceDefinition SOURCE_DEFINITION = new StandardSourceDefinition()
      .withName(SOURCE_NAME)
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
      .withSourceDefinitionId(ACTOR_DEFINITION_ID);
  private static final ActorDefinitionVersion ACTOR_DEFINITION_VERSION = new ActorDefinitionVersion()
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
    ActorDefinitionVersion writtenADV = configRepository.writeActorDefinitionVersion(ACTOR_DEFINITION_VERSION);
    Optional<ActorDefinitionVersion> optRetrievedADV = configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(writtenADV, optRetrievedADV.get());
    assertEquals(writtenADV, ACTOR_DEFINITION_VERSION.withVersionId(writtenADV.getVersionId()));

    // update same ADV
    final ActorDefinitionVersion modifiedADV = ACTOR_DEFINITION_VERSION.withSpec(SPEC_2);
    writtenADV = configRepository.writeActorDefinitionVersion(modifiedADV);
    optRetrievedADV = configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(writtenADV, optRetrievedADV.get());
    assertEquals(writtenADV, modifiedADV.withVersionId(writtenADV.getVersionId()));

    // different docker tag, new ADV
    final ActorDefinitionVersion newADV = ACTOR_DEFINITION_VERSION.withDockerImageTag(DOCKER_IMAGE_TAG_2);
    writtenADV = configRepository.writeActorDefinitionVersion(newADV);
    final Optional<ActorDefinitionVersion> optOldADV = configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG);
    final Optional<ActorDefinitionVersion> optNewADV = configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG_2);
    assertTrue(optNewADV.isPresent());
    assertTrue(optOldADV.isPresent());
    assertEquals(writtenADV, optNewADV.get());
    assertEquals(writtenADV, newADV.withVersionId(writtenADV.getVersionId()));
    assertEquals(optRetrievedADV.get(), optOldADV.get());
    assertNotEquals(optNewADV.get().getVersionId(), optOldADV.get().getVersionId());
  }

  @Test
  void testGetForNonExistentTagReturnsEmptyOptional() throws IOException {
    assertTrue(configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, UNPERSISTED_DOCKER_IMAGE_TAG).isEmpty());
  }

  @Test
  void testGetActorDefinitionVersionById() throws IOException, ConfigNotFoundException {
    final ActorDefinitionVersion actorDefinitionVersion = configRepository.writeActorDefinitionVersion(ACTOR_DEFINITION_VERSION);
    final UUID id = actorDefinitionVersion.getVersionId();

    assertNotNull(configRepository.getActorDefinitionVersion(id));
    assertEquals(configRepository.getActorDefinitionVersion(id), ACTOR_DEFINITION_VERSION.withVersionId(id));
  }

  @Test
  void testGetActorDefinitionVersionByIdNotExistentThrowsConfigNotFound() {
    assertThrows(ConfigNotFoundException.class, () -> configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID));
  }

}
