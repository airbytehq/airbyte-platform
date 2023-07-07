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
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.NormalizationDestinationDefinitionConfig;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
      .withSourceDefinitionId(ACTOR_DEFINITION_ID);

  private static ActorDefinitionVersion baseActorDefinitionVersion() {
    return new ActorDefinitionVersion()
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC)
        .withDocumentationUrl("https://airbyte.io/docs/")
        .withReleaseStage(ReleaseStage.BETA)
        .withReleaseDate("2021-01-21")
        .withSuggestedStreams(new SuggestedStreams().withStreams(List.of("users")))
        .withProtocolVersion("0.1.0")
        .withAllowedHosts(new AllowedHosts().withHosts(List.of("https://airbyte.com")))
        .withSupportsDbt(true)
        .withNormalizationConfig(new NormalizationDestinationDefinitionConfig()
            .withNormalizationRepository("airbyte/normalization")
            .withNormalizationTag("tag")
            .withNormalizationIntegrationType("bigquery"));
  }

  private static final ActorDefinitionVersion ACTOR_DEFINITION_VERSION = baseActorDefinitionVersion();

  private ConfigRepository configRepository;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    configRepository = new ConfigRepository(database, MockData.MAX_SECONDS_BETWEEN_MESSAGE_SUPPLIER);

    // Create correlated entry in actor_definition table to satisfy foreign key requirement
    configRepository.writeStandardSourceDefinition(SOURCE_DEFINITION);
  }

  @Test
  void testWriteActorDefinitionVersion() throws IOException {
    final ActorDefinitionVersion writtenADV = configRepository.writeActorDefinitionVersion(ACTOR_DEFINITION_VERSION);
    // All non-ID fields should match (the ID is randomly assigned)
    assertEquals(ACTOR_DEFINITION_VERSION.withVersionId(writtenADV.getVersionId()), writtenADV);
  }

  @Test
  void testGetActorDefinitionVersionByTag() throws IOException {
    final ActorDefinitionVersion actorDefinitionVersion = configRepository.writeActorDefinitionVersion(ACTOR_DEFINITION_VERSION);
    final UUID id = actorDefinitionVersion.getVersionId();

    final Optional<ActorDefinitionVersion> optRetrievedADV = configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(ACTOR_DEFINITION_VERSION.withVersionId(id), optRetrievedADV.get());
  }

  @Test
  void testGetForNonExistentTagReturnsEmptyOptional() throws IOException {
    assertTrue(configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, UNPERSISTED_DOCKER_IMAGE_TAG).isEmpty());
  }

  @Test
  void testGetActorDefinitionVersionById() throws IOException, ConfigNotFoundException {
    final ActorDefinitionVersion actorDefinitionVersion = configRepository.writeActorDefinitionVersion(ACTOR_DEFINITION_VERSION);
    final UUID id = actorDefinitionVersion.getVersionId();

    final ActorDefinitionVersion retrievedADV = configRepository.getActorDefinitionVersion(id);
    assertNotNull(retrievedADV);
    assertEquals(ACTOR_DEFINITION_VERSION.withVersionId(id), retrievedADV);
  }

  @Test
  void testGetActorDefinitionVersionByIdNotExistentThrowsConfigNotFound() {
    assertThrows(ConfigNotFoundException.class, () -> configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID));
  }

  @Test
  void testWriteSourceDefinitionAndDefaultVersion() throws IOException, JsonValidationException, ConfigNotFoundException {
    // Write initial source definition and default version
    configRepository.writeSourceDefinitionAndDefaultVersion(SOURCE_DEFINITION, ACTOR_DEFINITION_VERSION);

    final Optional<ActorDefinitionVersion> optADVForTag = configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG);
    assertTrue(optADVForTag.isPresent());
    final ActorDefinitionVersion advForTag = optADVForTag.get();
    final StandardSourceDefinition retrievedSourceDefinition =
        configRepository.getStandardSourceDefinition(SOURCE_DEFINITION.getSourceDefinitionId());
    assertEquals(retrievedSourceDefinition.getDefaultVersionId(), advForTag.getVersionId());

    // Modify spec without changing docker image tag
    final ActorDefinitionVersion modifiedADV = baseActorDefinitionVersion().withSpec(SPEC_2);
    configRepository.writeSourceDefinitionAndDefaultVersion(SOURCE_DEFINITION, modifiedADV);

    assertEquals(retrievedSourceDefinition, configRepository.getStandardSourceDefinition(SOURCE_DEFINITION.getSourceDefinitionId()));
    final Optional<ActorDefinitionVersion> optADVForTagAfterCall2 = configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG);
    assertTrue(optADVForTagAfterCall2.isPresent());
    // Versioned data does not get updated since the tag did not change - old spec is still returned
    assertEquals(advForTag, optADVForTagAfterCall2.get());

    // Modifying docker image tag creates a new version (which can contain new versioned data)
    final ActorDefinitionVersion newADV = baseActorDefinitionVersion().withDockerImageTag(DOCKER_IMAGE_TAG_2).withSpec(SPEC_2);
    configRepository.writeSourceDefinitionAndDefaultVersion(SOURCE_DEFINITION, newADV);

    final Optional<ActorDefinitionVersion> optADVForTag2 = configRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG_2);
    assertTrue(optADVForTag2.isPresent());
    final ActorDefinitionVersion advForTag2 = optADVForTag2.get();

    // Versioned data is updated as well as the version id
    assertEquals(advForTag2, newADV.withVersionId(advForTag2.getVersionId()));
    assertNotEquals(advForTag2.getVersionId(), advForTag.getVersionId());
    assertNotEquals(advForTag2.getSpec(), advForTag.getSpec());
  }

  @Test
  void testAlwaysGetWithProtocolVersion() throws IOException {
    final List<ActorDefinitionVersion> allActorDefVersions = List.of(
        baseActorDefinitionVersion().withDockerImageTag("5.0.0").withProtocolVersion(null),
        baseActorDefinitionVersion().withDockerImageTag("5.0.1")
            .withProtocolVersion(null)
            .withSpec(new ConnectorSpecification().withProtocolVersion("0.3.1")),
        baseActorDefinitionVersion().withDockerImageTag("5.0.2")
            .withProtocolVersion("0.4.0")
            .withSpec(new ConnectorSpecification().withProtocolVersion("0.4.1")),
        baseActorDefinitionVersion().withDockerImageTag("5.0.3")
            .withProtocolVersion("0.5.0")
            .withSpec(new ConnectorSpecification()));

    final List<UUID> versionIds = new ArrayList<>();
    for (final ActorDefinitionVersion actorDefVersion : allActorDefVersions) {
      versionIds.add(configRepository.writeActorDefinitionVersion(actorDefVersion).getVersionId());
    }

    final List<ActorDefinitionVersion> actorDefinitionVersions = configRepository.getActorDefinitionVersions(versionIds);
    final List<String> protocolVersions = actorDefinitionVersions.stream().map(ActorDefinitionVersion::getProtocolVersion).toList();
    assertEquals(
        List.of(
            AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION.serialize(),
            AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION.serialize(),
            "0.4.0",
            "0.5.0"),
        protocolVersions);
  }

}
