/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorDefinitionVersion.SupportState;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.NormalizationDestinationDefinitionConfig;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.CatalogServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectorBuilderServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.HealthCheckServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OAuthServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests for interacting with the actor_definition_version table.
 */
class ActorDefinitionVersionPersistenceTest extends BaseConfigDatabaseTest {

  private static final String SOURCE_NAME = "Test Source";
  private static final String DOCKER_REPOSITORY = "airbyte/source-test";
  private static final String DOCKER_IMAGE_TAG = "0.1.0";
  private static final String UNPERSISTED_DOCKER_IMAGE_TAG = "0.1.1";
  private static final String PROTOCOL_VERSION = "1.0.0";
  private static final ConnectorSpecification SPEC = new ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of("key", "value"))).withProtocolVersion(PROTOCOL_VERSION);
  private static final ConnectorSpecification SPEC_2 = new ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of("key2", "value2"))).withProtocolVersion(PROTOCOL_VERSION);
  private static final ConnectorSpecification SPEC_3 = new ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of("key3", "value3"))).withProtocolVersion(PROTOCOL_VERSION);

  private static StandardSourceDefinition baseSourceDefinition(final UUID actorDefinitionId) {
    return new StandardSourceDefinition()
        .withName(SOURCE_NAME)
        .withSourceDefinitionId(actorDefinitionId);
  }

  private static ActorDefinitionVersion initialActorDefinitionVersion(final UUID actorDefinitionId) {
    return new ActorDefinitionVersion()
        .withActorDefinitionId(actorDefinitionId)
        .withDockerImageTag("0.0.0")
        .withDockerRepository("overwrite me")
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withSpec(new ConnectorSpecification().withAdditionalProperty("overwrite", "me").withProtocolVersion("0.0.0"));
  }

  private static ActorDefinitionVersion baseActorDefinitionVersion(final UUID actorDefinitionId) {
    return new ActorDefinitionVersion()
        .withActorDefinitionId(actorDefinitionId)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC)
        .withDocumentationUrl("https://airbyte.io/docs/")
        .withReleaseStage(ReleaseStage.BETA)
        .withSupportLevel(SupportLevel.COMMUNITY)
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

  private ConfigRepository configRepository;
  private StandardSourceDefinition sourceDefinition;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    configRepository = spy(
        new ConfigRepository(
            new ActorDefinitionServiceJooqImpl(database),
            new CatalogServiceJooqImpl(database),
            new ConnectionServiceJooqImpl(database),
            new ConnectorBuilderServiceJooqImpl(database),
            new DestinationServiceJooqImpl(database,
                featureFlagClient,
                secretsRepositoryReader,
                secretsRepositoryWriter,
                secretPersistenceConfigService),
            new HealthCheckServiceJooqImpl(database),
            new OAuthServiceJooqImpl(database,
                featureFlagClient,
                secretsRepositoryReader,
                secretPersistenceConfigService),
            new OperationServiceJooqImpl(database),
            new OrganizationServiceJooqImpl(database),
            new SourceServiceJooqImpl(database,
                featureFlagClient,
                secretsRepositoryReader,
                secretsRepositoryWriter,
                secretPersistenceConfigService),
            new WorkspaceServiceJooqImpl(database,
                featureFlagClient,
                secretsRepositoryReader,
                secretsRepositoryWriter,
                secretPersistenceConfigService)));

    final UUID defId = UUID.randomUUID();
    final ActorDefinitionVersion initialADV = initialActorDefinitionVersion(defId);
    sourceDefinition = baseSourceDefinition(defId);

    // Make sure that the source definition exists before we start writing actor definition versions
    configRepository.writeConnectorMetadata(sourceDefinition, initialADV);
  }

  @Test
  void testWriteActorDefinitionVersion() throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion adv = baseActorDefinitionVersion(defId);
    final ActorDefinitionVersion writtenADV = configRepository.writeActorDefinitionVersion(adv);

    // All non-ID fields should match (the ID is randomly assigned)
    final ActorDefinitionVersion expectedADV = adv.withVersionId(writtenADV.getVersionId());

    assertEquals(expectedADV, writtenADV);
  }

  @Test
  void testGetActorDefinitionVersionByTag() throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion adv = baseActorDefinitionVersion(defId);
    final ActorDefinitionVersion actorDefinitionVersion = configRepository.writeActorDefinitionVersion(adv);
    final UUID id = actorDefinitionVersion.getVersionId();

    final Optional<ActorDefinitionVersion> optRetrievedADV = configRepository.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(adv.withVersionId(id), optRetrievedADV.get());
  }

  @Test
  void testUpdateActorDefinitionVersion() throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion initialADV = baseActorDefinitionVersion(defId);

    // initial insert
    final ActorDefinitionVersion insertedADV = configRepository.writeActorDefinitionVersion(Jsons.clone(initialADV));
    final UUID id = insertedADV.getVersionId();

    Optional<ActorDefinitionVersion> optRetrievedADV = configRepository.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(insertedADV, optRetrievedADV.get());
    assertEquals(Jsons.clone(initialADV).withVersionId(id), optRetrievedADV.get());

    // update w/o ID
    final ActorDefinitionVersion advWithNewSpec = Jsons.clone(initialADV).withSpec(SPEC_2);
    final ActorDefinitionVersion updatedADV = configRepository.writeActorDefinitionVersion(advWithNewSpec);

    optRetrievedADV = configRepository.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(updatedADV, optRetrievedADV.get());
    assertEquals(Jsons.clone(advWithNewSpec).withVersionId(id), optRetrievedADV.get());

    // update w/ ID
    final ActorDefinitionVersion advWithAnotherNewSpecAndId = Jsons.clone(updatedADV).withSpec(SPEC_3);
    final ActorDefinitionVersion updatedADV2 = configRepository.writeActorDefinitionVersion(advWithAnotherNewSpecAndId);

    optRetrievedADV = configRepository.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(updatedADV2, optRetrievedADV.get());
    assertEquals(advWithAnotherNewSpecAndId, optRetrievedADV.get());
  }

  @Test
  void testUpdateActorDefinitionVersionWithMismatchedIdFails() throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion initialADV = baseActorDefinitionVersion(defId);

    // initial insert
    final ActorDefinitionVersion insertedADV = configRepository.writeActorDefinitionVersion(Jsons.clone(initialADV));
    final UUID id = insertedADV.getVersionId();

    Optional<ActorDefinitionVersion> optRetrievedADV = configRepository.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(insertedADV, optRetrievedADV.get());
    assertEquals(Jsons.clone(initialADV).withVersionId(id), optRetrievedADV.get());

    // update same tag w/ different ID throws
    final ActorDefinitionVersion advWithNewId = Jsons.clone(initialADV).withSpec(SPEC_2).withVersionId(UUID.randomUUID());
    assertThrows(RuntimeException.class, () -> configRepository.writeActorDefinitionVersion(advWithNewId));

    // no change in DB
    optRetrievedADV = configRepository.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(Jsons.clone(initialADV).withVersionId(id), optRetrievedADV.get());
  }

  @Test
  void testGetForNonExistentTagReturnsEmptyOptional() throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    assertTrue(configRepository.getActorDefinitionVersion(defId, UNPERSISTED_DOCKER_IMAGE_TAG).isEmpty());
  }

  @Test
  void testGetActorDefinitionVersionById() throws IOException, ConfigNotFoundException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion adv = baseActorDefinitionVersion(defId);
    final ActorDefinitionVersion actorDefinitionVersion = configRepository.writeActorDefinitionVersion(adv);
    final UUID id = actorDefinitionVersion.getVersionId();

    final ActorDefinitionVersion retrievedADV = configRepository.getActorDefinitionVersion(id);
    assertNotNull(retrievedADV);
    assertEquals(adv.withVersionId(id), retrievedADV);
  }

  @Test
  void testGetActorDefinitionVersionByIdNotExistentThrowsConfigNotFound() {
    // Test using the definition id to catch any accidental assignment
    final UUID defId = sourceDefinition.getSourceDefinitionId();

    assertThrows(ConfigNotFoundException.class, () -> configRepository.getActorDefinitionVersion(defId));
  }

  @Test
  void testWriteSourceDefinitionSupportLevelNone() throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion adv = baseActorDefinitionVersion(defId).withActorDefinitionId(defId).withSupportLevel(SupportLevel.NONE);

    configRepository.writeConnectorMetadata(sourceDefinition, adv);

    final Optional<ActorDefinitionVersion> optADVForTag = configRepository.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
    assertTrue(optADVForTag.isPresent());
    final ActorDefinitionVersion advForTag = optADVForTag.get();
    assertEquals(advForTag.getSupportLevel(), SupportLevel.NONE);
  }

  @Test
  void testWriteSourceDefinitionSupportLevelNonNullable() {
    final UUID defId = sourceDefinition.getSourceDefinitionId();

    final ActorDefinitionVersion adv = baseActorDefinitionVersion(defId).withActorDefinitionId(defId).withSupportLevel(null);

    assertThrows(
        RuntimeException.class,
        () -> configRepository.writeConnectorMetadata(sourceDefinition, adv));
  }

  @Test
  void testAlwaysGetWithProtocolVersion() throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();

    final List<ActorDefinitionVersion> allActorDefVersions = List.of(
        baseActorDefinitionVersion(defId).withDockerImageTag("5.0.0").withProtocolVersion(null),
        baseActorDefinitionVersion(defId).withDockerImageTag("5.0.1")
            .withProtocolVersion(null)
            .withSpec(new ConnectorSpecification().withProtocolVersion("0.3.1")),
        baseActorDefinitionVersion(defId).withDockerImageTag("5.0.2")
            .withProtocolVersion("0.4.0")
            .withSpec(new ConnectorSpecification().withProtocolVersion("0.4.1")),
        baseActorDefinitionVersion(defId).withDockerImageTag("5.0.3")
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

  @Test
  void testListActorDefinitionVersionsForDefinition() throws IOException, JsonValidationException, ConfigNotFoundException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final StandardSourceDefinition otherSourceDef = new StandardSourceDefinition()
        .withName("Some other source")
        .withSourceDefinitionId(UUID.randomUUID());
    final ActorDefinitionVersion otherActorDefVersion =
        baseActorDefinitionVersion(defId).withActorDefinitionId(otherSourceDef.getSourceDefinitionId());
    configRepository.writeConnectorMetadata(otherSourceDef, otherActorDefVersion);

    final UUID otherActorDefVersionId = configRepository.getStandardSourceDefinition(otherSourceDef.getSourceDefinitionId()).getDefaultVersionId();

    final List<ActorDefinitionVersion> actorDefinitionVersions = List.of(
        baseActorDefinitionVersion(defId).withDockerImageTag("1.0.0"),
        baseActorDefinitionVersion(defId).withDockerImageTag("2.0.0"),
        baseActorDefinitionVersion(defId).withDockerImageTag("3.0.0"));

    final List<UUID> expectedVersionIds = new ArrayList<>();
    for (final ActorDefinitionVersion actorDefVersion : actorDefinitionVersions) {
      expectedVersionIds.add(configRepository.writeActorDefinitionVersion(actorDefVersion).getVersionId());
    }

    final UUID defaultVersionId = configRepository.getStandardSourceDefinition(defId).getDefaultVersionId();
    expectedVersionIds.add(defaultVersionId);

    final List<ActorDefinitionVersion> actorDefinitionVersionsForDefinition =
        configRepository.listActorDefinitionVersionsForDefinition(defId);
    assertThat(expectedVersionIds)
        .containsExactlyInAnyOrderElementsOf(actorDefinitionVersionsForDefinition.stream().map(ActorDefinitionVersion::getVersionId).toList());
    assertFalse(
        actorDefinitionVersionsForDefinition.stream().anyMatch(actorDefVersion -> actorDefVersion.getVersionId().equals(otherActorDefVersionId)));
  }

  @ParameterizedTest
  @CsvSource({
    "SUPPORTED, DEPRECATED",
    "SUPPORTED, UNSUPPORTED",
    "DEPRECATED, SUPPORTED",
    "DEPRECATED, UNSUPPORTED",
    "UNSUPPORTED, SUPPORTED",
    "UNSUPPORTED, DEPRECATED",
  })
  void testSetActorDefinitionVersionSupportStates(final String initialSupportStateStr, final String targetSupportStateStr) throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final SupportState initialSupportState = SupportState.valueOf(initialSupportStateStr);
    final SupportState targetSupportState = SupportState.valueOf(targetSupportStateStr);

    final List<ActorDefinitionVersion> actorDefinitionVersions = List.of(
        baseActorDefinitionVersion(defId).withDockerImageTag("1.0.0").withSupportState(initialSupportState),
        baseActorDefinitionVersion(defId).withDockerImageTag("2.0.0").withSupportState(initialSupportState));

    final List<UUID> versionIds = new ArrayList<>();
    for (final ActorDefinitionVersion actorDefVersion : actorDefinitionVersions) {
      versionIds.add(configRepository.writeActorDefinitionVersion(actorDefVersion).getVersionId());
    }

    configRepository.setActorDefinitionVersionSupportStates(versionIds, targetSupportState);

    final List<ActorDefinitionVersion> updatedActorDefinitionVersions = configRepository.getActorDefinitionVersions(versionIds);
    for (final ActorDefinitionVersion updatedActorDefinitionVersion : updatedActorDefinitionVersions) {
      assertEquals(targetSupportState, updatedActorDefinitionVersion.getSupportState());
    }
  }

}
