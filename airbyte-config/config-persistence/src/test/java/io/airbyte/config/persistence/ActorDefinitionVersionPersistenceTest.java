/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorDefinitionVersion.SupportState;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.config.SupportLevel;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.MetricClient;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
        .withInternalSupportLevel(100L)
        .withLanguage("manifest-only")
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
        .withInternalSupportLevel(100L)
        .withReleaseDate("2021-01-21")
        .withSuggestedStreams(new SuggestedStreams().withStreams(List.of("users")))
        .withProtocolVersion("0.1.0")
        .withAllowedHosts(new AllowedHosts().withHosts(List.of("https://airbyte.com")));
  }

  private StandardSourceDefinition sourceDefinition;
  private ActorDefinitionService actorDefinitionService;
  private SourceService sourceService;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    when(featureFlagClient.stringVariation(eq(HeartbeatMaxSecondsBetweenMessages.INSTANCE), any(SourceDefinition.class))).thenReturn("3600");

    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    actorDefinitionService = spy(new ActorDefinitionServiceJooqImpl(database));
    ConnectionService connectionService = mock(ConnectionService.class);
    final ScopedConfigurationService scopedConfigurationService = mock(ScopedConfigurationService.class);
    final ConnectionTimelineEventService connectionTimelineEventService = mock(ConnectionTimelineEventService.class);
    final MetricClient metricClient = mock(MetricClient.class);

    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater = new ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService,
        actorDefinitionService,
        scopedConfigurationService,
        connectionTimelineEventService);

    sourceService = spy(new SourceServiceJooqImpl(database, featureFlagClient,
        secretPersistenceConfigService, connectionService, actorDefinitionVersionUpdater, metricClient));

    final UUID defId = UUID.randomUUID();
    final ActorDefinitionVersion initialADV = initialActorDefinitionVersion(defId);
    sourceDefinition = baseSourceDefinition(defId);

    // Make sure that the source definition exists before we start writing actor definition versions
    sourceService.writeConnectorMetadata(sourceDefinition, initialADV, Collections.emptyList());
  }

  @Test
  void testWriteActorDefinitionVersion() throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion adv = baseActorDefinitionVersion(defId);
    final ActorDefinitionVersion writtenADV = actorDefinitionService.writeActorDefinitionVersion(adv);

    // All non-ID fields should match (the ID is randomly assigned)
    final ActorDefinitionVersion expectedADV = adv.withVersionId(writtenADV.getVersionId());

    assertEquals(expectedADV, writtenADV);
  }

  @Test
  void testGetActorDefinitionVersionByTag() throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion adv = baseActorDefinitionVersion(defId);
    final ActorDefinitionVersion actorDefinitionVersion = actorDefinitionService.writeActorDefinitionVersion(adv);
    final UUID id = actorDefinitionVersion.getVersionId();

    final Optional<ActorDefinitionVersion> optRetrievedADV = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(adv.withVersionId(id), optRetrievedADV.get());
  }

  @Test
  void testUpdateActorDefinitionVersion() throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion initialADV = baseActorDefinitionVersion(defId);

    // initial insert
    final ActorDefinitionVersion insertedADV = actorDefinitionService.writeActorDefinitionVersion(Jsons.clone(initialADV));
    final UUID id = insertedADV.getVersionId();

    Optional<ActorDefinitionVersion> optRetrievedADV = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(insertedADV, optRetrievedADV.get());
    assertEquals(Jsons.clone(initialADV).withVersionId(id), optRetrievedADV.get());

    // update w/o ID
    final ActorDefinitionVersion advWithNewSpec = Jsons.clone(initialADV).withSpec(SPEC_2);
    final ActorDefinitionVersion updatedADV = actorDefinitionService.writeActorDefinitionVersion(advWithNewSpec);

    optRetrievedADV = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(updatedADV, optRetrievedADV.get());
    assertEquals(Jsons.clone(advWithNewSpec).withVersionId(id), optRetrievedADV.get());

    // update w/ ID
    final ActorDefinitionVersion advWithAnotherNewSpecAndId = Jsons.clone(updatedADV).withSpec(SPEC_3);
    final ActorDefinitionVersion updatedADV2 = actorDefinitionService.writeActorDefinitionVersion(advWithAnotherNewSpecAndId);

    optRetrievedADV = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(updatedADV2, optRetrievedADV.get());
    assertEquals(advWithAnotherNewSpecAndId, optRetrievedADV.get());
  }

  @Test
  void testUpdateActorDefinitionVersionWithMismatchedIdFails() throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion initialADV = baseActorDefinitionVersion(defId);

    // initial insert
    final ActorDefinitionVersion insertedADV = actorDefinitionService.writeActorDefinitionVersion(Jsons.clone(initialADV));
    final UUID id = insertedADV.getVersionId();

    Optional<ActorDefinitionVersion> optRetrievedADV = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(insertedADV, optRetrievedADV.get());
    assertEquals(Jsons.clone(initialADV).withVersionId(id), optRetrievedADV.get());

    // update same tag w/ different ID throws
    final ActorDefinitionVersion advWithNewId = Jsons.clone(initialADV).withSpec(SPEC_2).withVersionId(UUID.randomUUID());
    assertThrows(RuntimeException.class, () -> actorDefinitionService.writeActorDefinitionVersion(advWithNewId));

    // no change in DB
    optRetrievedADV = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
    assertTrue(optRetrievedADV.isPresent());
    assertEquals(Jsons.clone(initialADV).withVersionId(id), optRetrievedADV.get());
  }

  @Test
  void testGetForNonExistentTagReturnsEmptyOptional() throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    assertTrue(actorDefinitionService.getActorDefinitionVersion(defId, UNPERSISTED_DOCKER_IMAGE_TAG).isEmpty());
  }

  @Test
  void testGetActorDefinitionVersionById() throws IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion adv = baseActorDefinitionVersion(defId);
    final ActorDefinitionVersion actorDefinitionVersion = actorDefinitionService.writeActorDefinitionVersion(adv);
    final UUID id = actorDefinitionVersion.getVersionId();

    final ActorDefinitionVersion retrievedADV = actorDefinitionService.getActorDefinitionVersion(id);
    assertNotNull(retrievedADV);
    assertEquals(adv.withVersionId(id), retrievedADV);
  }

  @Test
  void testGetActorDefinitionVersionByIdNotExistentThrowsConfigNotFound() {
    // Test using the definition id to catch any accidental assignment
    final UUID defId = sourceDefinition.getSourceDefinitionId();

    assertThrows(io.airbyte.data.exceptions.ConfigNotFoundException.class, () -> actorDefinitionService.getActorDefinitionVersion(defId));
  }

  @Test
  void testWriteSourceDefinitionSupportLevelNone() throws IOException {
    final UUID defId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion adv = baseActorDefinitionVersion(defId).withActorDefinitionId(defId).withSupportLevel(SupportLevel.NONE);

    sourceService.writeConnectorMetadata(sourceDefinition, adv, Collections.emptyList());

    final Optional<ActorDefinitionVersion> optADVForTag = actorDefinitionService.getActorDefinitionVersion(defId, DOCKER_IMAGE_TAG);
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
        () -> sourceService.writeConnectorMetadata(sourceDefinition, adv, Collections.emptyList()));
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
      versionIds.add(actorDefinitionService.writeActorDefinitionVersion(actorDefVersion).getVersionId());
    }

    final List<ActorDefinitionVersion> actorDefinitionVersions = actorDefinitionService.getActorDefinitionVersions(versionIds);
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
    sourceService.writeConnectorMetadata(otherSourceDef, otherActorDefVersion, Collections.emptyList());

    final UUID otherActorDefVersionId = sourceService.getStandardSourceDefinition(otherSourceDef.getSourceDefinitionId()).getDefaultVersionId();

    final List<ActorDefinitionVersion> actorDefinitionVersions = List.of(
        baseActorDefinitionVersion(defId).withDockerImageTag("1.0.0"),
        baseActorDefinitionVersion(defId).withDockerImageTag("2.0.0"),
        baseActorDefinitionVersion(defId).withDockerImageTag("3.0.0"));

    final List<UUID> expectedVersionIds = new ArrayList<>();
    for (final ActorDefinitionVersion actorDefVersion : actorDefinitionVersions) {
      expectedVersionIds.add(actorDefinitionService.writeActorDefinitionVersion(actorDefVersion).getVersionId());
    }

    final UUID defaultVersionId = sourceService.getStandardSourceDefinition(defId).getDefaultVersionId();
    expectedVersionIds.add(defaultVersionId);

    final List<ActorDefinitionVersion> actorDefinitionVersionsForDefinition =
        actorDefinitionService.listActorDefinitionVersionsForDefinition(defId);
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
      versionIds.add(actorDefinitionService.writeActorDefinitionVersion(actorDefVersion).getVersionId());
    }

    actorDefinitionService.setActorDefinitionVersionSupportStates(versionIds, targetSupportState);

    final List<ActorDefinitionVersion> updatedActorDefinitionVersions = actorDefinitionService.getActorDefinitionVersions(versionIds);
    for (final ActorDefinitionVersion updatedActorDefinitionVersion : updatedActorDefinitionVersions) {
      assertEquals(targetSupportState, updatedActorDefinitionVersion.getSupportState());
    }
  }

}
