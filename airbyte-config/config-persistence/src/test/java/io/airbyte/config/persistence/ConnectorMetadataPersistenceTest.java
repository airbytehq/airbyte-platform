/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.commons.ConstantsKt.DEFAULT_ORGANIZATION_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.BreakingChangeScope;
import io.airbyte.config.BreakingChangeScope.ScopeType;
import io.airbyte.config.DataplaneGroup;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.DataplaneGroupService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.MetricClient;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for configRepository methods that write connector metadata together. Includes writing
 * global metadata (source/destination definitions and breaking changes) and versioned metadata
 * (actor definition versions).
 */
class ConnectorMetadataPersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID DATAPLANE_GROUP_ID = UUID.randomUUID();

  private static final String DOCKER_IMAGE_TAG = "0.0.1";

  private static final String UPGRADE_IMAGE_TAG = "0.0.2";
  private static final String PROTOCOL_VERSION = "1.0.0";

  private ActorDefinitionVersionUpdater actorDefinitionVersionUpdater;
  private ActorDefinitionService actorDefinitionService;

  private ConnectionService connectionService;
  private SourceService sourceService;
  private DestinationService destinationService;
  private WorkspaceService workspaceService;

  @BeforeEach
  void setup() throws SQLException, JsonValidationException, IOException {
    truncateAllTables();
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    when(featureFlagClient.stringVariation(eq(HeartbeatMaxSecondsBetweenMessages.INSTANCE), any(SourceDefinition.class))).thenReturn("3600");

    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    final ConnectionTimelineEventService connectionTimelineEventService = mock(ConnectionTimelineEventService.class);
    final MetricClient metricClient = mock(MetricClient.class);
    final ScopedConfigurationService scopedConfigurationService = mock(ScopedConfigurationService.class);
    final DataplaneGroupService dataplaneGroupService = mock(DataplaneGroupService.class);
    when(dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(any(), any()))
        .thenReturn(new DataplaneGroup().withId(DATAPLANE_GROUP_ID));

    connectionService = new ConnectionServiceJooqImpl(database);
    actorDefinitionService = spy(new ActorDefinitionServiceJooqImpl(database));
    actorDefinitionVersionUpdater =
        spy(new ActorDefinitionVersionUpdater(featureFlagClient, connectionService, actorDefinitionService, scopedConfigurationService,
            connectionTimelineEventService));

    sourceService = new SourceServiceJooqImpl(
        database,
        featureFlagClient,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient);
    destinationService = new DestinationServiceJooqImpl(
        database,
        featureFlagClient,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient);

    workspaceService = new WorkspaceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient);

    final OrganizationService organizationService = new OrganizationServiceJooqImpl(database);
    organizationService.writeOrganization(MockData.defaultOrganization());
    workspaceService.writeStandardWorkspaceNoSecrets(new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("default")
        .withSlug("workspace-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID)
        .withOrganizationId(DEFAULT_ORGANIZATION_ID));
  }

  @Test
  void testWriteConnectorMetadataForSource()
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // Initial insert
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion1 = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());

    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion1, Collections.emptyList());

    StandardSourceDefinition sourceDefinitionFromDB = sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId());
    final Optional<ActorDefinitionVersion> actorDefinitionVersionFromDB =
        actorDefinitionService.getActorDefinitionVersion(actorDefinitionVersion1.getActorDefinitionId(), actorDefinitionVersion1.getDockerImageTag());

    assertTrue(actorDefinitionVersionFromDB.isPresent());
    final UUID firstVersionId = actorDefinitionVersionFromDB.get().getVersionId();

    assertEquals(actorDefinitionVersion1.withVersionId(firstVersionId), actorDefinitionVersionFromDB.get());
    assertEquals(firstVersionId, sourceDefinitionFromDB.getDefaultVersionId());
    assertEquals(sourceDefinition.withDefaultVersionId(firstVersionId), sourceDefinitionFromDB);

    // Updating an existing source definition/version
    final StandardSourceDefinition sourceDefinition2 = sourceDefinition.withName("updated name");
    final ActorDefinitionVersion actorDefinitionVersion2 =
        createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(UPGRADE_IMAGE_TAG);
    final List<BreakingChangeScope> scopedImpact =
        List.of(new BreakingChangeScope().withScopeType(ScopeType.STREAM).withImpactedScopes(List.of("stream_a", "stream_b")));
    final List<ActorDefinitionBreakingChange> breakingChanges =
        List.of(MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG).withActorDefinitionId(sourceDefinition2.getSourceDefinitionId())
            .withScopedImpact(scopedImpact));
    sourceService.writeConnectorMetadata(sourceDefinition2, actorDefinitionVersion2, breakingChanges);

    sourceDefinitionFromDB = sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId());
    final Optional<ActorDefinitionVersion> actorDefinitionVersion2FromDB =
        actorDefinitionService.getActorDefinitionVersion(actorDefinitionVersion2.getActorDefinitionId(), actorDefinitionVersion2.getDockerImageTag());
    final List<ActorDefinitionBreakingChange> breakingChangesForDefFromDb =
        actorDefinitionService.listBreakingChangesForActorDefinition(sourceDefinition2.getSourceDefinitionId());

    assertTrue(actorDefinitionVersion2FromDB.isPresent());
    final UUID newADVId = actorDefinitionVersion2FromDB.get().getVersionId();

    assertNotEquals(firstVersionId, newADVId);
    assertEquals(newADVId, sourceDefinitionFromDB.getDefaultVersionId());
    assertEquals(sourceDefinition2.withDefaultVersionId(newADVId), sourceDefinitionFromDB);
    assertThat(breakingChangesForDefFromDb).containsExactlyInAnyOrderElementsOf(breakingChanges);
    verify(actorDefinitionVersionUpdater).updateSourceDefaultVersion(sourceDefinition2, actorDefinitionVersion2, breakingChanges);
  }

  @Test
  void testWriteConnectorMetadataForDestination()
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // Initial insert
    final StandardDestinationDefinition destinationDefinition = createBaseDestDef();
    final ActorDefinitionVersion actorDefinitionVersion1 = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());

    destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion1, Collections.emptyList());

    StandardDestinationDefinition destinationDefinitionFromDB =
        destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId());
    final Optional<ActorDefinitionVersion> actorDefinitionVersionFromDB =
        actorDefinitionService.getActorDefinitionVersion(actorDefinitionVersion1.getActorDefinitionId(), actorDefinitionVersion1.getDockerImageTag());

    assertTrue(actorDefinitionVersionFromDB.isPresent());
    final UUID firstVersionId = actorDefinitionVersionFromDB.get().getVersionId();

    assertEquals(actorDefinitionVersion1.withVersionId(firstVersionId), actorDefinitionVersionFromDB.get());
    assertEquals(firstVersionId, destinationDefinitionFromDB.getDefaultVersionId());
    assertEquals(destinationDefinition.withDefaultVersionId(firstVersionId), destinationDefinitionFromDB);

    // Updating an existing destination definition/version
    final StandardDestinationDefinition destinationDefinition2 = destinationDefinition.withName("updated name");
    final ActorDefinitionVersion actorDefinitionVersion2 =
        createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(UPGRADE_IMAGE_TAG);

    final List<BreakingChangeScope> scopedImpact =
        List.of(new BreakingChangeScope().withScopeType(ScopeType.STREAM).withImpactedScopes(List.of("stream_a", "stream_b")));
    final List<ActorDefinitionBreakingChange> breakingChanges =
        List.of(MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG).withActorDefinitionId(destinationDefinition2.getDestinationDefinitionId())
            .withScopedImpact(scopedImpact));
    destinationService.writeConnectorMetadata(destinationDefinition2, actorDefinitionVersion2, breakingChanges);

    destinationDefinitionFromDB = destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId());
    final Optional<ActorDefinitionVersion> actorDefinitionVersion2FromDB =
        actorDefinitionService.getActorDefinitionVersion(actorDefinitionVersion2.getActorDefinitionId(), actorDefinitionVersion2.getDockerImageTag());
    final List<ActorDefinitionBreakingChange> breakingChangesForDefFromDb =
        actorDefinitionService.listBreakingChangesForActorDefinition(destinationDefinition2.getDestinationDefinitionId());

    assertTrue(actorDefinitionVersion2FromDB.isPresent());
    final UUID newADVId = actorDefinitionVersion2FromDB.get().getVersionId();

    assertNotEquals(firstVersionId, newADVId);
    assertEquals(newADVId, destinationDefinitionFromDB.getDefaultVersionId());
    assertEquals(destinationDefinition2.withDefaultVersionId(newADVId), destinationDefinitionFromDB);
    assertThat(breakingChangesForDefFromDb).containsExactlyInAnyOrderElementsOf(breakingChanges);
    verify(actorDefinitionVersionUpdater).updateDestinationDefaultVersion(destinationDefinition2, actorDefinitionVersion2, breakingChanges);
  }

  @Test
  void testUpdateConnectorMetadata() throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final UUID actorDefinitionId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion actorDefinitionVersion1 = createBaseActorDefVersion(actorDefinitionId);
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion1, Collections.emptyList());

    final Optional<ActorDefinitionVersion> optADVForTag = actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG);
    assertTrue(optADVForTag.isPresent());
    final ActorDefinitionVersion advForTag = optADVForTag.get();
    final StandardSourceDefinition retrievedSourceDefinition =
        sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId());
    assertEquals(retrievedSourceDefinition.getDefaultVersionId(), advForTag.getVersionId());

    final ConnectorSpecification updatedSpec = new ConnectorSpecification()
        .withConnectionSpecification(Jsons.jsonNode(Map.of("key", "value2"))).withProtocolVersion(PROTOCOL_VERSION);

    // Modify spec without changing docker image tag
    final ActorDefinitionVersion modifiedADV = createBaseActorDefVersion(actorDefinitionId).withSpec(updatedSpec);
    sourceService.writeConnectorMetadata(sourceDefinition, modifiedADV, Collections.emptyList());

    assertEquals(retrievedSourceDefinition, sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()));
    final Optional<ActorDefinitionVersion> optADVForTagAfterCall2 =
        actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG);
    assertTrue(optADVForTagAfterCall2.isPresent());

    // Modifying fields without updating image tag updates existing version
    assertEquals(modifiedADV.withVersionId(advForTag.getVersionId()), optADVForTagAfterCall2.get());

    // Modifying docker image tag creates a new version
    final ActorDefinitionVersion newADV =
        createBaseActorDefVersion(actorDefinitionId).withDockerImageTag(UPGRADE_IMAGE_TAG).withSpec(updatedSpec);
    sourceService.writeConnectorMetadata(sourceDefinition, newADV, Collections.emptyList());

    final Optional<ActorDefinitionVersion> optADVForTag2 = actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, UPGRADE_IMAGE_TAG);
    assertTrue(optADVForTag2.isPresent());
    final ActorDefinitionVersion advForTag2 = optADVForTag2.get();

    // Versioned data is updated as well as the version id
    assertEquals(advForTag2, newADV.withVersionId(advForTag2.getVersionId()));
    assertNotEquals(advForTag2.getVersionId(), advForTag.getVersionId());
    assertNotEquals(advForTag2.getSpec(), advForTag.getSpec());
    verify(actorDefinitionVersionUpdater).updateSourceDefaultVersion(sourceDefinition, newADV, List.of());
  }

  @ParameterizedTest
  @ValueSource(strings = {"2.0.0", "dev", "test", "1.9.1-dev.33a53e6236", "97b69a76-1f06-4680-8905-8beda74311d0"})
  void testCustomImageTagsDoNotBreakCustomConnectorUpgrade(final String dockerImageTag) throws IOException {
    // Initial insert
    final StandardSourceDefinition customSourceDefinition = createBaseSourceDef().withCustom(true);
    final StandardDestinationDefinition customDestinationDefinition = createBaseDestDef().withCustom(true);
    final ActorDefinitionVersion sourceActorDefinitionVersion = createBaseActorDefVersion(customSourceDefinition.getSourceDefinitionId());
    final ActorDefinitionVersion destinationActorDefinitionVersion =
        createBaseActorDefVersion(customDestinationDefinition.getDestinationDefinitionId());
    sourceService.writeConnectorMetadata(customSourceDefinition, sourceActorDefinitionVersion, Collections.emptyList());
    destinationService.writeConnectorMetadata(customDestinationDefinition, destinationActorDefinitionVersion, Collections.emptyList());

    // Update
    assertDoesNotThrow(() -> sourceService.writeConnectorMetadata(customSourceDefinition,
        createBaseActorDefVersion(customSourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag), List.of()));
    assertDoesNotThrow(() -> destinationService.writeConnectorMetadata(customDestinationDefinition,
        createBaseActorDefVersion(customDestinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag), List.of()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"2.0.0", "dev", "test", "1.9.1-dev.33a53e6236", "97b69a76-1f06-4680-8905-8beda74311d0"})
  void testImageTagExpectationsNorNonCustomConnectorUpgradesWithoutBreakingChanges(final String dockerImageTag) throws IOException {
    // Initial insert
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final StandardDestinationDefinition destinationDefinition = createBaseDestDef();
    final ActorDefinitionVersion sourceActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());
    final ActorDefinitionVersion destinationActorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());
    sourceService.writeConnectorMetadata(sourceDefinition, sourceActorDefinitionVersion, Collections.emptyList());
    destinationService.writeConnectorMetadata(destinationDefinition, destinationActorDefinitionVersion, Collections.emptyList());

    // Update
    assertDoesNotThrow(() -> sourceService.writeConnectorMetadata(sourceDefinition,
        createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag), List.of()));
    assertDoesNotThrow(() -> destinationService.writeConnectorMetadata(destinationDefinition,
        createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag), List.of()));
  }

  @ParameterizedTest
  @CsvSource({"0.0.1, true", "dev, true", "test, false", "1.9.1-dev.33a53e6236, true", "97b69a76-1f06-4680-8905-8beda74311d0, false"})
  void testImageTagExpectationsNorNonCustomConnectorUpgradesWithBreakingChanges(final String dockerImageTag, final boolean upgradeShouldSucceed)
      throws IOException {
    // Initial insert
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final StandardDestinationDefinition destinationDefinition = createBaseDestDef();
    final ActorDefinitionVersion sourceActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());
    final ActorDefinitionVersion destinationActorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());
    sourceService.writeConnectorMetadata(sourceDefinition, sourceActorDefinitionVersion, Collections.emptyList());
    destinationService.writeConnectorMetadata(destinationDefinition, destinationActorDefinitionVersion, Collections.emptyList());

    final List<ActorDefinitionBreakingChange> sourceBreakingChanges =
        List.of(MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG).withActorDefinitionId(sourceDefinition.getSourceDefinitionId()));
    final List<ActorDefinitionBreakingChange> destinationBreakingChanges =
        List.of(MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG).withActorDefinitionId(destinationDefinition.getDestinationDefinitionId()));

    // Update
    if (upgradeShouldSucceed) {
      assertDoesNotThrow(() -> sourceService.writeConnectorMetadata(sourceDefinition,
          createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag),
          sourceBreakingChanges));
      assertDoesNotThrow(() -> destinationService.writeConnectorMetadata(destinationDefinition,
          createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag),
          destinationBreakingChanges));
    } else {
      assertThrows(IllegalArgumentException.class, () -> sourceService.writeConnectorMetadata(sourceDefinition,
          createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag), sourceBreakingChanges));
      assertThrows(IllegalArgumentException.class, () -> destinationService.writeConnectorMetadata(destinationDefinition,
          createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag),
          destinationBreakingChanges));
    }
  }

  @Test
  void testTransactionRollbackOnFailure()
      throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final UUID initialADVId = UUID.randomUUID();
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion1 =
        createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withVersionId(initialADVId);

    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion1, Collections.emptyList());

    final UUID sourceDefId = sourceDefinition.getSourceDefinitionId();
    final SourceConnection sourceConnection = createBaseSourceActor(sourceDefId);
    sourceService.writeSourceConnectionNoSecrets(sourceConnection);

    final UUID initialSourceDefinitionDefaultVersionId =
        sourceService.getStandardSourceDefinition(sourceDefId).getDefaultVersionId();
    assertNotNull(initialSourceDefinitionDefaultVersionId);

    // Introduce a breaking change between 0.0.1 and UPGRADE_IMAGE_TAG to make the upgrade breaking, but
    // with a version that will fail to write (due to null docker repo).
    // We want to check that the state is rolled back correctly.
    final String invalidVersion = "1.0.0";
    final List<ActorDefinitionBreakingChange> breakingChangesForDef =
        List.of(MockData.actorDefinitionBreakingChange(invalidVersion).withActorDefinitionId(sourceDefId));

    final UUID newVersionId = UUID.randomUUID();
    final ActorDefinitionVersion newVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(sourceDefId)
        .withVersionId(newVersionId)
        .withDockerRepository(null)
        .withDockerImageTag(invalidVersion)
        .withDocumentationUrl("https://www.something.new");

    final StandardSourceDefinition updatedSourceDefinition = Jsons.clone(sourceDefinition).withName("updated name");

    assertThrows(DataAccessException.class,
        () -> sourceService.writeConnectorMetadata(updatedSourceDefinition, newVersion, breakingChangesForDef));

    final UUID sourceDefinitionDefaultVersionIdAfterFailedUpgrade =
        sourceService.getStandardSourceDefinition(sourceDefId).getDefaultVersionId();
    final StandardSourceDefinition sourceDefinitionAfterFailedUpgrade =
        sourceService.getStandardSourceDefinition(sourceDefId);
    final Optional<ActorDefinitionVersion> newActorDefinitionVersionAfterFailedUpgrade =
        actorDefinitionService.getActorDefinitionVersion(sourceDefId, invalidVersion);
    final ActorDefinitionVersion defaultActorDefinitionVersionAfterFailedUpgrade =
        actorDefinitionService.getActorDefinitionVersion(sourceDefinitionDefaultVersionIdAfterFailedUpgrade);

    // New actor definition version was not persisted
    assertFalse(newActorDefinitionVersionAfterFailedUpgrade.isPresent());
    // Valid breaking change was not persisted
    assertEquals(0, actorDefinitionService.listBreakingChangesForActorDefinition(sourceDefId).size());

    // The default version does not get upgraded
    assertEquals(initialSourceDefinitionDefaultVersionId, sourceDefinitionDefaultVersionIdAfterFailedUpgrade);

    // Source definition metadata is the same as before
    assertEquals(sourceDefinition.withDefaultVersionId(initialADVId), sourceDefinitionAfterFailedUpgrade);
    // Actor definition metadata is the same as before
    assertEquals(actorDefinitionVersion1, defaultActorDefinitionVersionAfterFailedUpgrade);
  }

  private static StandardSourceDefinition createBaseSourceDef() {
    final UUID id = UUID.randomUUID();

    return new StandardSourceDefinition()
        .withName("source-def-" + id)
        .withSourceDefinitionId(id)
        .withTombstone(false)
        .withMaxSecondsBetweenMessages(MockData.DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES);
  }

  private static ActorDefinitionVersion createBaseActorDefVersion(final UUID actorDefId) {
    return new ActorDefinitionVersion()
        .withActorDefinitionId(actorDefId)
        .withDockerRepository("source-image-" + actorDefId)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withSupportLevel(SupportLevel.CERTIFIED)
        .withInternalSupportLevel(200L)
        .withSpec(new ConnectorSpecification()
            .withConnectionSpecification(Jsons.jsonNode(Map.of("key", "value1"))).withProtocolVersion(PROTOCOL_VERSION));
  }

  private static StandardDestinationDefinition createBaseDestDef() {
    final UUID id = UUID.randomUUID();

    return new StandardDestinationDefinition()
        .withName("source-def-" + id)
        .withDestinationDefinitionId(id)
        .withTombstone(false);
  }

  private static SourceConnection createBaseSourceActor(final UUID actorDefinitionId) {
    final UUID id = UUID.randomUUID();

    return new SourceConnection()
        .withSourceId(id)
        .withSourceDefinitionId(actorDefinitionId)
        .withWorkspaceId(WORKSPACE_ID)
        .withName("source-" + id);
  }

}
