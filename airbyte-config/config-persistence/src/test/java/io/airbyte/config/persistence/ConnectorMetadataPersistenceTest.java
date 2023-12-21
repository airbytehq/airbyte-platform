/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.BreakingChangeScope;
import io.airbyte.config.BreakingChangeScope.ScopeType;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Geography;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardWorkspace;
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
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for configRepository methods that write connector metadata together. Includes writing
 * global metadata (source/destination definitions and breaking changes) and versioned metadata
 * (actor definition versions) as well as the logic that determines whether actors should be
 * upgraded upon changing the dockerImageTag.
 */
class ConnectorMetadataPersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID WORKSPACE_ID = UUID.randomUUID();

  private static final String DOCKER_IMAGE_TAG = "0.0.1";

  private static final String UPGRADE_IMAGE_TAG = "0.0.2";
  private static final String PROTOCOL_VERSION = "1.0.0";

  private ConfigRepository configRepository;

  @BeforeEach
  void setup() throws SQLException, JsonValidationException, IOException {
    truncateAllTables();
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    configRepository = new ConfigRepository(
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
            secretPersistenceConfigService));
    configRepository.writeStandardWorkspaceNoSecrets(new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("default")
        .withSlug("workspace-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withDefaultGeography(Geography.US));
  }

  @Test
  void testWriteConnectorMetadataForSource()
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // Initial insert
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion1 = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());

    configRepository.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion1);

    StandardSourceDefinition sourceDefinitionFromDB = configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId());
    final Optional<ActorDefinitionVersion> actorDefinitionVersionFromDB =
        configRepository.getActorDefinitionVersion(actorDefinitionVersion1.getActorDefinitionId(), actorDefinitionVersion1.getDockerImageTag());

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
    configRepository.writeConnectorMetadata(sourceDefinition2, actorDefinitionVersion2, breakingChanges);

    sourceDefinitionFromDB = configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId());
    final Optional<ActorDefinitionVersion> actorDefinitionVersion2FromDB =
        configRepository.getActorDefinitionVersion(actorDefinitionVersion2.getActorDefinitionId(), actorDefinitionVersion2.getDockerImageTag());
    final List<ActorDefinitionBreakingChange> breakingChangesForDefFromDb =
        configRepository.listBreakingChangesForActorDefinition(sourceDefinition2.getSourceDefinitionId());

    assertTrue(actorDefinitionVersion2FromDB.isPresent());
    final UUID newADVId = actorDefinitionVersion2FromDB.get().getVersionId();

    assertNotEquals(firstVersionId, newADVId);
    assertEquals(newADVId, sourceDefinitionFromDB.getDefaultVersionId());
    assertEquals(sourceDefinition2.withDefaultVersionId(newADVId), sourceDefinitionFromDB);
    assertThat(breakingChangesForDefFromDb).containsExactlyInAnyOrderElementsOf(breakingChanges);
  }

  @Test
  void testWriteConnectorMetadataForDestination()
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // Initial insert
    final StandardDestinationDefinition destinationDefinition = createBaseDestDef();
    final ActorDefinitionVersion actorDefinitionVersion1 = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());

    configRepository.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion1);

    StandardDestinationDefinition destinationDefinitionFromDB =
        configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId());
    final Optional<ActorDefinitionVersion> actorDefinitionVersionFromDB =
        configRepository.getActorDefinitionVersion(actorDefinitionVersion1.getActorDefinitionId(), actorDefinitionVersion1.getDockerImageTag());

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
    configRepository.writeConnectorMetadata(destinationDefinition2, actorDefinitionVersion2, breakingChanges);

    destinationDefinitionFromDB = configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId());
    final Optional<ActorDefinitionVersion> actorDefinitionVersion2FromDB =
        configRepository.getActorDefinitionVersion(actorDefinitionVersion2.getActorDefinitionId(), actorDefinitionVersion2.getDockerImageTag());
    final List<ActorDefinitionBreakingChange> breakingChangesForDefFromDb =
        configRepository.listBreakingChangesForActorDefinition(destinationDefinition2.getDestinationDefinitionId());

    assertTrue(actorDefinitionVersion2FromDB.isPresent());
    final UUID newADVId = actorDefinitionVersion2FromDB.get().getVersionId();

    assertNotEquals(firstVersionId, newADVId);
    assertEquals(newADVId, destinationDefinitionFromDB.getDefaultVersionId());
    assertEquals(destinationDefinition2.withDefaultVersionId(newADVId), destinationDefinitionFromDB);
    assertThat(breakingChangesForDefFromDb).containsExactlyInAnyOrderElementsOf(breakingChanges);
  }

  @Test
  void testUpdateConnectorMetadata() throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final UUID actorDefinitionId = sourceDefinition.getSourceDefinitionId();
    final ActorDefinitionVersion actorDefinitionVersion1 = createBaseActorDefVersion(actorDefinitionId);
    configRepository.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion1);

    final Optional<ActorDefinitionVersion> optADVForTag = configRepository.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG);
    assertTrue(optADVForTag.isPresent());
    final ActorDefinitionVersion advForTag = optADVForTag.get();
    final StandardSourceDefinition retrievedSourceDefinition =
        configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId());
    assertEquals(retrievedSourceDefinition.getDefaultVersionId(), advForTag.getVersionId());

    final ConnectorSpecification updatedSpec = new ConnectorSpecification()
        .withConnectionSpecification(Jsons.jsonNode(Map.of("key", "value2"))).withProtocolVersion(PROTOCOL_VERSION);

    // Modify spec without changing docker image tag
    final ActorDefinitionVersion modifiedADV = createBaseActorDefVersion(actorDefinitionId).withSpec(updatedSpec);
    configRepository.writeConnectorMetadata(sourceDefinition, modifiedADV);

    assertEquals(retrievedSourceDefinition, configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId()));
    final Optional<ActorDefinitionVersion> optADVForTagAfterCall2 = configRepository.getActorDefinitionVersion(actorDefinitionId, DOCKER_IMAGE_TAG);
    assertTrue(optADVForTagAfterCall2.isPresent());

    // Modifying fields without updating image tag updates existing version
    assertEquals(modifiedADV.withVersionId(advForTag.getVersionId()), optADVForTagAfterCall2.get());

    // Modifying docker image tag creates a new version
    final ActorDefinitionVersion newADV =
        createBaseActorDefVersion(actorDefinitionId).withDockerImageTag(UPGRADE_IMAGE_TAG).withSpec(updatedSpec);
    configRepository.writeConnectorMetadata(sourceDefinition, newADV);

    final Optional<ActorDefinitionVersion> optADVForTag2 = configRepository.getActorDefinitionVersion(actorDefinitionId, UPGRADE_IMAGE_TAG);
    assertTrue(optADVForTag2.isPresent());
    final ActorDefinitionVersion advForTag2 = optADVForTag2.get();

    // Versioned data is updated as well as the version id
    assertEquals(advForTag2, newADV.withVersionId(advForTag2.getVersionId()));
    assertNotEquals(advForTag2.getVersionId(), advForTag.getVersionId());
    assertNotEquals(advForTag2.getSpec(), advForTag.getSpec());
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
    configRepository.writeConnectorMetadata(customSourceDefinition, sourceActorDefinitionVersion);
    configRepository.writeConnectorMetadata(customDestinationDefinition, destinationActorDefinitionVersion);

    // Update
    assertDoesNotThrow(() -> configRepository.writeConnectorMetadata(customSourceDefinition,
        createBaseActorDefVersion(customSourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag), List.of()));
    assertDoesNotThrow(() -> configRepository.writeConnectorMetadata(customDestinationDefinition,
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
    configRepository.writeConnectorMetadata(sourceDefinition, sourceActorDefinitionVersion);
    configRepository.writeConnectorMetadata(destinationDefinition, destinationActorDefinitionVersion);

    // Update
    assertDoesNotThrow(() -> configRepository.writeConnectorMetadata(sourceDefinition,
        createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag), List.of()));
    assertDoesNotThrow(() -> configRepository.writeConnectorMetadata(destinationDefinition,
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
    configRepository.writeConnectorMetadata(sourceDefinition, sourceActorDefinitionVersion);
    configRepository.writeConnectorMetadata(destinationDefinition, destinationActorDefinitionVersion);

    final List<ActorDefinitionBreakingChange> sourceBreakingChanges =
        List.of(MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG).withActorDefinitionId(sourceDefinition.getSourceDefinitionId()));
    final List<ActorDefinitionBreakingChange> destinationBreakingChanges =
        List.of(MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG).withActorDefinitionId(destinationDefinition.getDestinationDefinitionId()));

    // Update
    if (upgradeShouldSucceed) {
      assertDoesNotThrow(() -> configRepository.writeConnectorMetadata(sourceDefinition,
          createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag),
          sourceBreakingChanges));
      assertDoesNotThrow(() -> configRepository.writeConnectorMetadata(destinationDefinition,
          createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag),
          destinationBreakingChanges));
    } else {
      assertThrows(IllegalArgumentException.class, () -> configRepository.writeConnectorMetadata(sourceDefinition,
          createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag), sourceBreakingChanges));
      assertThrows(IllegalArgumentException.class, () -> configRepository.writeConnectorMetadata(destinationDefinition,
          createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag),
          destinationBreakingChanges));
    }
  }

  @Test
  void testSourceDefaultVersionIsUpgradedOnNonbreakingUpgrade() throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion1 = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());

    configRepository.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion1);

    final UUID sourceDefId = sourceDefinition.getSourceDefinitionId();
    final SourceConnection sourceConnection = createBaseSourceActor(sourceDefId);
    configRepository.writeSourceConnectionNoSecrets(sourceConnection);

    final UUID initialSourceDefinitionDefaultVersionId = configRepository.getStandardSourceDefinition(sourceDefId).getDefaultVersionId();
    final UUID initialSourceDefaultVersionId = configRepository.getSourceConnection(sourceConnection.getSourceId()).getDefaultVersionId();
    assertNotNull(initialSourceDefinitionDefaultVersionId);
    assertEquals(initialSourceDefinitionDefaultVersionId, initialSourceDefaultVersionId);

    final UUID newVersionId = UUID.randomUUID();
    final ActorDefinitionVersion newVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(sourceDefId)
        .withVersionId(newVersionId)
        .withDockerImageTag(UPGRADE_IMAGE_TAG);

    configRepository.writeConnectorMetadata(sourceDefinition, newVersion);
    final UUID sourceDefinitionDefaultVersionIdAfterUpgrade = configRepository.getStandardSourceDefinition(sourceDefId).getDefaultVersionId();
    final UUID sourceDefaultVersionIdAfterUpgrade = configRepository.getSourceConnection(sourceConnection.getSourceId()).getDefaultVersionId();

    assertEquals(newVersionId, sourceDefinitionDefaultVersionIdAfterUpgrade);
    assertEquals(newVersionId, sourceDefaultVersionIdAfterUpgrade);
  }

  @Test
  void testDestinationDefaultVersionIsUpgradedOnNonbreakingUpgrade() throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardDestinationDefinition destinationDefinition = createBaseDestDef();
    final ActorDefinitionVersion actorDefinitionVersion1 = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());

    configRepository.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion1);

    final UUID destinationDefId = destinationDefinition.getDestinationDefinitionId();
    final DestinationConnection destinationConnection = createBaseDestinationActor(destinationDefId);
    configRepository.writeDestinationConnectionNoSecrets(destinationConnection);

    final UUID initialDestinationDefinitionDefaultVersionId =
        configRepository.getStandardDestinationDefinition(destinationDefId).getDefaultVersionId();
    final UUID initialDestinationDefaultVersionId =
        configRepository.getDestinationConnection(destinationConnection.getDestinationId()).getDefaultVersionId();
    assertNotNull(initialDestinationDefinitionDefaultVersionId);
    assertEquals(initialDestinationDefinitionDefaultVersionId, initialDestinationDefaultVersionId);

    final UUID newVersionId = UUID.randomUUID();
    final ActorDefinitionVersion newVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(destinationDefId)
        .withVersionId(newVersionId)
        .withDockerImageTag(UPGRADE_IMAGE_TAG);

    configRepository.writeConnectorMetadata(destinationDefinition, newVersion);
    final UUID destinationDefinitionDefaultVersionIdAfterUpgrade =
        configRepository.getStandardDestinationDefinition(destinationDefId).getDefaultVersionId();
    final UUID destinationDefaultVersionIdAfterUpgrade =
        configRepository.getDestinationConnection(destinationConnection.getDestinationId()).getDefaultVersionId();

    assertEquals(newVersionId, destinationDefinitionDefaultVersionIdAfterUpgrade);
    assertEquals(newVersionId, destinationDefaultVersionIdAfterUpgrade);
  }

  @Test
  void testDestinationDefaultVersionIsNotModifiedOnBreakingUpgrade() throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardDestinationDefinition destinationDefinition = createBaseDestDef();
    final ActorDefinitionVersion actorDefinitionVersion1 = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());

    configRepository.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion1);

    final UUID destinationDefId = destinationDefinition.getDestinationDefinitionId();
    final DestinationConnection destinationConnection = createBaseDestinationActor(destinationDefId);
    configRepository.writeDestinationConnectionNoSecrets(destinationConnection);

    final UUID initialDestinationDefinitionDefaultVersionId =
        configRepository.getStandardDestinationDefinition(destinationDefId).getDefaultVersionId();
    final UUID initialDestinationDefaultVersionId =
        configRepository.getDestinationConnection(destinationConnection.getDestinationId()).getDefaultVersionId();
    assertNotNull(initialDestinationDefinitionDefaultVersionId);
    assertEquals(initialDestinationDefinitionDefaultVersionId, initialDestinationDefaultVersionId);

    // Introduce a breaking change between 0.0.1 and UPGRADE_IMAGE_TAG to make the upgrade breaking
    final List<ActorDefinitionBreakingChange> breakingChangesForDef =
        List.of(MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG).withActorDefinitionId(destinationDefId));

    final UUID newVersionId = UUID.randomUUID();
    final ActorDefinitionVersion newVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(destinationDefId)
        .withVersionId(newVersionId)
        .withDockerImageTag(UPGRADE_IMAGE_TAG);

    configRepository.writeConnectorMetadata(destinationDefinition, newVersion, breakingChangesForDef);
    final UUID destinationDefinitionDefaultVersionIdAfterUpgrade =
        configRepository.getStandardDestinationDefinition(destinationDefId).getDefaultVersionId();
    final UUID destinationDefaultVersionIdAfterUpgrade =
        configRepository.getDestinationConnection(destinationConnection.getDestinationId()).getDefaultVersionId();

    assertEquals(newVersionId, destinationDefinitionDefaultVersionIdAfterUpgrade);
    assertEquals(initialDestinationDefaultVersionId, destinationDefaultVersionIdAfterUpgrade);
  }

  @Test
  void testSourceDefaultVersionIsNotModifiedOnBreakingUpgrade()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion1 = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());

    configRepository.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion1);

    final UUID sourceDefId = sourceDefinition.getSourceDefinitionId();
    final SourceConnection sourceConnection = createBaseSourceActor(sourceDefId);
    configRepository.writeSourceConnectionNoSecrets(sourceConnection);

    final UUID initialSourceDefinitionDefaultVersionId =
        configRepository.getStandardSourceDefinition(sourceDefId).getDefaultVersionId();
    final UUID initialSourceDefaultVersionId =
        configRepository.getSourceConnection(sourceConnection.getSourceId()).getDefaultVersionId();
    assertNotNull(initialSourceDefinitionDefaultVersionId);
    assertEquals(initialSourceDefinitionDefaultVersionId, initialSourceDefaultVersionId);

    // Introduce a breaking change between 0.0.1 and UPGRADE_IMAGE_TAG to make the upgrade breaking
    final List<ActorDefinitionBreakingChange> breakingChangesForDef =
        List.of(MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG).withActorDefinitionId(sourceDefId));

    final UUID newVersionId = UUID.randomUUID();
    final ActorDefinitionVersion newVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(sourceDefId)
        .withVersionId(newVersionId)
        .withDockerImageTag(UPGRADE_IMAGE_TAG);

    configRepository.writeConnectorMetadata(sourceDefinition, newVersion, breakingChangesForDef);
    final UUID sourceDefinitionDefaultVersionIdAfterUpgrade =
        configRepository.getStandardSourceDefinition(sourceDefId).getDefaultVersionId();
    final UUID sourceDefaultVersionIdAfterUpgrade =
        configRepository.getSourceConnection(sourceConnection.getSourceId()).getDefaultVersionId();

    assertEquals(newVersionId, sourceDefinitionDefaultVersionIdAfterUpgrade);
    assertEquals(initialSourceDefaultVersionId, sourceDefaultVersionIdAfterUpgrade);
  }

  @Test
  void testTransactionRollbackOnFailure() throws IOException, JsonValidationException, ConfigNotFoundException {
    final UUID initialADVId = UUID.randomUUID();
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion1 =
        createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withVersionId(initialADVId);

    configRepository.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion1);

    final UUID sourceDefId = sourceDefinition.getSourceDefinitionId();
    final SourceConnection sourceConnection = createBaseSourceActor(sourceDefId);
    configRepository.writeSourceConnectionNoSecrets(sourceConnection);

    final UUID initialSourceDefinitionDefaultVersionId =
        configRepository.getStandardSourceDefinition(sourceDefId).getDefaultVersionId();
    final UUID initialSourceDefaultVersionId =
        configRepository.getSourceConnection(sourceConnection.getSourceId()).getDefaultVersionId();
    assertNotNull(initialSourceDefinitionDefaultVersionId);
    assertEquals(initialSourceDefinitionDefaultVersionId, initialSourceDefaultVersionId);

    // Introduce a breaking change between 0.0.1 and UPGRADE_IMAGE_TAG to make the upgrade breaking, but
    // with a tag that will
    // fail validation. We want to check that the state is rolled back correctly.
    final String invalidUpgradeTag = "1.0";
    final List<ActorDefinitionBreakingChange> breakingChangesForDef =
        List.of(MockData.actorDefinitionBreakingChange("1.0.0").withActorDefinitionId(sourceDefId));

    final UUID newVersionId = UUID.randomUUID();
    final ActorDefinitionVersion newVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(sourceDefId)
        .withVersionId(newVersionId)
        .withDockerImageTag(invalidUpgradeTag)
        .withDocumentationUrl("https://www.something.new");

    final StandardSourceDefinition updatedSourceDefinition = Jsons.clone(sourceDefinition).withName("updated name");

    assertThrows(IllegalArgumentException.class,
        () -> configRepository.writeConnectorMetadata(updatedSourceDefinition, newVersion, breakingChangesForDef));

    final UUID sourceDefinitionDefaultVersionIdAfterFailedUpgrade =
        configRepository.getStandardSourceDefinition(sourceDefId).getDefaultVersionId();
    final UUID sourceDefaultVersionIdAfterFailedUpgrade =
        configRepository.getSourceConnection(sourceConnection.getSourceId()).getDefaultVersionId();
    final StandardSourceDefinition sourceDefinitionAfterFailedUpgrade =
        configRepository.getStandardSourceDefinition(sourceDefId);
    final Optional<ActorDefinitionVersion> newActorDefinitionVersionAfterFailedUpgrade =
        configRepository.getActorDefinitionVersion(sourceDefId, invalidUpgradeTag);
    final ActorDefinitionVersion defaultActorDefinitionVersionAfterFailedUpgrade =
        configRepository.getActorDefinitionVersion(sourceDefinitionDefaultVersionIdAfterFailedUpgrade);

    // New actor definition version was not persisted
    assertFalse(newActorDefinitionVersionAfterFailedUpgrade.isPresent());
    // Valid breaking change was not persisted
    assertEquals(0, configRepository.listBreakingChangesForActorDefinition(sourceDefId).size());

    // Neither the default version nor the actors get upgraded, the actors are still on the default
    // version
    assertEquals(initialSourceDefaultVersionId, sourceDefinitionDefaultVersionIdAfterFailedUpgrade);
    assertEquals(initialSourceDefaultVersionId, sourceDefaultVersionIdAfterFailedUpgrade);

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

  private static DestinationConnection createBaseDestinationActor(final UUID actorDefinitionId) {
    final UUID id = UUID.randomUUID();

    return new DestinationConnection()
        .withDestinationId(id)
        .withDestinationDefinitionId(actorDefinitionId)
        .withWorkspaceId(WORKSPACE_ID)
        .withName("destination-" + id);
  }

}
