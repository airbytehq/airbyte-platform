/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.BreakingChanges;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.ConnectorReleases;
import io.airbyte.config.VersionBreakingChange;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.specs.DefinitionsProvider;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.RunSupportStateUpdater;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test suite for the {@link ApplyDefinitionsHelper} class.
 */
class ApplyDefinitionsHelperTest {

  private ConfigRepository configRepository;
  private DefinitionsProvider definitionsProvider;
  private JobPersistence jobPersistence;
  private SupportStateUpdater supportStateUpdater;
  private FeatureFlagClient featureFlagClient;
  private ApplyDefinitionsHelper applyDefinitionsHelper;

  private static final String PROTOCOL_VERSION = "2.0.0";

  protected static final UUID POSTGRES_ID = UUID.fromString("decd338e-5647-4c0b-adf4-da0e75f5a750");
  private static final BreakingChanges registryBreakingChanges = new BreakingChanges().withAdditionalProperty("1.0.0", new VersionBreakingChange()
      .withMessage("Sample message").withUpgradeDeadline("2023-07-20").withMigrationDocumentationUrl("https://example.com"));

  protected static final ConnectorRegistrySourceDefinition SOURCE_POSTGRES = new ConnectorRegistrySourceDefinition()
      .withSourceDefinitionId(POSTGRES_ID)
      .withName("Postgres")
      .withDockerRepository("airbyte/source-postgres")
      .withDockerImageTag("0.3.11")
      .withDocumentationUrl("https://docs.airbyte.io/integrations/sources/postgres")
      .withSpec(new ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION));
  protected static final ConnectorRegistrySourceDefinition SOURCE_POSTGRES_2 = new ConnectorRegistrySourceDefinition()
      .withSourceDefinitionId(POSTGRES_ID)
      .withName("Postgres - Updated")
      .withDockerRepository("airbyte/source-postgres")
      .withDockerImageTag("0.4.0")
      .withDocumentationUrl("https://docs.airbyte.io/integrations/sources/postgres/new")
      .withSpec(new ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION))
      .withReleases(new ConnectorReleases().withBreakingChanges(registryBreakingChanges));

  protected static final UUID S3_ID = UUID.fromString("4816b78f-1489-44c1-9060-4b19d5fa9362");
  protected static final ConnectorRegistryDestinationDefinition DESTINATION_S3 = new ConnectorRegistryDestinationDefinition()
      .withName("S3")
      .withDestinationDefinitionId(S3_ID)
      .withDockerRepository("airbyte/destination-s3")
      .withDockerImageTag("0.1.12")
      .withDocumentationUrl("https://docs.airbyte.io/integrations/destinations/s3")
      .withSpec(new ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION));
  protected static final ConnectorRegistryDestinationDefinition DESTINATION_S3_2 = new ConnectorRegistryDestinationDefinition()
      .withName("S3 - Updated")
      .withDestinationDefinitionId(S3_ID)
      .withDockerRepository("airbyte/destination-s3")
      .withDockerImageTag("0.2.0")
      .withDocumentationUrl("https://docs.airbyte.io/integrations/destinations/s3/new")
      .withSpec(new ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION))
      .withReleases(new ConnectorReleases().withBreakingChanges(registryBreakingChanges));

  @BeforeEach
  void setup() {
    configRepository = mock(ConfigRepository.class);
    definitionsProvider = mock(DefinitionsProvider.class);
    jobPersistence = mock(JobPersistence.class);
    supportStateUpdater = mock(SupportStateUpdater.class);
    featureFlagClient = mock(TestClient.class);

    applyDefinitionsHelper =
        new ApplyDefinitionsHelper(definitionsProvider, jobPersistence, configRepository, featureFlagClient, supportStateUpdater);

    when(featureFlagClient.boolVariation(RunSupportStateUpdater.INSTANCE, new Workspace(ANONYMOUS))).thenReturn(true);

    // Default calls to empty.
    when(definitionsProvider.getDestinationDefinitions()).thenReturn(Collections.emptyList());
    when(definitionsProvider.getSourceDefinitions()).thenReturn(Collections.emptyList());
  }

  private void mockSeedInitialDefinitions() throws IOException {
    final Map<UUID, ActorDefinitionVersion> seededDefinitionsAndDefaultVersions = new HashMap<>();
    seededDefinitionsAndDefaultVersions.put(POSTGRES_ID, ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES));
    seededDefinitionsAndDefaultVersions.put(S3_ID, ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3));
    when(configRepository.getActorDefinitionIdsToDefaultVersionsMap()).thenReturn(seededDefinitionsAndDefaultVersions);
  }

  private void verifyConfigRepositoryGetInteractions() throws IOException {
    verify(configRepository).getActorDefinitionIdsToDefaultVersionsMap();
    verify(configRepository).getActorDefinitionIdsInUse();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testNewConnectorIsWritten(final boolean updateAll)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    when(definitionsProvider.getSourceDefinitions()).thenReturn(List.of(SOURCE_POSTGRES));
    when(definitionsProvider.getDestinationDefinitions()).thenReturn(List.of(DESTINATION_S3));

    applyDefinitionsHelper.apply(updateAll);
    verifyConfigRepositoryGetInteractions();

    verify(configRepository).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES));
    verify(configRepository).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3));
    verify(supportStateUpdater).updateSupportStates();

    verifyNoMoreInteractions(configRepository, supportStateUpdater);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testConnectorIsUpdatedIfItIsNotInUse(final boolean updateAll)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    mockSeedInitialDefinitions();
    when(configRepository.getActorDefinitionIdsInUse()).thenReturn(Set.of());

    // New definitions come in
    when(definitionsProvider.getSourceDefinitions()).thenReturn(List.of(SOURCE_POSTGRES_2));
    when(definitionsProvider.getDestinationDefinitions()).thenReturn(List.of(DESTINATION_S3_2));

    applyDefinitionsHelper.apply(updateAll);
    verifyConfigRepositoryGetInteractions();

    verify(configRepository).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES_2));
    verify(configRepository).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3_2));
    verify(supportStateUpdater).updateSupportStates();

    verifyNoMoreInteractions(configRepository, supportStateUpdater);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testUpdateBehaviorIfConnectorIsInUse(final boolean updateAll)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    mockSeedInitialDefinitions();
    when(configRepository.getActorDefinitionIdsInUse()).thenReturn(Set.of(POSTGRES_ID, S3_ID));

    when(definitionsProvider.getSourceDefinitions()).thenReturn(List.of(SOURCE_POSTGRES_2));
    when(definitionsProvider.getDestinationDefinitions()).thenReturn(List.of(DESTINATION_S3_2));

    applyDefinitionsHelper.apply(updateAll);
    verifyConfigRepositoryGetInteractions();

    if (updateAll) {
      verify(configRepository).writeConnectorMetadata(
          ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2),
          ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_2),
          ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES_2));
      verify(configRepository).writeConnectorMetadata(
          ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2),
          ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_2),
          ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3_2));
    } else {
      verify(configRepository).updateStandardSourceDefinition(ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2));
      verify(configRepository).updateStandardDestinationDefinition(ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2));
    }
    verify(supportStateUpdater).updateSupportStates();

    verifyNoMoreInteractions(configRepository, supportStateUpdater);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDefinitionsFiltering(final boolean updateAll)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    when(jobPersistence.getCurrentProtocolVersionRange())
        .thenReturn(Optional.of(new AirbyteProtocolVersionRange(new Version("2.0.0"), new Version("3.0.0"))));
    final ConnectorRegistrySourceDefinition postgresWithOldProtocolVersion =
        SOURCE_POSTGRES.withSpec(new ConnectorSpecification().withProtocolVersion("1.0.0"));
    final ConnectorRegistryDestinationDefinition s3withOldProtocolVersion =
        DESTINATION_S3.withSpec(new ConnectorSpecification().withProtocolVersion("1.0.0"));

    when(definitionsProvider.getSourceDefinitions()).thenReturn(List.of(postgresWithOldProtocolVersion, SOURCE_POSTGRES_2));
    when(definitionsProvider.getDestinationDefinitions()).thenReturn(List.of(s3withOldProtocolVersion, DESTINATION_S3_2));

    applyDefinitionsHelper.apply(updateAll);
    verifyConfigRepositoryGetInteractions();

    verify(configRepository, never()).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(postgresWithOldProtocolVersion),
        ConnectorRegistryConverters.toActorDefinitionVersion(s3withOldProtocolVersion),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(postgresWithOldProtocolVersion));
    verify(configRepository, never()).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(s3withOldProtocolVersion),
        ConnectorRegistryConverters.toActorDefinitionVersion(postgresWithOldProtocolVersion),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(s3withOldProtocolVersion));

    verify(configRepository).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES_2));
    verify(configRepository).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3_2));
    verify(supportStateUpdater).updateSupportStates();

    verifyNoMoreInteractions(configRepository, supportStateUpdater);
  }

  @Test
  void testTurnOffRunSupportStateUpdaterFeatureFlag() throws JsonValidationException, ConfigNotFoundException, IOException {
    when(featureFlagClient.boolVariation(RunSupportStateUpdater.INSTANCE, new Workspace(ANONYMOUS))).thenReturn(false);

    when(definitionsProvider.getSourceDefinitions()).thenReturn(List.of(SOURCE_POSTGRES_2));
    when(definitionsProvider.getDestinationDefinitions()).thenReturn(List.of(DESTINATION_S3_2));

    applyDefinitionsHelper.apply(true);
    verifyConfigRepositoryGetInteractions();

    verify(configRepository).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES_2));
    verify(configRepository).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3_2));

    verify(supportStateUpdater, never()).updateSupportStates();
    verifyNoMoreInteractions(configRepository, supportStateUpdater);
  }

}
