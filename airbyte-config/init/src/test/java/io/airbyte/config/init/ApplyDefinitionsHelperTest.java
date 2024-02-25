/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import static io.airbyte.metrics.lib.OssMetricsRegistry.CONNECTOR_REGISTRY_DEFINITION_PROCESSED;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.BreakingChanges;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.ConnectorReleases;
import io.airbyte.config.VersionBreakingChange;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingFailureReason;
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingSuccessOutcome;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.specs.DefinitionsProvider;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
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

  private DefinitionsProvider definitionsProvider;
  private JobPersistence jobPersistence;
  private SupportStateUpdater supportStateUpdater;
  private ActorDefinitionService actorDefinitionService;
  private SourceService sourceService;
  private DestinationService destinationService;

  private MetricClient metricClient;
  private ApplyDefinitionsHelper applyDefinitionsHelper;
  private static final String INITIAL_CONNECTOR_VERSION = "0.1.0";
  private static final String UPDATED_CONNECTOR_VERSION = "0.2.0";
  private static final String BREAKING_CHANGE_VERSION = "1.0.0";

  private static final String PROTOCOL_VERSION = "2.0.0";

  protected static final UUID POSTGRES_ID = UUID.fromString("decd338e-5647-4c0b-adf4-da0e75f5a750");
  private static final BreakingChanges registryBreakingChanges =
      new BreakingChanges().withAdditionalProperty(BREAKING_CHANGE_VERSION, new VersionBreakingChange()
          .withMessage("Sample message").withUpgradeDeadline("2023-07-20").withMigrationDocumentationUrl("https://example.com"));

  protected static final ConnectorRegistrySourceDefinition SOURCE_POSTGRES = new ConnectorRegistrySourceDefinition()
      .withSourceDefinitionId(POSTGRES_ID)
      .withName("Postgres")
      .withDockerRepository("airbyte/source-postgres")
      .withDockerImageTag(INITIAL_CONNECTOR_VERSION)
      .withDocumentationUrl("https://docs.airbyte.io/integrations/sources/postgres")
      .withSpec(new ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION));
  protected static final ConnectorRegistrySourceDefinition SOURCE_POSTGRES_2 = new ConnectorRegistrySourceDefinition()
      .withSourceDefinitionId(POSTGRES_ID)
      .withName("Postgres - Updated")
      .withDockerRepository("airbyte/source-postgres")
      .withDockerImageTag(UPDATED_CONNECTOR_VERSION)
      .withDocumentationUrl("https://docs.airbyte.io/integrations/sources/postgres/new")
      .withSpec(new ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION))
      .withReleases(new ConnectorReleases().withBreakingChanges(registryBreakingChanges));

  protected static final UUID S3_ID = UUID.fromString("4816b78f-1489-44c1-9060-4b19d5fa9362");
  protected static final ConnectorRegistryDestinationDefinition DESTINATION_S3 = new ConnectorRegistryDestinationDefinition()
      .withName("S3")
      .withDestinationDefinitionId(S3_ID)
      .withDockerRepository("airbyte/destination-s3")
      .withDockerImageTag(INITIAL_CONNECTOR_VERSION)
      .withDocumentationUrl("https://docs.airbyte.io/integrations/destinations/s3")
      .withSpec(new ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION));
  protected static final ConnectorRegistryDestinationDefinition DESTINATION_S3_2 = new ConnectorRegistryDestinationDefinition()
      .withName("S3 - Updated")
      .withDestinationDefinitionId(S3_ID)
      .withDockerRepository("airbyte/destination-s3")
      .withDockerImageTag(UPDATED_CONNECTOR_VERSION)
      .withDocumentationUrl("https://docs.airbyte.io/integrations/destinations/s3/new")
      .withSpec(new ConnectorSpecification().withProtocolVersion(PROTOCOL_VERSION))
      .withReleases(new ConnectorReleases().withBreakingChanges(registryBreakingChanges));

  @BeforeEach
  void setup() {
    definitionsProvider = mock(DefinitionsProvider.class);
    jobPersistence = mock(JobPersistence.class);
    supportStateUpdater = mock(SupportStateUpdater.class);
    actorDefinitionService = mock(ActorDefinitionService.class);
    sourceService = mock(SourceService.class);
    destinationService = mock(DestinationService.class);

    metricClient = mock(MetricClient.class);

    applyDefinitionsHelper = new ApplyDefinitionsHelper(definitionsProvider, jobPersistence, actorDefinitionService, sourceService,
        destinationService, metricClient, supportStateUpdater);

    // Default calls to empty.
    when(definitionsProvider.getDestinationDefinitions()).thenReturn(Collections.emptyList());
    when(definitionsProvider.getSourceDefinitions()).thenReturn(Collections.emptyList());
  }

  private void mockSeedInitialDefinitions() throws IOException {
    final Map<UUID, ActorDefinitionVersion> seededDefinitionsAndDefaultVersions = new HashMap<>();
    seededDefinitionsAndDefaultVersions.put(POSTGRES_ID, ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES));
    seededDefinitionsAndDefaultVersions.put(S3_ID, ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3));
    when(actorDefinitionService.getActorDefinitionIdsToDefaultVersionsMap()).thenReturn(seededDefinitionsAndDefaultVersions);
  }

  private void verifyActorDefinitionServiceInteractions() throws IOException {
    verify(actorDefinitionService).getActorDefinitionIdsToDefaultVersionsMap();
    verify(actorDefinitionService).getActorDefinitionIdsInUse();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testNewConnectorIsWritten(final boolean updateAll)
      throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(definitionsProvider.getSourceDefinitions()).thenReturn(List.of(SOURCE_POSTGRES));
    when(definitionsProvider.getDestinationDefinitions()).thenReturn(List.of(DESTINATION_S3));

    applyDefinitionsHelper.apply(updateAll);
    verifyActorDefinitionServiceInteractions();

    verify(sourceService).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES));
    verify(destinationService).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3));
    List.of("airbyte/source-postgres", "airbyte/destination-s3").forEach(
        dockerRepo -> verify(metricClient, times(1)).count(
            CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
            1,
            new MetricAttribute("status", "ok"),
            new MetricAttribute("outcome", DefinitionProcessingSuccessOutcome.INITIAL_VERSION_ADDED.toString()),
            new MetricAttribute("docker_repository", dockerRepo),
            new MetricAttribute("docker_image_tag", INITIAL_CONNECTOR_VERSION)));
    verify(supportStateUpdater).updateSupportStates();

    verifyNoMoreInteractions(actorDefinitionService, sourceService, destinationService, supportStateUpdater, metricClient);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testConnectorIsUpdatedIfItIsNotInUse(final boolean updateAll)
      throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    mockSeedInitialDefinitions();
    when(actorDefinitionService.getActorDefinitionIdsInUse()).thenReturn(Set.of());

    // New definitions come in
    when(definitionsProvider.getSourceDefinitions()).thenReturn(List.of(SOURCE_POSTGRES_2));
    when(definitionsProvider.getDestinationDefinitions()).thenReturn(List.of(DESTINATION_S3_2));

    applyDefinitionsHelper.apply(updateAll);
    verifyActorDefinitionServiceInteractions();

    verify(sourceService).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES_2));
    verify(destinationService).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3_2));
    List.of("airbyte/source-postgres", "airbyte/destination-s3").forEach(
        dockerRepo -> verify(metricClient, times(1)).count(
            CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
            1,
            new MetricAttribute("status", "ok"),
            new MetricAttribute("outcome", DefinitionProcessingSuccessOutcome.DEFAULT_VERSION_UPDATED.toString()),
            new MetricAttribute("docker_repository", dockerRepo),
            new MetricAttribute("docker_image_tag", UPDATED_CONNECTOR_VERSION)));
    verify(supportStateUpdater).updateSupportStates();

    verifyNoMoreInteractions(actorDefinitionService, sourceService, destinationService, supportStateUpdater, metricClient);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testUpdateBehaviorIfConnectorIsInUse(final boolean updateAll)
      throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    mockSeedInitialDefinitions();
    when(actorDefinitionService.getActorDefinitionIdsInUse()).thenReturn(Set.of(POSTGRES_ID, S3_ID));

    when(definitionsProvider.getSourceDefinitions()).thenReturn(List.of(SOURCE_POSTGRES_2));
    when(definitionsProvider.getDestinationDefinitions()).thenReturn(List.of(DESTINATION_S3_2));

    applyDefinitionsHelper.apply(updateAll);
    verifyActorDefinitionServiceInteractions();

    if (updateAll) {
      verify(sourceService).writeConnectorMetadata(
          ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2),
          ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_2),
          ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES_2));
      verify(destinationService).writeConnectorMetadata(
          ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2),
          ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_2),
          ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3_2));
      List.of("airbyte/source-postgres", "airbyte/destination-s3").forEach(
          dockerRepo -> verify(metricClient, times(1)).count(
              CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
              1,
              new MetricAttribute("status", "ok"),
              new MetricAttribute("outcome", DefinitionProcessingSuccessOutcome.DEFAULT_VERSION_UPDATED.toString()),
              new MetricAttribute("docker_repository", dockerRepo),
              new MetricAttribute("docker_image_tag", UPDATED_CONNECTOR_VERSION)));
    } else {
      verify(sourceService).updateStandardSourceDefinition(ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2));
      verify(destinationService).updateStandardDestinationDefinition(ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2));
      verify(metricClient, times(2)).count(CONNECTOR_REGISTRY_DEFINITION_PROCESSED, 1, new MetricAttribute("status", "ok"),
          new MetricAttribute("outcome", DefinitionProcessingSuccessOutcome.VERSION_UNCHANGED.toString()));
    }
    verify(supportStateUpdater).updateSupportStates();

    verifyNoMoreInteractions(actorDefinitionService, sourceService, destinationService, supportStateUpdater, metricClient);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDefinitionsFiltering(final boolean updateAll)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(jobPersistence.getCurrentProtocolVersionRange())
        .thenReturn(Optional.of(new AirbyteProtocolVersionRange(new Version("2.0.0"), new Version("3.0.0"))));
    final ConnectorRegistrySourceDefinition postgresWithOldProtocolVersion =
        SOURCE_POSTGRES.withSpec(new ConnectorSpecification().withProtocolVersion("1.0.0"));
    final ConnectorRegistryDestinationDefinition s3withOldProtocolVersion =
        DESTINATION_S3.withSpec(new ConnectorSpecification().withProtocolVersion("1.0.0"));

    when(definitionsProvider.getSourceDefinitions()).thenReturn(List.of(postgresWithOldProtocolVersion, SOURCE_POSTGRES_2));
    when(definitionsProvider.getDestinationDefinitions()).thenReturn(List.of(s3withOldProtocolVersion, DESTINATION_S3_2));

    applyDefinitionsHelper.apply(updateAll);
    verifyActorDefinitionServiceInteractions();

    List.of("airbyte/source-postgres", "airbyte/destination-s3").forEach(
        dockerRepo -> verify(metricClient, times(1)).count(
            CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
            1,
            new MetricAttribute("status", "failed"),
            new MetricAttribute("outcome", DefinitionProcessingFailureReason.INCOMPATIBLE_PROTOCOL_VERSION.toString()),
            new MetricAttribute("docker_repository", dockerRepo),
            new MetricAttribute("docker_image_tag", INITIAL_CONNECTOR_VERSION)));

    verify(sourceService, never()).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(postgresWithOldProtocolVersion),
        ConnectorRegistryConverters.toActorDefinitionVersion(s3withOldProtocolVersion),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(postgresWithOldProtocolVersion));
    verify(destinationService, never()).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(s3withOldProtocolVersion),
        ConnectorRegistryConverters.toActorDefinitionVersion(postgresWithOldProtocolVersion),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(s3withOldProtocolVersion));

    verify(sourceService).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES_2));
    verify(destinationService).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3_2),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3_2));
    verify(supportStateUpdater).updateSupportStates();
    List.of("airbyte/source-postgres", "airbyte/destination-s3").forEach(
        dockerRepo -> verify(metricClient, times(1)).count(
            CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
            1,
            new MetricAttribute("status", "ok"),
            new MetricAttribute("outcome", DefinitionProcessingSuccessOutcome.INITIAL_VERSION_ADDED.toString()),
            new MetricAttribute("docker_repository", dockerRepo),
            new MetricAttribute("docker_image_tag", UPDATED_CONNECTOR_VERSION)));
    verifyNoMoreInteractions(actorDefinitionService, sourceService, destinationService, supportStateUpdater, metricClient);
  }

  @Test
  void testMalformedDefinitionDoesNotBlockOtherDefinitionsFromUpdating()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final ConnectorRegistrySourceDefinition malformedRegistrySourceDefinition =
        Jsons.clone(SOURCE_POSTGRES).withDockerImageTag("a-non-semantic-version-for-example");
    assertThrows(RuntimeException.class, () -> ConnectorRegistryConverters.toActorDefinitionVersion(malformedRegistrySourceDefinition));

    final ConnectorRegistryDestinationDefinition malformedRegistryDestinationDefinition =
        Jsons.clone(DESTINATION_S3).withDockerImageTag("a-non-semantic-version-for-example");
    assertThrows(RuntimeException.class, () -> ConnectorRegistryConverters.toActorDefinitionVersion(malformedRegistryDestinationDefinition));

    final ConnectorRegistrySourceDefinition anotherNewSourceDefinition =
        Jsons.clone(SOURCE_POSTGRES).withName("new").withDockerRepository("airbyte/source-new").withSourceDefinitionId(UUID.randomUUID());
    final ConnectorRegistryDestinationDefinition anotherNewDestinationDefinition =
        Jsons.clone(DESTINATION_S3).withName("new").withDockerRepository("airbyte/destination-new").withDestinationDefinitionId(UUID.randomUUID());

    when(definitionsProvider.getSourceDefinitions())
        .thenReturn(List.of(SOURCE_POSTGRES, malformedRegistrySourceDefinition, anotherNewSourceDefinition));
    when(definitionsProvider.getDestinationDefinitions())
        .thenReturn(List.of(DESTINATION_S3, malformedRegistryDestinationDefinition, anotherNewDestinationDefinition));

    applyDefinitionsHelper.apply(true);
    verifyActorDefinitionServiceInteractions();
    List.of("airbyte/source-postgres", "airbyte/destination-s3").forEach(
        dockerRepo -> verify(metricClient, times(1)).count(
            CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
            1,
            new MetricAttribute("status", "failed"),
            new MetricAttribute("outcome", DefinitionProcessingFailureReason.DEFINITION_CONVERSION_FAILED.toString()),
            new MetricAttribute("docker_repository", dockerRepo),
            new MetricAttribute("docker_image_tag", "a-non-semantic-version-for-example")));

    verify(sourceService).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(SOURCE_POSTGRES),
        ConnectorRegistryConverters.toActorDefinitionVersion(SOURCE_POSTGRES),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(SOURCE_POSTGRES));
    verify(destinationService).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(DESTINATION_S3),
        ConnectorRegistryConverters.toActorDefinitionVersion(DESTINATION_S3),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(DESTINATION_S3));
    verify(sourceService).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardSourceDefinition(anotherNewSourceDefinition),
        ConnectorRegistryConverters.toActorDefinitionVersion(anotherNewSourceDefinition),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(anotherNewSourceDefinition));
    verify(destinationService).writeConnectorMetadata(
        ConnectorRegistryConverters.toStandardDestinationDefinition(anotherNewDestinationDefinition),
        ConnectorRegistryConverters.toActorDefinitionVersion(anotherNewDestinationDefinition),
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(anotherNewDestinationDefinition));
    verify(supportStateUpdater).updateSupportStates();
    List.of("airbyte/source-postgres", "airbyte/destination-s3", "airbyte/source-new", "airbyte/destination-new").forEach(
        dockerRepo -> verify(metricClient, times(1)).count(
            CONNECTOR_REGISTRY_DEFINITION_PROCESSED,
            1,
            new MetricAttribute("status", "ok"),
            new MetricAttribute("outcome", DefinitionProcessingSuccessOutcome.INITIAL_VERSION_ADDED.toString()),
            new MetricAttribute("docker_repository", dockerRepo),
            new MetricAttribute("docker_image_tag", INITIAL_CONNECTOR_VERSION)));

    // The malformed definitions should not have been written.
    verifyNoMoreInteractions(actorDefinitionService, sourceService, destinationService, supportStateUpdater, metricClient);
  }

}
