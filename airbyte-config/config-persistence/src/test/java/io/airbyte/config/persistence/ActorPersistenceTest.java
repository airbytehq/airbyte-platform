/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Geography;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardWorkspace;
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
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorPersistenceTest extends BaseConfigDatabaseTest {

  private ConfigRepository configRepository;
  private StandardSourceDefinition standardSourceDefinition;
  private StandardDestinationDefinition standardDestinationDefinition;
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final String SOURCE_NAME = "My Source";
  private static final String DESTINATION_NAME = "My Destination";
  private static final String UPGRADE_IMAGE_TAG = "9.9.9";

  @BeforeEach
  void setup() throws SQLException, IOException, JsonValidationException {
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
    standardSourceDefinition = MockData.publicSourceDefinition();
    standardDestinationDefinition = MockData.publicDestinationDefinition();
    configRepository.writeConnectorMetadata(standardSourceDefinition, MockData.actorDefinitionVersion()
        .withActorDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .withVersionId(standardSourceDefinition.getDefaultVersionId()));
    configRepository.writeConnectorMetadata(standardDestinationDefinition, MockData.actorDefinitionVersion()
        .withActorDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .withVersionId(standardDestinationDefinition.getDefaultVersionId()));
    configRepository.writeStandardWorkspaceNoSecrets(new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("default")
        .withSlug("workspace-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withDefaultGeography(Geography.US));
  }

  @Test
  void testNewSourceGetsActorDefinitionDefaultVersionId() throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection sourceConnection = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withSourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName(SOURCE_NAME);
    configRepository.writeSourceConnectionNoSecrets(sourceConnection);
    final SourceConnection sourceConnectionFromDb = configRepository.getSourceConnection(sourceConnection.getSourceId());
    assertNotNull(sourceConnectionFromDb.getDefaultVersionId());
    assertEquals(standardSourceDefinition.getDefaultVersionId(), sourceConnectionFromDb.getDefaultVersionId());
  }

  @Test
  void testNewDestinationGetsActorDefinitionDefaultVersionId() throws IOException, JsonValidationException, ConfigNotFoundException {
    final DestinationConnection destinationConnection = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withDestinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName(DESTINATION_NAME);
    configRepository.writeDestinationConnectionNoSecrets(destinationConnection);
    final DestinationConnection destinationConnectionFromDb = configRepository.getDestinationConnection(destinationConnection.getDestinationId());
    assertNotNull(destinationConnectionFromDb.getDefaultVersionId());
    assertEquals(standardDestinationDefinition.getDefaultVersionId(), destinationConnectionFromDb.getDefaultVersionId());
  }

  @Test
  void testSetSourceDefaultVersion() throws IOException, JsonValidationException, ConfigNotFoundException {
    final ActorDefinitionVersion newActorDefinitionVersion = configRepository.writeActorDefinitionVersion(MockData.actorDefinitionVersion()
        .withActorDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .withVersionId(UUID.randomUUID())
        .withDockerImageTag(UPGRADE_IMAGE_TAG));

    final SourceConnection sourceConnection = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withSourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName(SOURCE_NAME);
    configRepository.writeSourceConnectionNoSecrets(sourceConnection);

    configRepository.setActorDefaultVersion(sourceConnection.getSourceId(), newActorDefinitionVersion.getVersionId());

    final SourceConnection sourceConnectionFromDb = configRepository.getSourceConnection(sourceConnection.getSourceId());
    assertEquals(newActorDefinitionVersion.getVersionId(), sourceConnectionFromDb.getDefaultVersionId());
  }

  @Test
  void testSetDestinationDefaultVersion() throws IOException, JsonValidationException, ConfigNotFoundException {
    final ActorDefinitionVersion newActorDefinitionVersion = configRepository.writeActorDefinitionVersion(MockData.actorDefinitionVersion()
        .withActorDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .withVersionId(UUID.randomUUID())
        .withDockerImageTag(UPGRADE_IMAGE_TAG));

    final DestinationConnection destinationConnection = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withDestinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName(DESTINATION_NAME);
    configRepository.writeDestinationConnectionNoSecrets(destinationConnection);

    configRepository.setActorDefaultVersion(destinationConnection.getDestinationId(), newActorDefinitionVersion.getVersionId());

    final DestinationConnection destinationConnectionFromDb = configRepository.getDestinationConnection(destinationConnection.getDestinationId());
    assertEquals(newActorDefinitionVersion.getVersionId(), destinationConnectionFromDb.getDefaultVersionId());
  }

  @Test
  void testGetSourcesWithVersions() throws IOException {
    final SourceConnection sourceConnection1 = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withSourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName(SOURCE_NAME);
    configRepository.writeSourceConnectionNoSecrets(sourceConnection1);

    final SourceConnection sourceConnection2 = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withSourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName(SOURCE_NAME);
    configRepository.writeSourceConnectionNoSecrets(sourceConnection2);

    final List<SourceConnection> sourceConnections =
        configRepository.listSourcesWithVersionIds(List.of(standardSourceDefinition.getDefaultVersionId()));
    assertEquals(2, sourceConnections.size());
    assertEquals(
        Stream.of(sourceConnection1.getSourceId(), sourceConnection2.getSourceId()).sorted().toList(),
        sourceConnections.stream().map(SourceConnection::getSourceId).sorted().toList());

    final ActorDefinitionVersion newActorDefinitionVersion = configRepository.writeActorDefinitionVersion(MockData.actorDefinitionVersion()
        .withActorDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .withVersionId(UUID.randomUUID())
        .withDockerImageTag(UPGRADE_IMAGE_TAG));

    configRepository.setActorDefaultVersion(sourceConnection1.getSourceId(), newActorDefinitionVersion.getVersionId());

    final List<SourceConnection> sourcesWithNewVersion =
        configRepository.listSourcesWithVersionIds(List.of(newActorDefinitionVersion.getVersionId()));
    assertEquals(1, sourcesWithNewVersion.size());
    assertEquals(sourceConnection1.getSourceId(), sourcesWithNewVersion.get(0).getSourceId());

    final List<SourceConnection> sourcesWithOldVersion =
        configRepository.listSourcesWithVersionIds(List.of(standardSourceDefinition.getDefaultVersionId()));
    assertEquals(1, sourcesWithOldVersion.size());
    assertEquals(sourceConnection2.getSourceId(), sourcesWithOldVersion.get(0).getSourceId());

    final List<SourceConnection> sourcesWithBothVersions =
        configRepository.listSourcesWithVersionIds(List.of(standardSourceDefinition.getDefaultVersionId(), newActorDefinitionVersion.getVersionId()));
    assertEquals(2, sourcesWithBothVersions.size());
    assertEquals(
        Stream.of(sourceConnection1.getSourceId(), sourceConnection2.getSourceId()).sorted().toList(),
        sourcesWithBothVersions.stream().map(SourceConnection::getSourceId).sorted().toList());
  }

  @Test
  void testGetDestinationsWithVersions() throws IOException {
    final DestinationConnection destinationConnection1 = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withDestinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName(DESTINATION_NAME);
    configRepository.writeDestinationConnectionNoSecrets(destinationConnection1);

    final DestinationConnection destinationConnection2 = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withDestinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName(DESTINATION_NAME);
    configRepository.writeDestinationConnectionNoSecrets(destinationConnection2);

    final List<DestinationConnection> destinationConnections =
        configRepository.listDestinationsWithVersionIds(List.of(standardDestinationDefinition.getDefaultVersionId()));
    assertEquals(2, destinationConnections.size());
    assertEquals(
        Stream.of(destinationConnection1.getDestinationId(), destinationConnection2.getDestinationId()).sorted().toList(),
        destinationConnections.stream().map(DestinationConnection::getDestinationId).sorted().toList());

    final ActorDefinitionVersion newActorDefinitionVersion = configRepository.writeActorDefinitionVersion(MockData.actorDefinitionVersion()
        .withActorDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .withVersionId(UUID.randomUUID())
        .withDockerImageTag(UPGRADE_IMAGE_TAG));

    configRepository.setActorDefaultVersion(destinationConnection1.getDestinationId(), newActorDefinitionVersion.getVersionId());

    final List<DestinationConnection> destinationsWithNewVersion =
        configRepository.listDestinationsWithVersionIds(List.of(newActorDefinitionVersion.getVersionId()));
    assertEquals(1, destinationsWithNewVersion.size());
    assertEquals(destinationConnection1.getDestinationId(), destinationsWithNewVersion.get(0).getDestinationId());

    final List<DestinationConnection> destinationsWithOldVersion =
        configRepository.listDestinationsWithVersionIds(List.of(standardDestinationDefinition.getDefaultVersionId()));
    assertEquals(1, destinationsWithOldVersion.size());
    assertEquals(destinationConnection2.getDestinationId(), destinationsWithOldVersion.get(0).getDestinationId());

    final List<DestinationConnection> destinationsWithBothVersions = configRepository
        .listDestinationsWithVersionIds(List.of(standardDestinationDefinition.getDefaultVersionId(), newActorDefinitionVersion.getVersionId()));
    assertEquals(2, destinationsWithBothVersions.size());
    assertEquals(
        Stream.of(destinationConnection1.getDestinationId(), destinationConnection2.getDestinationId()).sorted().toList(),
        destinationsWithBothVersions.stream().map(DestinationConnection::getDestinationId).sorted().toList());
  }

}
