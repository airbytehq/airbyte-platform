/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.config.persistence.OrganizationPersistence.DEFAULT_ORGANIZATION_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Geography;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.CatalogServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectorBuilderServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OAuthServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;
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

    final ConnectionService connectionService = mock(ConnectionService.class);
    final ScopedConfigurationService scopedConfigurationService = mock(ScopedConfigurationService.class);
    final ActorDefinitionService actorDefinitionService = new ActorDefinitionServiceJooqImpl(database);
    final OrganizationService organizationService = new OrganizationServiceJooqImpl(database);
    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater =
        new ActorDefinitionVersionUpdater(featureFlagClient, connectionService, actorDefinitionService, scopedConfigurationService);
    configRepository = spy(
        new ConfigRepository(
            new ActorDefinitionServiceJooqImpl(database),
            new CatalogServiceJooqImpl(database),
            connectionService,
            new ConnectorBuilderServiceJooqImpl(database),
            new DestinationServiceJooqImpl(database,
                featureFlagClient,
                secretsRepositoryReader,
                secretsRepositoryWriter,
                secretPersistenceConfigService,
                connectionService,
                actorDefinitionVersionUpdater),
            new OAuthServiceJooqImpl(database,
                featureFlagClient,
                secretsRepositoryReader,
                secretPersistenceConfigService),
            new OperationServiceJooqImpl(database),
            new SourceServiceJooqImpl(database,
                featureFlagClient,
                secretsRepositoryReader,
                secretsRepositoryWriter,
                secretPersistenceConfigService,
                connectionService,
                actorDefinitionVersionUpdater),
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
    organizationService.writeOrganization(MockData.defaultOrganization());
    configRepository.writeStandardWorkspaceNoSecrets(new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("default")
        .withSlug("workspace-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withDefaultGeography(Geography.US)
        .withOrganizationId(DEFAULT_ORGANIZATION_ID));
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

}
