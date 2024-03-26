/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Geography;
import io.airbyte.config.Organization;
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
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.TestClient;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;

public class JooqTestDbSetupHelper extends BaseConfigDatabaseTest {

  private final SourceServiceJooqImpl sourceServiceJooqImpl;
  private final DestinationServiceJooqImpl destinationServiceJooqImpl;
  private final WorkspaceServiceJooqImpl workspaceServiceJooqImpl;
  private final OrganizationServiceJooqImpl organizationServiceJooqImpl;
  private final TestClient featureFlagClient;
  private final UUID ORGANIZATION_ID = UUID.randomUUID();
  private final UUID WORKSPACE_ID = UUID.randomUUID();
  private final UUID SOURCE_DEFINITION_ID = UUID.randomUUID();
  private final UUID DESTINATION_DEFINITION_ID = UUID.randomUUID();
  @Getter
  private Organization organization;
  @Getter
  private StandardWorkspace workspace;
  @Getter
  private StandardSourceDefinition sourceDefinition;
  @Getter
  private StandardDestinationDefinition destinationDefinition;
  @Getter
  private ActorDefinitionVersion sourceDefinitionVersion;
  @Getter
  private ActorDefinitionVersion destinationDefinitionVersion;
  @Getter
  private SourceConnection source;
  @Getter
  private DestinationConnection destination;
  @Getter
  private UUID initialSourceDefaultVersionId;
  @Getter
  private UUID initialDestinationDefaultVersionId;

  public JooqTestDbSetupHelper() {
    this.featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    final ConnectionService connectionService = mock(ConnectionService.class);
    final ScopedConfigurationService scopedConfigurationService = mock(ScopedConfigurationService.class);

    when(featureFlagClient.stringVariation(eq(HeartbeatMaxSecondsBetweenMessages.INSTANCE), any(SourceDefinition.class))).thenReturn("3600");

    final ActorDefinitionService actorDefinitionService = new ActorDefinitionServiceJooqImpl(database);
    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater =
        new ActorDefinitionVersionUpdater(featureFlagClient, connectionService, actorDefinitionService, scopedConfigurationService);
    this.destinationServiceJooqImpl = new DestinationServiceJooqImpl(database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater);
    this.sourceServiceJooqImpl = new SourceServiceJooqImpl(database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater);
    this.workspaceServiceJooqImpl = new WorkspaceServiceJooqImpl(database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService);
    this.organizationServiceJooqImpl = new OrganizationServiceJooqImpl(database);
  }

  public void setupForVersionUpgradeTest() throws IOException, JsonValidationException, ConfigNotFoundException {
    // Create org
    organization = createBaseOrganization();
    organizationServiceJooqImpl.writeOrganization(organization);

    // Create workspace
    workspace = createBaseWorkspace();
    workspaceServiceJooqImpl.writeStandardWorkspaceNoSecrets(createBaseWorkspace());

    // Create source definition
    sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(SOURCE_DEFINITION_ID)
        .withName("Test source def")
        .withTombstone(false);
    sourceDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());
    createActorDefinition(sourceDefinition, sourceDefinitionVersion);

    // Create destination definition
    destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withName("Test destination def")
        .withTombstone(false);
    destinationDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());
    createActorDefinition(destinationDefinition, destinationDefinitionVersion);

    // Create actors
    source = createActorForActorDefinition(sourceDefinition);
    destination = createActorForActorDefinition(destinationDefinition);

    // Verify and store initial source versions
    final UUID initialSourceDefinitionDefaultVersionId =
        sourceServiceJooqImpl.getStandardSourceDefinition(SOURCE_DEFINITION_ID).getDefaultVersionId();
    initialSourceDefaultVersionId =
        sourceServiceJooqImpl.getSourceConnection(source.getSourceId()).getDefaultVersionId();
    assertNotNull(initialSourceDefinitionDefaultVersionId);
    assertEquals(initialSourceDefinitionDefaultVersionId, initialSourceDefaultVersionId);

    // Verify and store initial destination versions
    final UUID initialDestinationDefinitionDefaultVersionId =
        destinationServiceJooqImpl.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID).getDefaultVersionId();
    initialDestinationDefaultVersionId =
        destinationServiceJooqImpl.getDestinationConnection(destination.getDestinationId()).getDefaultVersionId();
    assertNotNull(initialDestinationDefinitionDefaultVersionId);
    assertEquals(initialDestinationDefinitionDefaultVersionId, initialDestinationDefaultVersionId);
  }

  public void createActorDefinition(final StandardSourceDefinition sourceDefinition, final ActorDefinitionVersion actorDefinitionVersion)
      throws IOException {
    sourceServiceJooqImpl.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, List.of());
  }

  public void createActorDefinition(final StandardDestinationDefinition destinationDefinition, final ActorDefinitionVersion actorDefinitionVersion)
      throws IOException {
    destinationServiceJooqImpl.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, List.of());
  }

  public SourceConnection createActorForActorDefinition(final StandardSourceDefinition sourceDefinition) throws IOException {
    final SourceConnection source = createBaseSourceActor().withSourceDefinitionId(sourceDefinition.getSourceDefinitionId());
    sourceServiceJooqImpl.writeSourceConnectionNoSecrets(source);
    return source;
  }

  public DestinationConnection createActorForActorDefinition(final StandardDestinationDefinition destinationDefinition) throws IOException {
    final DestinationConnection destination =
        createBaseDestinationActor().withDestinationDefinitionId(destinationDefinition.getDestinationDefinitionId());
    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(destination);
    return destination;
  }

  private DestinationConnection createBaseDestinationActor() {
    return new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(WORKSPACE_ID)
        .withName("destination");
  }

  private SourceConnection createBaseSourceActor() {
    return new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(WORKSPACE_ID)
        .withName("source");
  }

  private Organization createBaseOrganization() {
    return new Organization()
        .withOrganizationId(ORGANIZATION_ID)
        .withName("organization")
        .withEmail("org@airbyte.io")
        .withPba(false)
        .withOrgLevelBilling(false);
  }

  private StandardWorkspace createBaseWorkspace() {
    return new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withOrganizationId(ORGANIZATION_ID)
        .withName("default")
        .withSlug("workspace-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withDefaultGeography(Geography.US);
  }

  private static ActorDefinitionVersion createBaseActorDefVersion(final UUID actorDefId) {
    return new ActorDefinitionVersion()
        .withActorDefinitionId(actorDefId)
        .withDockerRepository("destination-image-" + actorDefId)
        .withDockerImageTag("0.0.1")
        .withProtocolVersion("1.0.0")
        .withSupportLevel(SupportLevel.CERTIFIED)
        .withSpec(new ConnectorSpecification()
            .withConnectionSpecification(Jsons.jsonNode(Map.of("key", "value1"))).withProtocolVersion("1.0.0"));
  }

}
