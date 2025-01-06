/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

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
import io.airbyte.data.services.ConnectionTimelineEventService;
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
  private final String DOCKER_IMAGE_TAG = "0.0.1";
  private Organization organization;
  private StandardWorkspace workspace;
  private StandardSourceDefinition sourceDefinition;
  private StandardDestinationDefinition destinationDefinition;
  private ActorDefinitionVersion sourceDefinitionVersion;
  private ActorDefinitionVersion destinationDefinitionVersion;
  private SourceConnection source;
  private DestinationConnection destination;

  public JooqTestDbSetupHelper() {
    this.featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    final ConnectionService connectionService = mock(ConnectionService.class);
    final ScopedConfigurationService scopedConfigurationService = mock(ScopedConfigurationService.class);
    final ConnectionTimelineEventService connectionTimelineEventService = mock(ConnectionTimelineEventService.class);

    when(featureFlagClient.stringVariation(eq(HeartbeatMaxSecondsBetweenMessages.INSTANCE), any(SourceDefinition.class))).thenReturn("3600");

    final ActorDefinitionService actorDefinitionService = new ActorDefinitionServiceJooqImpl(database);
    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater =
        new ActorDefinitionVersionUpdater(featureFlagClient, connectionService, actorDefinitionService, scopedConfigurationService,
            connectionTimelineEventService);
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
    sourceDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId(), DOCKER_IMAGE_TAG);
    createActorDefinition(sourceDefinition, sourceDefinitionVersion);

    // Create destination definition
    destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withName("Test destination def")
        .withTombstone(false);
    destinationDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId(), DOCKER_IMAGE_TAG);
    createActorDefinition(destinationDefinition, destinationDefinitionVersion);

    // Create actors
    source = createActorForActorDefinition(sourceDefinition);
    destination = createActorForActorDefinition(destinationDefinition);

    // Verify initial source version
    final UUID initialSourceDefinitionDefaultVersionId =
        sourceServiceJooqImpl.getStandardSourceDefinition(SOURCE_DEFINITION_ID).getDefaultVersionId();
    assertNotNull(initialSourceDefinitionDefaultVersionId);

    // Verify initial destination version
    final UUID initialDestinationDefinitionDefaultVersionId =
        destinationServiceJooqImpl.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID).getDefaultVersionId();
    assertNotNull(initialDestinationDefinitionDefaultVersionId);
  }

  public void setUpDependencies() throws IOException, JsonValidationException, ConfigNotFoundException {
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
    sourceDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId(), DOCKER_IMAGE_TAG);
    createActorDefinition(sourceDefinition, sourceDefinitionVersion);

    // Create destination definition
    destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withName("Test destination def")
        .withTombstone(false);
    destinationDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId(), DOCKER_IMAGE_TAG);
    createActorDefinition(destinationDefinition, destinationDefinitionVersion);

    // Create actors
    source = createActorForActorDefinition(sourceDefinition);
    destination = createActorForActorDefinition(destinationDefinition);

    // Verify initial source version
    final UUID initialSourceDefinitionDefaultVersionId =
        sourceServiceJooqImpl.getStandardSourceDefinition(SOURCE_DEFINITION_ID).getDefaultVersionId();
    assertNotNull(initialSourceDefinitionDefaultVersionId);

    // Verify initial destination version
    final UUID initialDestinationDefinitionDefaultVersionId =
        destinationServiceJooqImpl.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID).getDefaultVersionId();
    assertNotNull(initialDestinationDefinitionDefaultVersionId);
  }

  public void setupForGetActorDefinitionVersionByDockerRepositoryAndDockerImageTagTests(UUID sourceDefinitionId, String name, String version)
      throws IOException {
    // Add another version of the source definition
    sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(sourceDefinitionId)
        .withName(name)
        .withTombstone(false);
    sourceDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId(), version);
    sourceDefinitionVersion.withDockerRepository(name).withDockerImageTag(version);
    createActorDefinition(sourceDefinition, sourceDefinitionVersion);
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
        .withEmail("org@airbyte.io");
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

  private static ActorDefinitionVersion createBaseActorDefVersion(final UUID actorDefId, final String dockerImageTag) {
    return new ActorDefinitionVersion()
        .withActorDefinitionId(actorDefId)
        .withDockerRepository("destination-image-" + actorDefId)
        .withDockerImageTag(dockerImageTag)
        .withProtocolVersion("1.0.0")
        .withSupportLevel(SupportLevel.CERTIFIED)
        .withInternalSupportLevel(200L)
        .withSpec(new ConnectorSpecification()
            .withConnectionSpecification(Jsons.jsonNode(Map.of("key", "value1"))).withProtocolVersion("1.0.0"));
  }

  public SourceConnection getSource() {
    return source;
  }

  public DestinationConnection getDestination() {
    return destination;
  }

  public Organization getOrganization() {
    return organization;
  }

  public StandardWorkspace getWorkspace() {
    return workspace;
  }

  public StandardSourceDefinition getSourceDefinition() {
    return sourceDefinition;
  }

  public StandardDestinationDefinition getDestinationDefinition() {
    return destinationDefinition;
  }

  public ActorDefinitionVersion getSourceDefinitionVersion() {
    return sourceDefinitionVersion;
  }

  public ActorDefinitionVersion getDestinationDefinitionVersion() {
    return destinationDefinitionVersion;
  }

}
