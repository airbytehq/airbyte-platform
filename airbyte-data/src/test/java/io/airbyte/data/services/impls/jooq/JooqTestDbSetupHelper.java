/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.TAG;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DataplaneGroup;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Organization;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.Tag;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.DataplaneGroupService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.TagRecord;
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.MetricClient;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.jooq.impl.DSL;

public class JooqTestDbSetupHelper extends BaseConfigDatabaseTest {

  private final SourceServiceJooqImpl sourceServiceJooqImpl;
  private final DestinationServiceJooqImpl destinationServiceJooqImpl;
  private final WorkspaceServiceJooqImpl workspaceServiceJooqImpl;
  private final OrganizationServiceJooqImpl organizationServiceJooqImpl;
  private final DataplaneGroupService dataplaneGroupServiceDataImpl;
  private final TestClient featureFlagClient;
  private final UUID ORGANIZATION_ID = UUID.randomUUID();
  private final UUID WORKSPACE_ID = UUID.randomUUID();
  private final UUID SOURCE_DEFINITION_ID = UUID.randomUUID();
  private final UUID DESTINATION_DEFINITION_ID = UUID.randomUUID();
  private final String DOCKER_IMAGE_TAG = "0.0.1";
  private final UUID DATAPLANE_GROUP_ID = UUID.randomUUID();
  private Organization organization;
  private StandardWorkspace workspace;
  private StandardSourceDefinition sourceDefinition;
  private StandardDestinationDefinition destinationDefinition;
  private ActorDefinitionVersion sourceDefinitionVersion;
  private ActorDefinitionVersion destinationDefinitionVersion;
  private SourceConnection source;
  private DestinationConnection destination;
  private List<Tag> tags;
  private List<Tag> tagsFromAnotherWorkspace;

  public JooqTestDbSetupHelper() {
    this.featureFlagClient = mock(TestClient.class);
    final MetricClient metricClient = mock(MetricClient.class);
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
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient);
    this.sourceServiceJooqImpl = new SourceServiceJooqImpl(database,
        featureFlagClient,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient);
    this.dataplaneGroupServiceDataImpl = new DataplaneGroupServiceTestJooqImpl(database);
    this.workspaceServiceJooqImpl = new WorkspaceServiceJooqImpl(database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient);
    this.organizationServiceJooqImpl = new OrganizationServiceJooqImpl(database);
  }

  public void setupForVersionUpgradeTest() throws IOException, JsonValidationException, ConfigNotFoundException {
    // Create org
    organization = createBaseOrganization();
    organizationServiceJooqImpl.writeOrganization(organization);

    // Create dataplane group
    dataplaneGroupServiceDataImpl.writeDataplaneGroup(createBaseDataplaneGroup());

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

  public void setUpDependencies() throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    // Create org
    organization = createBaseOrganization();
    organizationServiceJooqImpl.writeOrganization(organization);

    // Create dataplane group
    dataplaneGroupServiceDataImpl.writeDataplaneGroup(createBaseDataplaneGroup());

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

    // Create connection tags
    tags = createTags(workspace.getWorkspaceId());
    final StandardWorkspace secondWorkspace = createSecondWorkspace();
    workspaceServiceJooqImpl.writeStandardWorkspaceNoSecrets(secondWorkspace);
    tagsFromAnotherWorkspace = createTags(secondWorkspace.getWorkspaceId());
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

  // It's kind of ugly and brittle to create the tags in this way, but since TagService is a micronaut
  // data service, we cannot instantiate it here and use it to create the tags
  public List<Tag> createTags(final UUID workspaceId) throws SQLException {
    final Tag tagOne = new Tag().withName("tag_one").withTagId(UUID.randomUUID()).withWorkspaceId(workspaceId).withColor("111111");
    final Tag tagTwo = new Tag().withName("tag_two").withTagId(UUID.randomUUID()).withWorkspaceId(workspaceId).withColor("222222");
    final Tag tagThree = new Tag().withName("tag_three").withTagId(UUID.randomUUID()).withWorkspaceId(workspaceId).withColor("333333");
    final List<Tag> tags = List.of(tagOne, tagTwo, tagThree);

    database.query(ctx -> {
      final List<TagRecord> records = tags.stream()
          .map(tag -> {
            final TagRecord record = DSL.using(ctx.configuration()).newRecord(TAG);
            record.setId(tag.getTagId());
            record.setWorkspaceId(workspaceId);
            record.setName(tag.getName());
            record.setColor(tag.getColor());
            return record;
          })
          .collect(Collectors.toList());

      return ctx.batchInsert(records).execute();
    });

    return tags;
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

  private DataplaneGroup createBaseDataplaneGroup() {
    return new DataplaneGroup()
        .withId(DATAPLANE_GROUP_ID)
        .withOrganizationId(ORGANIZATION_ID)
        .withName("test")
        .withEnabled(true)
        .withTombstone(false);
  }

  private StandardWorkspace createBaseWorkspace() {
    return new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withOrganizationId(ORGANIZATION_ID)
        .withName("default")
        .withSlug("workspace-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID);
  }

  private StandardWorkspace createSecondWorkspace() {
    return new StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withOrganizationId(ORGANIZATION_ID)
        .withName("second")
        .withSlug("second-workspace-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID);
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

  public List<Tag> getTags() {
    return tags;
  }

  public List<Tag> getTagsFromAnotherWorkspace() {
    return tagsFromAnotherWorkspace;
  }

}
