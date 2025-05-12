/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.config.persistence.OrganizationPersistence.DEFAULT_ORGANIZATION_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DataplaneGroup;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Organization;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
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
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl;
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
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ActorDefinitionPersistenceTest extends BaseConfigDatabaseTest {

  private static final String TEST_DEFAULT_MAX_SECONDS = "3600";
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final String DOCKER_IMAGE_TAG = "0.0.1";
  private static final UUID dataplaneGroupId = UUID.randomUUID();

  private ActorDefinitionService actorDefinitionService;
  private SourceServiceJooqImpl sourceService;
  private DestinationServiceJooqImpl destinationService;
  private WorkspaceService workspaceService;
  private ConnectionService connectionService;
  private DataplaneGroupService dataplaneGroupService;

  @BeforeEach
  void setup() throws SQLException, IOException {
    truncateAllTables();

    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    when(featureFlagClient.stringVariation(eq(HeartbeatMaxSecondsBetweenMessages.INSTANCE), any(SourceDefinition.class)))
        .thenReturn(TEST_DEFAULT_MAX_SECONDS);

    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    final ScopedConfigurationService scopedConfigurationService = mock(ScopedConfigurationService.class);
    final ConnectionTimelineEventService connectionTimelineEventService = mock(ConnectionTimelineEventService.class);
    final MetricClient metricClient = mock(MetricClient.class);

    actorDefinitionService = new ActorDefinitionServiceJooqImpl(database);
    final OrganizationService organizationService = new OrganizationServiceJooqImpl(database);
    organizationService.writeOrganization(new Organization()
        .withOrganizationId(DEFAULT_ORGANIZATION_ID).withName("Test Organization").withEmail("test@test.com"));

    dataplaneGroupService = spy(new DataplaneGroupServiceTestJooqImpl(database));
    dataplaneGroupService.writeDataplaneGroup(new DataplaneGroup()
        .withId(dataplaneGroupId)
        .withOrganizationId(DEFAULT_ORGANIZATION_ID)
        .withName("test")
        .withEnabled(true)
        .withTombstone(false));

    connectionService = spy(new ConnectionServiceJooqImpl(database));

    sourceService = spy(
        new SourceServiceJooqImpl(
            database,
            featureFlagClient,
            secretPersistenceConfigService,
            connectionService,
            new ActorDefinitionVersionUpdater(
                featureFlagClient,
                connectionService,
                actorDefinitionService,
                scopedConfigurationService,
                connectionTimelineEventService),
            metricClient));
    destinationService = spy(
        new DestinationServiceJooqImpl(
            database,
            featureFlagClient,
            connectionService,
            new ActorDefinitionVersionUpdater(
                featureFlagClient,
                connectionService,
                actorDefinitionService,
                scopedConfigurationService,
                connectionTimelineEventService),
            metricClient));

    organizationService.writeOrganization(MockData.defaultOrganization());

    workspaceService = spy(
        new WorkspaceServiceJooqImpl(database, featureFlagClient, secretsRepositoryReader, secretsRepositoryWriter, secretPersistenceConfigService,
            metricClient));
  }

  @Test
  void testSourceDefinitionWithNullTombstone() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsSrcDef(createBaseSourceDef());
  }

  @Test
  void testSourceDefinitionWithTrueTombstone() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsSrcDef(createBaseSourceDef().withTombstone(true));
  }

  @Test
  void testSourceDefinitionWithFalseTombstone() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsSrcDef(createBaseSourceDef().withTombstone(false));
  }

  @Test
  void testSourceDefinitionDefaultMaxSeconds() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsSrcDefDefaultMaxSecondsBetweenMessages(createBaseSourceDefWithoutMaxSecondsBetweenMessages());
  }

  @Test
  void testSourceDefinitionMaxSecondsGreaterThenDefaultShouldReturnConfigured() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsSrcDef(
        createBaseSourceDefWithoutMaxSecondsBetweenMessages().withMaxSecondsBetweenMessages(Long.parseLong(TEST_DEFAULT_MAX_SECONDS) + 1));
  }

  @Test
  void testSourceDefinitionMaxSecondsLessThenDefaultShouldReturnDefault() throws JsonValidationException, ConfigNotFoundException, IOException {
    final var def = createBaseSourceDefWithoutMaxSecondsBetweenMessages().withMaxSecondsBetweenMessages(1L);
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(def.getSourceDefinitionId());
    sourceService.writeConnectorMetadata(def, actorDefinitionVersion, Collections.emptyList());
    final var exp =
        def.withDefaultVersionId(actorDefinitionVersion.getVersionId()).withMaxSecondsBetweenMessages(Long.parseLong(TEST_DEFAULT_MAX_SECONDS));
    assertEquals(exp, sourceService.getStandardSourceDefinition(def.getSourceDefinitionId()));
  }

  private void assertReturnsSrcDef(final StandardSourceDefinition srcDef) throws ConfigNotFoundException, IOException, JsonValidationException {
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(srcDef.getSourceDefinitionId());
    sourceService.writeConnectorMetadata(srcDef, actorDefinitionVersion, Collections.emptyList());
    assertEquals(srcDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
        sourceService.getStandardSourceDefinition(srcDef.getSourceDefinitionId()));
  }

  private void assertReturnsSrcDefDefaultMaxSecondsBetweenMessages(final StandardSourceDefinition srcDef)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(srcDef.getSourceDefinitionId());
    sourceService.writeConnectorMetadata(srcDef, actorDefinitionVersion, Collections.emptyList());
    assertEquals(
        srcDef.withDefaultVersionId(actorDefinitionVersion.getVersionId())
            .withMaxSecondsBetweenMessages(MockData.DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES),
        sourceService.getStandardSourceDefinition(srcDef.getSourceDefinitionId()));
  }

  @Test
  void testGetSourceDefinitionFromSource() throws JsonValidationException, IOException {
    final StandardWorkspace workspace = createBaseStandardWorkspace();
    final StandardSourceDefinition srcDef = createBaseSourceDef().withTombstone(false);
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(srcDef.getSourceDefinitionId());
    final SourceConnection source = createSource(srcDef.getSourceDefinitionId(), workspace.getWorkspaceId());
    workspaceService.writeStandardWorkspaceNoSecrets(workspace);
    sourceService.writeConnectorMetadata(srcDef, actorDefinitionVersion, Collections.emptyList());
    sourceService.writeSourceConnectionNoSecrets(source);

    assertEquals(srcDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
        sourceService.getSourceDefinitionFromSource(source.getSourceId()));
  }

  @Test
  void testGetSourceDefinitionsFromConnection() throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardWorkspace workspace = createBaseStandardWorkspace();
    final StandardDestinationDefinition destDef = createBaseDestDef().withTombstone(false);
    final ActorDefinitionVersion destActorDefinitionVersion = createBaseActorDefVersion(destDef.getDestinationDefinitionId());
    final DestinationConnection dest = createDest(destDef.getDestinationDefinitionId(), workspace.getWorkspaceId());
    final StandardSourceDefinition srcDef = createBaseSourceDef().withTombstone(false);
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(srcDef.getSourceDefinitionId());
    final SourceConnection source = createSource(srcDef.getSourceDefinitionId(), workspace.getWorkspaceId());
    workspaceService.writeStandardWorkspaceNoSecrets(workspace);
    sourceService.writeConnectorMetadata(srcDef, actorDefinitionVersion, Collections.emptyList());
    sourceService.writeSourceConnectionNoSecrets(source);
    destinationService.writeConnectorMetadata(destDef, destActorDefinitionVersion, Collections.emptyList());
    destinationService.writeDestinationConnectionNoSecrets(dest);

    final UUID connectionId = UUID.randomUUID();
    final StandardSync connection = new StandardSync()
        .withName("Test Sync")
        .withDestinationId(dest.getDestinationId())
        .withConnectionId(connectionId)
        .withSourceId(source.getSourceId())
        .withBreakingChange(false)
        .withDataplaneGroupId(dataplaneGroupId);

    connectionService.writeStandardSync(connection);

    assertEquals(srcDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
        sourceService.getSourceDefinitionFromConnection(connectionId));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 10})
  void testListStandardSourceDefsHandlesTombstoneSourceDefs(final int numSrcDefs) throws IOException {
    final List<StandardSourceDefinition> allSourceDefinitions = new ArrayList<>();
    final List<StandardSourceDefinition> notTombstoneSourceDefinitions = new ArrayList<>();
    for (int i = 0; i < numSrcDefs; i++) {
      final boolean isTombstone = i % 2 == 0; // every other is tombstone
      final StandardSourceDefinition sourceDefinition = createBaseSourceDef().withTombstone(isTombstone);
      final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());
      allSourceDefinitions.add(sourceDefinition);
      if (!isTombstone) {
        notTombstoneSourceDefinitions.add(sourceDefinition);
      }
      sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, Collections.emptyList());
      sourceDefinition.setDefaultVersionId(actorDefinitionVersion.getVersionId());
    }

    final List<StandardSourceDefinition> returnedSrcDefsWithoutTombstone = sourceService.listStandardSourceDefinitions(false);
    assertEquals(notTombstoneSourceDefinitions, returnedSrcDefsWithoutTombstone);

    final List<StandardSourceDefinition> returnedSrcDefsWithTombstone = sourceService.listStandardSourceDefinitions(true);
    assertEquals(allSourceDefinitions, returnedSrcDefsWithTombstone);
  }

  @Test
  void testDestinationDefinitionWithNullTombstone() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsDestDef(createBaseDestDef());
  }

  @Test
  void testDestinationDefinitionWithTrueTombstone() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsDestDef(createBaseDestDef().withTombstone(true));
  }

  @Test
  void testDestinationDefinitionWithFalseTombstone() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsDestDef(createBaseDestDef().withTombstone(false));
  }

  void assertReturnsDestDef(final StandardDestinationDefinition destDef) throws ConfigNotFoundException, IOException, JsonValidationException {
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(destDef.getDestinationDefinitionId());
    destinationService.writeConnectorMetadata(destDef, actorDefinitionVersion, Collections.emptyList());
    assertEquals(destDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
        destinationService.getStandardDestinationDefinition(destDef.getDestinationDefinitionId()));
  }

  @Test
  void testGetDestinationDefinitionFromDestination() throws JsonValidationException, IOException {
    final StandardWorkspace workspace = createBaseStandardWorkspace();
    final StandardDestinationDefinition destDef = createBaseDestDef().withTombstone(false);
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(destDef.getDestinationDefinitionId());
    final DestinationConnection dest = createDest(destDef.getDestinationDefinitionId(), workspace.getWorkspaceId());
    workspaceService.writeStandardWorkspaceNoSecrets(workspace);
    destinationService.writeConnectorMetadata(destDef, actorDefinitionVersion, Collections.emptyList());
    destinationService.writeDestinationConnectionNoSecrets(dest);

    assertEquals(destDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
        destinationService.getDestinationDefinitionFromDestination(dest.getDestinationId()));
  }

  @Test
  void testGetDestinationDefinitionsFromConnection() throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardWorkspace workspace = createBaseStandardWorkspace();
    final StandardDestinationDefinition destDef = createBaseDestDef().withTombstone(false);
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef().withTombstone(false);
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(destDef.getDestinationDefinitionId());
    final ActorDefinitionVersion sourceActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());
    final DestinationConnection dest = createDest(destDef.getDestinationDefinitionId(), workspace.getWorkspaceId());
    final SourceConnection source = createSource(sourceDefinition.getSourceDefinitionId(), workspace.getWorkspaceId());
    workspaceService.writeStandardWorkspaceNoSecrets(workspace);
    destinationService.writeConnectorMetadata(destDef, actorDefinitionVersion, Collections.emptyList());
    sourceService.writeConnectorMetadata(sourceDefinition, sourceActorDefinitionVersion, Collections.emptyList());
    destinationService.writeDestinationConnectionNoSecrets(dest);
    sourceService.writeSourceConnectionNoSecrets(source);

    final UUID connectionId = UUID.randomUUID();
    final StandardSync connection = new StandardSync()
        .withName("Test Sync")
        .withDestinationId(dest.getDestinationId())
        .withConnectionId(connectionId)
        .withSourceId(source.getSourceId())
        .withBreakingChange(false)
        .withDataplaneGroupId(dataplaneGroupId);

    connectionService.writeStandardSync(connection);

    assertEquals(destDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
        destinationService.getDestinationDefinitionFromConnection(connectionId));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 10})
  void testListStandardDestDefsHandlesTombstoneDestDefs(final int numDestinationDefinitions) throws IOException {
    final List<StandardDestinationDefinition> allDestinationDefinitions = new ArrayList<>();
    final List<StandardDestinationDefinition> notTombstoneDestinationDefinitions = new ArrayList<>();
    for (int i = 0; i < numDestinationDefinitions; i++) {
      final boolean isTombstone = i % 2 == 0; // every other is tombstone
      final StandardDestinationDefinition destinationDefinition = createBaseDestDef().withTombstone(isTombstone);
      final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());
      allDestinationDefinitions.add(destinationDefinition);
      if (!isTombstone) {
        notTombstoneDestinationDefinitions.add(destinationDefinition);
      }
      destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, Collections.emptyList());
      destinationDefinition.setDefaultVersionId(actorDefinitionVersion.getVersionId());
    }

    final List<StandardDestinationDefinition> returnedDestDefsWithoutTombstone = destinationService.listStandardDestinationDefinitions(false);
    assertEquals(notTombstoneDestinationDefinitions, returnedDestDefsWithoutTombstone);

    final List<StandardDestinationDefinition> returnedDestDefsWithTombstone = destinationService.listStandardDestinationDefinitions(true);
    assertEquals(allDestinationDefinitions, returnedDestDefsWithTombstone);
  }

  @Test
  void testUpdateDeclarativeActorDefinitionVersions()
      throws IOException, ConfigNotFoundException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final String declarativeDockerRepository = "airbyte/source-declarative-manifest";
    final String previousTag = "0.1.0";
    final String newTag = "0.2.0";
    final String differentMajorTag = "1.0.0";

    // Write multiple definitions to be updated and one to not be updated
    final StandardSourceDefinition sourceDef = createBaseSourceDef();
    final ActorDefinitionVersion adv = createBaseActorDefVersion(sourceDef.getSourceDefinitionId()).withDockerRepository(declarativeDockerRepository)
        .withDockerImageTag(previousTag);
    sourceService.writeConnectorMetadata(sourceDef, adv, Collections.emptyList());

    final StandardSourceDefinition sourceDef2 = createBaseSourceDef();
    final ActorDefinitionVersion adv2 = createBaseActorDefVersion(sourceDef2.getSourceDefinitionId())
        .withDockerRepository(declarativeDockerRepository).withDockerImageTag(previousTag);
    sourceService.writeConnectorMetadata(sourceDef2, adv2, Collections.emptyList());

    final StandardSourceDefinition sourceDef3 = createBaseSourceDef();
    final ActorDefinitionVersion adv3 = createBaseActorDefVersion(sourceDef3.getSourceDefinitionId())
        .withDockerRepository(declarativeDockerRepository).withDockerImageTag(differentMajorTag);
    sourceService.writeConnectorMetadata(sourceDef3, adv3, Collections.emptyList());

    final int numUpdated = actorDefinitionService.updateDeclarativeActorDefinitionVersions(previousTag, newTag);
    assertEquals(2, numUpdated);

    final StandardSourceDefinition updatedSourceDef = sourceService.getStandardSourceDefinition(sourceDef.getSourceDefinitionId());
    final StandardSourceDefinition updatedSourceDef2 = sourceService.getStandardSourceDefinition(sourceDef2.getSourceDefinitionId());
    final StandardSourceDefinition persistedSourceDef3 = sourceService.getStandardSourceDefinition(sourceDef3.getSourceDefinitionId());

    // Definitions that were on the previous tag should be updated to the new tag
    assertEquals(newTag, actorDefinitionService.getActorDefinitionVersion(updatedSourceDef.getDefaultVersionId()).getDockerImageTag());
    assertEquals(newTag, actorDefinitionService.getActorDefinitionVersion(updatedSourceDef2.getDefaultVersionId()).getDockerImageTag());
    // Definitions on a different version don't get updated
    assertEquals(differentMajorTag, actorDefinitionService.getActorDefinitionVersion(persistedSourceDef3.getDefaultVersionId()).getDockerImageTag());
  }

  @Test
  void getActorDefinitionIdsInUse() throws IOException, JsonValidationException {
    final StandardWorkspace workspace = createBaseStandardWorkspace();
    workspaceService.writeStandardWorkspaceNoSecrets(workspace);

    final StandardSourceDefinition sourceDefInUse = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion3 = createBaseActorDefVersion(sourceDefInUse.getSourceDefinitionId());
    sourceService.writeConnectorMetadata(sourceDefInUse, actorDefinitionVersion3, Collections.emptyList());
    final SourceConnection sourceConnection = createSource(sourceDefInUse.getSourceDefinitionId(), workspace.getWorkspaceId());
    sourceService.writeSourceConnectionNoSecrets(sourceConnection);

    final StandardSourceDefinition sourceDefNotInUse = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion4 = createBaseActorDefVersion(sourceDefNotInUse.getSourceDefinitionId());
    sourceService.writeConnectorMetadata(sourceDefNotInUse, actorDefinitionVersion4, Collections.emptyList());

    final StandardDestinationDefinition destDefInUse = createBaseDestDef();
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(destDefInUse.getDestinationDefinitionId());
    destinationService.writeConnectorMetadata(destDefInUse, actorDefinitionVersion, Collections.emptyList());
    final DestinationConnection destinationConnection = createDest(destDefInUse.getDestinationDefinitionId(), workspace.getWorkspaceId());
    destinationService.writeDestinationConnectionNoSecrets(destinationConnection);

    final StandardDestinationDefinition destDefNotInUse = createBaseDestDef();
    final ActorDefinitionVersion actorDefinitionVersion2 = createBaseActorDefVersion(destDefNotInUse.getDestinationDefinitionId());
    destinationService.writeConnectorMetadata(destDefNotInUse, actorDefinitionVersion2, Collections.emptyList());

    assertTrue(actorDefinitionService.getActorDefinitionIdsInUse().contains(sourceDefInUse.getSourceDefinitionId()));
    assertTrue(actorDefinitionService.getActorDefinitionIdsInUse().contains(destDefInUse.getDestinationDefinitionId()));
    assertFalse(actorDefinitionService.getActorDefinitionIdsInUse().contains(sourceDefNotInUse.getSourceDefinitionId()));
    assertFalse(actorDefinitionService.getActorDefinitionIdsInUse().contains(destDefNotInUse.getDestinationDefinitionId()));
  }

  @Test
  void testGetActorDefinitionIdsToDefaultVersionsMap() throws IOException {
    final StandardSourceDefinition sourceDef = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(sourceDef.getSourceDefinitionId());
    sourceService.writeConnectorMetadata(sourceDef, actorDefinitionVersion, Collections.emptyList());

    final StandardDestinationDefinition destDef = createBaseDestDef();
    final ActorDefinitionVersion actorDefinitionVersion2 = createBaseActorDefVersion(destDef.getDestinationDefinitionId());
    destinationService.writeConnectorMetadata(destDef, actorDefinitionVersion2, Collections.emptyList());

    final Map<UUID, ActorDefinitionVersion> actorDefIdToDefaultVersionId = actorDefinitionService.getActorDefinitionIdsToDefaultVersionsMap();
    assertEquals(actorDefIdToDefaultVersionId.size(), 2);
    assertEquals(actorDefIdToDefaultVersionId.get(sourceDef.getSourceDefinitionId()), actorDefinitionVersion);
    assertEquals(actorDefIdToDefaultVersionId.get(destDef.getDestinationDefinitionId()), actorDefinitionVersion2);
  }

  @Test
  void testUpdateStandardSourceDefinition() throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());

    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, Collections.emptyList());

    final StandardSourceDefinition sourceDefinitionFromDB =
        sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId());
    assertEquals(sourceDefinition.withDefaultVersionId(actorDefinitionVersion.getVersionId()), sourceDefinitionFromDB);

    final StandardSourceDefinition sourceDefinition2 = sourceDefinition
        .withName("new name")
        .withIcon("updated icon")
        .withTombstone(true);
    sourceService.updateStandardSourceDefinition(sourceDefinition2);

    final StandardSourceDefinition sourceDefinition2FromDB =
        sourceService.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId());

    // Default version has not changed
    assertEquals(sourceDefinition2FromDB.getDefaultVersionId(), sourceDefinitionFromDB.getDefaultVersionId());

    // Source definition has been updated
    assertEquals(sourceDefinition2.withDefaultVersionId(actorDefinitionVersion.getVersionId()), sourceDefinition2FromDB);
  }

  @Test
  void testUpdateNonexistentStandardSourceDefinitionThrows() {
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    assertThrows(ConfigNotFoundException.class, () -> sourceService.updateStandardSourceDefinition(sourceDefinition));
  }

  @Test
  void testUpdateStandardDestinationDefinition() throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardDestinationDefinition destinationDefinition = createBaseDestDef();
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());

    destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, Collections.emptyList());

    final StandardDestinationDefinition destinationDefinitionFromDB =
        destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId());
    assertEquals(destinationDefinition.withDefaultVersionId(actorDefinitionVersion.getVersionId()), destinationDefinitionFromDB);

    final StandardDestinationDefinition destinationDefinition2 = destinationDefinition
        .withName("new name")
        .withIcon("updated icon")
        .withTombstone(true);
    destinationService.updateStandardDestinationDefinition(destinationDefinition2);

    final StandardDestinationDefinition destinationDefinition2FromDB =
        destinationService.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId());

    // Default version has not changed
    assertEquals(destinationDefinition2FromDB.getDefaultVersionId(), destinationDefinitionFromDB.getDefaultVersionId());

    // Destination definition has been updated
    assertEquals(destinationDefinition2.withDefaultVersionId(actorDefinitionVersion.getVersionId()), destinationDefinition2FromDB);
  }

  @Test
  void testUpdateNonexistentStandardDestinationDefinitionThrows() {
    final StandardDestinationDefinition destinationDefinition = createBaseDestDef();
    assertThrows(ConfigNotFoundException.class, () -> destinationService.updateStandardDestinationDefinition(destinationDefinition));
  }

  @SuppressWarnings("SameParameterValue")
  private static SourceConnection createSource(final UUID sourceDefId, final UUID workspaceId) {
    return new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withSourceDefinitionId(sourceDefId)
        .withWorkspaceId(workspaceId)
        .withName("source");
  }

  @SuppressWarnings("SameParameterValue")
  private static DestinationConnection createDest(final UUID destDefId, final UUID workspaceId) {
    return new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withDestinationDefinitionId(destDefId)
        .withWorkspaceId(workspaceId)
        .withName("dest");
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
        .withVersionId(UUID.randomUUID())
        .withActorDefinitionId(actorDefId)
        .withDockerRepository("source-image-" + actorDefId)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withInternalSupportLevel(100L)
        .withProtocolVersion("0.2.0");
  }

  private static StandardSourceDefinition createBaseSourceDefWithoutMaxSecondsBetweenMessages() {
    final UUID id = UUID.randomUUID();

    return new StandardSourceDefinition()
        .withName("source-def-" + id)
        .withSourceDefinitionId(id)
        .withTombstone(false);
  }

  private static StandardDestinationDefinition createBaseDestDef() {
    final UUID id = UUID.randomUUID();

    return new StandardDestinationDefinition()
        .withName("source-def-" + id)
        .withDestinationDefinitionId(id)
        .withTombstone(false);
  }

  private static StandardWorkspace createBaseStandardWorkspace() {
    return new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("workspace-a")
        .withSlug("workspace-a-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withDataplaneGroupId(dataplaneGroupId)
        .withOrganizationId(DEFAULT_ORGANIZATION_ID);
  }

}
