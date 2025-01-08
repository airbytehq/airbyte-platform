/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.config.persistence.OrganizationPersistence.DEFAULT_ORGANIZATION_ID;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION_OPERATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.NOTIFICATION_CONFIGURATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.SCHEMA_MANAGEMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionResourceRequirements;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.Geography;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.NonBreakingChangesPreference;
import io.airbyte.config.StandardSync.Status;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.SyncMode;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.OperationService;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.data.services.shared.StandardSyncQuery;
import io.airbyte.data.services.shared.StandardSyncsQueryPaginated;
import io.airbyte.db.instance.configs.jooq.generated.enums.AutoPropagationStatus;
import io.airbyte.db.instance.configs.jooq.generated.enums.NotificationType;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.NotificationConfigurationRecord;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.SchemaManagementRecord;
import io.airbyte.featureflag.TestClient;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StandardSyncPersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID workspaceId = UUID.randomUUID();

  private StandardSyncPersistence standardSyncPersistence;

  private ConnectionService connectionService;
  private SourceService sourceService;
  private DestinationService destinationService;
  private WorkspaceService workspaceService;
  private OperationService operationService;

  private StandardSourceDefinition sourceDef1;
  private StandardSourceDefinition sourceDefAlpha;
  private SourceConnection source1;
  private SourceConnection source2;
  private SourceConnection sourceAlpha;
  private StandardDestinationDefinition destDef1;
  private StandardDestinationDefinition destDef2;
  private StandardDestinationDefinition destDefBeta;
  private DestinationConnection destination1;
  private DestinationConnection destination2;
  private DestinationConnection destinationBeta;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();

    standardSyncPersistence = new StandardSyncPersistence(database);

    final var featureFlagClient = mock(TestClient.class);
    final var secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final var secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final var secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    final var connectionTimelineEventService = mock(ConnectionTimelineEventService.class);
    connectionService = new ConnectionServiceJooqImpl(database);
    final var actorDefinitionVersionUpdater = new ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService,
        new ActorDefinitionServiceJooqImpl(database),
        mock(ScopedConfigurationService.class),
        connectionTimelineEventService);
    sourceService = new SourceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater);
    destinationService = new DestinationServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater);
    workspaceService = new WorkspaceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService);
    operationService = new OperationServiceJooqImpl(database);

    final OrganizationService organizationService = new OrganizationServiceJooqImpl(database);
    organizationService.writeOrganization(MockData.defaultOrganization());
  }

  @Test
  void testReadWrite() throws IOException, ConfigNotFoundException, JsonValidationException, SQLException {
    createBaseObjects();
    final StandardSync sync = createStandardSync(source1, destination1);
    standardSyncPersistence.writeStandardSync(sync);

    final StandardSync expectedSync = Jsons.clone(sync);
    assertEquals(expectedSync, standardSyncPersistence.getStandardSync(sync.getConnectionId()));

    final SchemaManagementRecord schemaManagementRecord = getSchemaManagementByConnectionId(sync.getConnectionId());
    assertEquals(NonBreakingChangesPreference.IGNORE.value(), schemaManagementRecord.getAutoPropagationStatus().getLiteral());
  }

  @Test
  void testReadNotExists() {
    assertThrows(ConfigNotFoundException.class, () -> standardSyncPersistence.getStandardSync(UUID.randomUUID()));
  }

  @Test
  void testList() throws IOException, JsonValidationException {
    createBaseObjects();
    final StandardSync sync1 = createStandardSync(source1, destination1);
    final StandardSync sync2 = createStandardSync(source1, destination2);

    final List<StandardSync> expected = List.of(
        Jsons.clone(sync1)
            .withNonBreakingChangesPreference(NonBreakingChangesPreference.IGNORE),
        Jsons.clone(sync2)
            .withNonBreakingChangesPreference(NonBreakingChangesPreference.IGNORE));

    assertEquals(expected, standardSyncPersistence.listStandardSync());
  }

  @Test
  void testDelete() throws IOException, ConfigNotFoundException, JsonValidationException {
    createBaseObjects();

    final StandardSync sync1 = createStandardSync(source1, destination1);
    final StandardSync sync2 = createStandardSync(source1, destination2);

    assertNotNull(standardSyncPersistence.getStandardSync(sync1.getConnectionId()));
    assertNotNull(standardSyncPersistence.getStandardSync(sync2.getConnectionId()));

    standardSyncPersistence.deleteStandardSync(sync1.getConnectionId());

    assertThrows(ConfigNotFoundException.class, () -> standardSyncPersistence.getStandardSync(sync1.getConnectionId()));
    assertNotNull(standardSyncPersistence.getStandardSync(sync2.getConnectionId()));
  }

  @Test
  void testGetAllStreamsForConnection() throws Exception {
    createBaseObjects();
    final AirbyteStream airbyteStream = new AirbyteStream("stream1", Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL)).withNamespace("namespace1");
    final ConfiguredAirbyteStream configuredStream = new ConfiguredAirbyteStream(airbyteStream, SyncMode.INCREMENTAL, DestinationSyncMode.APPEND);
    final AirbyteStream airbyteStream2 = new AirbyteStream("stream2", Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL));
    final ConfiguredAirbyteStream configuredStream2 = new ConfiguredAirbyteStream(airbyteStream2, SyncMode.INCREMENTAL, DestinationSyncMode.APPEND);
    final ConfiguredAirbyteCatalog configuredCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(configuredStream, configuredStream2));
    final StandardSync sync = createStandardSync(source1, destination1).withCatalog(configuredCatalog);

    standardSyncPersistence.writeStandardSync(sync);

    final List<StreamDescriptor> result = standardSyncPersistence.getAllStreamsForConnection(sync.getConnectionId());
    assertEquals(2, result.size());

    assertTrue(
        result.stream().anyMatch(
            streamDescriptor -> "stream1".equals(streamDescriptor.getName()) && "namespace1".equals(streamDescriptor.getNamespace())));
    assertTrue(
        result.stream().anyMatch(
            streamDescriptor -> "stream2".equals(streamDescriptor.getName()) && streamDescriptor.getNamespace() == null));
  }

  @Test
  void testConnectionHasAlphaOrBetaConnector() throws JsonValidationException, IOException {
    createBaseObjects();

    final StandardSync syncGa = createStandardSync(source1, destination1);
    standardSyncPersistence.writeStandardSync(syncGa);
    assertFalse(connectionService.getConnectionHasAlphaOrBetaConnector(syncGa.getConnectionId()));

    final StandardSync syncAlpha = createStandardSync(sourceAlpha, destination1);
    standardSyncPersistence.writeStandardSync(syncAlpha);
    assertTrue(connectionService.getConnectionHasAlphaOrBetaConnector(syncAlpha.getConnectionId()));

    final StandardSync syncBeta = createStandardSync(source1, destinationBeta);
    standardSyncPersistence.writeStandardSync(syncBeta);
    assertTrue(connectionService.getConnectionHasAlphaOrBetaConnector(syncBeta.getConnectionId()));
  }

  @Test
  void testWriteNotificationConfigurationIfNeeded() throws JsonValidationException, IOException, SQLException {
    createBaseObjects();

    final StandardSync syncGa = createStandardSync(source1, destination1);
    syncGa.setNotifySchemaChangesByEmail(true);
    standardSyncPersistence.writeStandardSync(syncGa);

    List<NotificationConfigurationRecord> notificationConfigurations = getNotificationConfigurations();
    assertEquals(1, notificationConfigurations.size());
    assertEquals(syncGa.getConnectionId(), notificationConfigurations.get(0).getConnectionId());
    assertEquals(NotificationType.email, notificationConfigurations.get(0).getNotificationType());
    assertEquals(true, notificationConfigurations.get(0).getEnabled());

    syncGa.setNotifySchemaChanges(true);
    standardSyncPersistence.writeStandardSync(syncGa);
    notificationConfigurations = getNotificationConfigurations();

    assertEquals(2, notificationConfigurations.size());
  }

  @Test
  void testCreateSchemaManagementIfNeeded() throws JsonValidationException, IOException, SQLException {
    createBaseObjects();

    final StandardSync sync = createStandardSync(source1, destination1);
    standardSyncPersistence.writeStandardSync(sync);

    final SchemaManagementRecord schemaManagementRecord = getSchemaManagementByConnectionId(sync.getConnectionId());
    assertEquals(NonBreakingChangesPreference.IGNORE.value(), schemaManagementRecord.getAutoPropagationStatus().getLiteral());

    sync.setNonBreakingChangesPreference(NonBreakingChangesPreference.PROPAGATE_FULLY);
    standardSyncPersistence.writeStandardSync(sync);

    final SchemaManagementRecord schemaManagementRecordAfterUpdate = getSchemaManagementByConnectionId(sync.getConnectionId());

    assertEquals(schemaManagementRecord.getId(), schemaManagementRecordAfterUpdate.getId());
    assertEquals(NonBreakingChangesPreference.PROPAGATE_FULLY.value(), schemaManagementRecordAfterUpdate.getAutoPropagationStatus().getLiteral());
  }

  @Test
  void testCreateSchemaManagementAccessor() throws JsonValidationException, IOException, SQLException {
    createBaseObjects();

    final StandardSync sync = createStandardSync(source1, destination1);
    sync.setNonBreakingChangesPreference(NonBreakingChangesPreference.PROPAGATE_COLUMNS);
    standardSyncPersistence.writeStandardSync(sync);
    final UUID operationId = writeOperationForConnection(sync.getConnectionId());

    List<StandardSync> standardSyncs = connectionService.listStandardSyncsUsingOperation(operationId);
    assertEquals(1, standardSyncs.size());
    assertEquals(NonBreakingChangesPreference.PROPAGATE_COLUMNS, standardSyncs.get(0).getNonBreakingChangesPreference());

    standardSyncs = connectionService.listWorkspaceStandardSyncs(new StandardSyncQuery(
        workspaceId,
        null,
        null,
        true));
    assertEquals(1, standardSyncs.size());
    assertEquals(NonBreakingChangesPreference.PROPAGATE_COLUMNS, standardSyncs.get(0).getNonBreakingChangesPreference());

    standardSyncs = connectionService.listWorkspaceStandardSyncsPaginated(new StandardSyncsQueryPaginated(
        List.of(workspaceId),
        null,
        null,
        true,
        1000,
        0

    )).values().stream().findFirst().get();
    assertEquals(1, standardSyncs.size());
    assertEquals(NonBreakingChangesPreference.PROPAGATE_COLUMNS, standardSyncs.get(0).getNonBreakingChangesPreference());

    standardSyncs = connectionService.listConnectionsBySource(sync.getSourceId(), true);
    assertEquals(1, standardSyncs.size());
    assertEquals(NonBreakingChangesPreference.PROPAGATE_COLUMNS, standardSyncs.get(0).getNonBreakingChangesPreference());
  }

  @Test
  void testDontUpdateIfNotNeeded() throws JsonValidationException, IOException, SQLException {
    createBaseObjects();

    final StandardSync syncGa = createStandardSync(source1, destination1);
    syncGa.setNotifySchemaChangesByEmail(true);
    standardSyncPersistence.writeStandardSync(syncGa);
    final StandardSync syncGa2 = createStandardSync(source2, destination2);
    syncGa2.setNotifySchemaChangesByEmail(true);
    standardSyncPersistence.writeStandardSync(syncGa2);

    syncGa.setNotifySchemaChangesByEmail(false);
    standardSyncPersistence.writeStandardSync(syncGa);
    final List<NotificationConfigurationRecord> notificationConfigurations = getNotificationConfigurations();

    assertEquals(2, notificationConfigurations.size());
    assertEquals(NotificationType.email,
        notificationConfigurations.stream().filter(notificationConfigurationRecord -> notificationConfigurationRecord.getConnectionId()
            .equals(syncGa.getConnectionId())).map(NotificationConfigurationRecord::getNotificationType).findFirst().get());
    assertFalse(notificationConfigurations.stream().filter(notificationConfigurationRecord -> notificationConfigurationRecord.getConnectionId()
        .equals(syncGa.getConnectionId())).map(NotificationConfigurationRecord::getEnabled).findFirst().get());
  }

  private List<NotificationConfigurationRecord> getNotificationConfigurations() throws SQLException {
    return database.query(ctx -> ctx.selectFrom(NOTIFICATION_CONFIGURATION).fetch());
  }

  @Test
  void testGetNotificationEnable() {
    final StandardSync standardSync = new StandardSync();
    assertFalse(StandardSyncPersistence.getNotificationEnabled(standardSync, NotificationType.webhook));
    assertFalse(StandardSyncPersistence.getNotificationEnabled(standardSync, NotificationType.email));

    standardSync.setNotifySchemaChanges(true);
    assertTrue(StandardSyncPersistence.getNotificationEnabled(standardSync, NotificationType.webhook));

    standardSync.setNotifySchemaChanges(false);
    standardSync.setNotifySchemaChangesByEmail(true);
    assertTrue(StandardSyncPersistence.getNotificationEnabled(standardSync, NotificationType.email));
  }

  @Test
  void testEnumValues() {
    assertEquals(NonBreakingChangesPreference.IGNORE.value(), AutoPropagationStatus.ignore.getLiteral());
    assertEquals(NonBreakingChangesPreference.DISABLE.value(), AutoPropagationStatus.disable.getLiteral());
    assertEquals(NonBreakingChangesPreference.PROPAGATE_COLUMNS.value(), AutoPropagationStatus.propagate_columns.getLiteral());
    assertEquals(NonBreakingChangesPreference.PROPAGATE_FULLY.value(), AutoPropagationStatus.propagate_fully.getLiteral());
  }

  @Test
  void testListConnectionsByActorDefinitionIdAndType() throws IOException, JsonValidationException {
    createBaseObjects();
    final var expectedSync = createStandardSync(source1, destination1);
    final List<StandardSync> actualSyncs = connectionService.listConnectionsByActorDefinitionIdAndType(
        destination1.getDestinationDefinitionId(),
        ActorType.DESTINATION.value(), false, false);
    assertThat(actualSyncs.size()).isEqualTo(1);
    assertThat(actualSyncs.get(0)).isEqualTo(expectedSync);
  }

  @Test
  void testListWorkspaceActiveSyncIds() throws JsonValidationException, IOException {
    createBaseObjects();

    final StandardSync sync1 = createStandardSync(source1, destination1);
    standardSyncPersistence.writeStandardSync(sync1);

    final StandardSync sync1Disabled = createStandardSync(source1, destination1);
    standardSyncPersistence.writeStandardSync(sync1Disabled.withStatus(Status.INACTIVE));

    final StandardSync sync1Deprecated = createStandardSync(source1, destination1);
    standardSyncPersistence.writeStandardSync(sync1Deprecated.withStatus(Status.DEPRECATED));

    final StandardSync sync2 = createStandardSync(source2, destination2);
    standardSyncPersistence.writeStandardSync(sync2);

    final StandardSyncQuery syncQueryBySource = new StandardSyncQuery(workspaceId, List.of(source1.getSourceId()), null, false);
    final List<UUID> activeSyncsForSource1 = workspaceService.listWorkspaceActiveSyncIds(syncQueryBySource);
    assertEquals(activeSyncsForSource1.size(), 1);
    assertEquals(activeSyncsForSource1.get(0), sync1.getConnectionId());

    final StandardSyncQuery syncQueryByDestination = new StandardSyncQuery(workspaceId, null, List.of(destination1.getDestinationId()), false);
    final List<UUID> activeSyncsForDestination1 = workspaceService.listWorkspaceActiveSyncIds(syncQueryByDestination);
    assertEquals(activeSyncsForDestination1.size(), 1);
    assertEquals(activeSyncsForDestination1.get(0), sync1.getConnectionId());
  }

  @Test
  void testDisableConnectionsById() throws IOException, JsonValidationException, ConfigNotFoundException {
    createBaseObjects();

    final StandardSync sync1 = createStandardSync(source1, destination1);
    standardSyncPersistence.writeStandardSync(sync1);

    final StandardSync sync2 = createStandardSync(source2, destination2);
    standardSyncPersistence.writeStandardSync(sync2);

    final StandardSync sync3 = createStandardSync(source2, destination2);
    standardSyncPersistence.writeStandardSync(sync3.withStatus(Status.INACTIVE));

    final StandardSync sync4 = createStandardSync(source2, destination2);
    standardSyncPersistence.writeStandardSync(sync4.withStatus(Status.DEPRECATED));

    assertEquals(Status.ACTIVE, sync1.getStatus());
    assertEquals(Status.ACTIVE, sync2.getStatus());
    assertEquals(Status.INACTIVE, sync3.getStatus());
    assertEquals(Status.DEPRECATED, sync4.getStatus());

    connectionService.disableConnectionsById(List.of(sync1.getConnectionId(), sync2.getConnectionId(), sync3.getConnectionId()));

    final StandardSync updatedSync1 = standardSyncPersistence.getStandardSync(sync1.getConnectionId());
    final StandardSync updatedSync2 = standardSyncPersistence.getStandardSync(sync2.getConnectionId());
    final StandardSync updatedSync3 = standardSyncPersistence.getStandardSync(sync3.getConnectionId());

    assertEquals(Status.INACTIVE, updatedSync1.getStatus());
    assertEquals(Status.INACTIVE, updatedSync2.getStatus());
    assertEquals(Status.INACTIVE, updatedSync3.getStatus());
    assertEquals(Status.DEPRECATED, sync4.getStatus());
  }

  private void createBaseObjects() throws IOException, JsonValidationException {
    final StandardWorkspace workspace = new StandardWorkspace()
        .withWorkspaceId(workspaceId)
        .withName("Another Workspace")
        .withSlug("another-workspace")
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDefaultGeography(Geography.AUTO)
        .withOrganizationId(DEFAULT_ORGANIZATION_ID);
    workspaceService.writeStandardWorkspaceNoSecrets(workspace);

    sourceDef1 = createStandardSourceDefinition("0.2.2", ReleaseStage.GENERALLY_AVAILABLE);
    source1 = createSourceConnection(workspaceId, sourceDef1);

    final StandardSourceDefinition sourceDef2 = createStandardSourceDefinition("1.1.0", ReleaseStage.GENERALLY_AVAILABLE);
    source2 = createSourceConnection(workspaceId, sourceDef2);

    sourceDefAlpha = createStandardSourceDefinition("1.0.0", ReleaseStage.ALPHA);
    sourceAlpha = createSourceConnection(workspaceId, sourceDefAlpha);

    destDef1 = createStandardDestDefinition("0.2.3", ReleaseStage.GENERALLY_AVAILABLE);
    destination1 = createDestinationConnection(workspaceId, destDef1);

    destDef2 = createStandardDestDefinition("1.3.0", ReleaseStage.GENERALLY_AVAILABLE);
    destination2 = createDestinationConnection(workspaceId, destDef2);

    destDefBeta = createStandardDestDefinition("1.3.0", ReleaseStage.BETA);
    destinationBeta = createDestinationConnection(workspaceId, destDefBeta);
  }

  private StandardSourceDefinition createStandardSourceDefinition(final String protocolVersion, final ReleaseStage releaseStage)
      throws IOException {
    final UUID sourceDefId = UUID.randomUUID();
    final StandardSourceDefinition sourceDef = new StandardSourceDefinition()
        .withSourceDefinitionId(sourceDefId)
        .withSourceType(SourceType.API)
        .withName("random-source-" + sourceDefId)
        .withIcon("icon-1")
        .withTombstone(false)
        .withPublic(true)
        .withCustom(false)
        .withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(new ResourceRequirements().withCpuRequest("2")));
    final ActorDefinitionVersion sourceDefVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(sourceDefId)
        .withDockerImageTag("tag-1")
        .withDockerRepository("repository-1")
        .withDocumentationUrl("documentation-url-1")
        .withSpec(new ConnectorSpecification())
        .withProtocolVersion(protocolVersion)
        .withReleaseStage(releaseStage)
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withInternalSupportLevel(100L);
    sourceService.writeConnectorMetadata(sourceDef, sourceDefVersion, Collections.emptyList());
    return sourceDef;
  }

  private StandardDestinationDefinition createStandardDestDefinition(final String protocolVersion, final ReleaseStage releaseStage)
      throws IOException {
    final UUID destDefId = UUID.randomUUID();
    final StandardDestinationDefinition destDef = new StandardDestinationDefinition()
        .withDestinationDefinitionId(destDefId)
        .withName("random-destination-" + destDefId)
        .withIcon("icon-3")
        .withTombstone(false)
        .withPublic(true)
        .withCustom(false)
        .withResourceRequirements(new ActorDefinitionResourceRequirements().withDefault(new ResourceRequirements().withCpuRequest("2")));
    final ActorDefinitionVersion destDefVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(destDefId)
        .withDockerImageTag("tag-3")
        .withDockerRepository("repository-3")
        .withDocumentationUrl("documentation-url-3")
        .withSpec(new ConnectorSpecification())
        .withProtocolVersion(protocolVersion)
        .withReleaseStage(releaseStage)
        .withSupportLevel(SupportLevel.COMMUNITY)
        .withInternalSupportLevel(100L);

    destinationService.writeConnectorMetadata(destDef, destDefVersion, Collections.emptyList());
    return destDef;
  }

  private SourceConnection createSourceConnection(final UUID workspaceId, final StandardSourceDefinition sourceDef) throws IOException {
    final UUID sourceId = UUID.randomUUID();
    final SourceConnection source = new SourceConnection()
        .withName("source-" + sourceId)
        .withTombstone(false)
        .withConfiguration(Jsons.deserialize("{}"))
        .withSourceDefinitionId(sourceDef.getSourceDefinitionId())
        .withWorkspaceId(workspaceId)
        .withSourceId(sourceId);
    sourceService.writeSourceConnectionNoSecrets(source);
    return source;
  }

  private DestinationConnection createDestinationConnection(final UUID workspaceId, final StandardDestinationDefinition destDef)
      throws IOException {
    final UUID destinationId = UUID.randomUUID();
    final DestinationConnection dest = new DestinationConnection()
        .withName("source-" + destinationId)
        .withTombstone(false)
        .withConfiguration(Jsons.deserialize("{}"))
        .withDestinationDefinitionId(destDef.getDestinationDefinitionId())
        .withWorkspaceId(workspaceId)
        .withDestinationId(destinationId);
    destinationService.writeDestinationConnectionNoSecrets(dest);
    return dest;
  }

  private StandardSync createStandardSync(final SourceConnection source, final DestinationConnection dest) throws IOException {
    final UUID connectionId = UUID.randomUUID();
    final StandardSync sync = new StandardSync()
        .withConnectionId(connectionId)
        .withSourceId(source.getSourceId())
        .withDestinationId(dest.getDestinationId())
        .withName("standard-sync-" + connectionId)
        .withManual(true)
        .withNamespaceDefinition(NamespaceDefinitionType.CUSTOMFORMAT)
        .withNamespaceFormat("")
        .withPrefix("")
        .withStatus(Status.ACTIVE)
        .withGeography(Geography.AUTO)
        .withNonBreakingChangesPreference(NonBreakingChangesPreference.IGNORE)
        .withCreatedAt(OffsetDateTime.now().toEpochSecond())
        .withBackfillPreference(StandardSync.BackfillPreference.DISABLED)
        .withNotifySchemaChanges(false)
        .withNotifySchemaChangesByEmail(false)
        .withBreakingChange(false);
    standardSyncPersistence.writeStandardSync(sync);
    return sync;
  }

  private SchemaManagementRecord getSchemaManagementByConnectionId(final UUID connectionId) throws SQLException {
    return database.query(ctx -> ctx.select(SCHEMA_MANAGEMENT.asterisk())
        .from(SCHEMA_MANAGEMENT)
        .where(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(connectionId))
        .fetchInto(SchemaManagementRecord.class)
        .stream().findFirst().get());
  }

  private UUID writeOperationForConnection(final UUID connectionId) throws SQLException, IOException {
    final UUID operationId = UUID.randomUUID();
    final UUID standardSyncOperationId = UUID.randomUUID();
    operationService.writeStandardSyncOperation(new StandardSyncOperation()
        .withOperationId(standardSyncOperationId)
        .withName("name")
        .withWorkspaceId(workspaceId)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK));

    database.transaction(ctx -> ctx.insertInto(CONNECTION_OPERATION)
        .set(CONNECTION_OPERATION.ID, operationId)
        .set(CONNECTION_OPERATION.CONNECTION_ID, connectionId)
        .set(CONNECTION_OPERATION.OPERATION_ID, standardSyncOperationId)
        .execute());

    return standardSyncOperationId;
  }

}
