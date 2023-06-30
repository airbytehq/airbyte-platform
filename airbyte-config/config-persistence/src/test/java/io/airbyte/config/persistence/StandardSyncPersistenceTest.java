/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION_OPERATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.NOTIFICATION_CONFIGURATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.SCHEMA_MANAGEMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionResourceRequirements;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.DestinationConnection;
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
import io.airbyte.db.instance.configs.jooq.generated.enums.AutoPropagationStatus;
import io.airbyte.db.instance.configs.jooq.generated.enums.NotificationType;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.NotificationConfigurationRecord;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.SchemaManagementRecord;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StandardSyncPersistenceTest extends BaseConfigDatabaseTest {

  record StandardSyncProtocolVersionFlag(UUID standardSyncId, boolean unsupportedProtocolVersion) {}

  private static final AirbyteProtocolVersionRange protocolRange_0_0 = new AirbyteProtocolVersionRange(new Version("0.0.0"), new Version("0.1.0"));
  private static final AirbyteProtocolVersionRange protocolRange_0_1 = new AirbyteProtocolVersionRange(new Version("0.0.1"), new Version("1.0.0"));
  private static final AirbyteProtocolVersionRange protocolRange_1_1 = new AirbyteProtocolVersionRange(new Version("1.0.0"), new Version("1.10.0"));

  private static final UUID workspaceId = UUID.randomUUID();

  private ConfigRepository configRepository;
  private StandardSyncPersistence standardSyncPersistence;

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

    // only used for creating records that sync depends on.
    configRepository = new ConfigRepository(database, MockData.MAX_SECONDS_BETWEEN_MESSAGE_SUPPLIER);
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
    standardSyncPersistence.writeStandardSync(sync1);
    standardSyncPersistence.writeStandardSync(sync2);

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
  void testClearUnsupportedProtocolVersionFlagFromSource() throws IOException, JsonValidationException, SQLException {
    createBaseObjects();

    final StandardSync sync1 = createStandardSync(source1, destination1);
    final StandardSync sync2 = createStandardSync(source1, destination2);
    final List<StandardSync> syncs = List.of(sync1, sync2);

    setProtocolVersionFlagForSyncs(List.of(
        new StandardSyncProtocolVersionFlag(sync1.getConnectionId(), true),
        new StandardSyncProtocolVersionFlag(sync2.getConnectionId(), true)));

    // Only sync1 should be flipped since sync2 has dest2 with protocol v1
    standardSyncPersistence.clearUnsupportedProtocolVersionFlag(sourceDef1.getSourceDefinitionId(), ActorType.SOURCE, protocolRange_0_0);
    assertEquals(Set.of(
        new StandardSyncProtocolVersionFlag(sync1.getConnectionId(), false),
        new StandardSyncProtocolVersionFlag(sync2.getConnectionId(), true)), getProtocolVersionFlagForSyncs(syncs));

    standardSyncPersistence.clearUnsupportedProtocolVersionFlag(sourceDef1.getSourceDefinitionId(), ActorType.SOURCE, protocolRange_0_1);
    assertEquals(Set.of(
        new StandardSyncProtocolVersionFlag(sync1.getConnectionId(), false),
        new StandardSyncProtocolVersionFlag(sync2.getConnectionId(), false)), getProtocolVersionFlagForSyncs(syncs));

    // Making sure we updated the updated_at timestamp
    final Optional<Pair<OffsetDateTime, OffsetDateTime>> datetimes = database.query(ctx -> ctx
        .select(CONNECTION.CREATED_AT, CONNECTION.UPDATED_AT).from(CONNECTION).where(CONNECTION.ID.eq(sync2.getConnectionId()))
        .stream().findFirst()
        .map(r -> new ImmutablePair<>(r.get(CONNECTION.CREATED_AT), r.get(CONNECTION.UPDATED_AT))));
    assertTrue(datetimes.isPresent());
    assertNotEquals(datetimes.get().getLeft(), datetimes.get().getRight());
  }

  @Test
  void testClearUnsupportedProtocolVersionFlagFromSourceMultiFlipAtOnce() throws IOException, JsonValidationException, SQLException {
    createBaseObjects();

    final StandardSync sync1 = createStandardSync(source1, destination1);
    final StandardSync sync2 = createStandardSync(source1, destination2);
    final List<StandardSync> syncs = List.of(sync1, sync2);

    setProtocolVersionFlagForSyncs(List.of(
        new StandardSyncProtocolVersionFlag(sync1.getConnectionId(), true),
        new StandardSyncProtocolVersionFlag(sync2.getConnectionId(), true)));

    // Making sure we flip all the connections if more than one is impacted
    standardSyncPersistence.clearUnsupportedProtocolVersionFlag(sourceDef1.getSourceDefinitionId(), ActorType.SOURCE, protocolRange_0_1);
    assertEquals(Set.of(
        new StandardSyncProtocolVersionFlag(sync1.getConnectionId(), false),
        new StandardSyncProtocolVersionFlag(sync2.getConnectionId(), false)), getProtocolVersionFlagForSyncs(syncs));
  }

  @Test
  void testClearUnsupportedProtocolVersionFlagFromDest() throws IOException, JsonValidationException, SQLException {
    createBaseObjects();

    final StandardSync sync1 = createStandardSync(source1, destination2);
    final StandardSync sync2 = createStandardSync(source2, destination2);
    final List<StandardSync> syncs = List.of(sync1, sync2);

    setProtocolVersionFlagForSyncs(List.of(
        new StandardSyncProtocolVersionFlag(sync1.getConnectionId(), true),
        new StandardSyncProtocolVersionFlag(sync2.getConnectionId(), true)));

    // destDef1 is not tied to anything, there should be no change
    standardSyncPersistence.clearUnsupportedProtocolVersionFlag(destDef1.getDestinationDefinitionId(), ActorType.DESTINATION, protocolRange_0_1);
    assertEquals(Set.of(
        new StandardSyncProtocolVersionFlag(sync1.getConnectionId(), true),
        new StandardSyncProtocolVersionFlag(sync2.getConnectionId(), true)), getProtocolVersionFlagForSyncs(syncs));

    // Only sync1 should be flipped since sync2 has source1 with protocol v0
    standardSyncPersistence.clearUnsupportedProtocolVersionFlag(destDef2.getDestinationDefinitionId(), ActorType.DESTINATION, protocolRange_1_1);
    assertEquals(Set.of(
        new StandardSyncProtocolVersionFlag(sync1.getConnectionId(), true),
        new StandardSyncProtocolVersionFlag(sync2.getConnectionId(), false)), getProtocolVersionFlagForSyncs(syncs));

    standardSyncPersistence.clearUnsupportedProtocolVersionFlag(destDef2.getDestinationDefinitionId(), ActorType.DESTINATION, protocolRange_0_1);
    assertEquals(Set.of(
        new StandardSyncProtocolVersionFlag(sync1.getConnectionId(), false),
        new StandardSyncProtocolVersionFlag(sync2.getConnectionId(), false)), getProtocolVersionFlagForSyncs(syncs));
  }

  @Test
  void testGetAllStreamsForConnection() throws Exception {
    createBaseObjects();
    final AirbyteStream airbyteStream = new AirbyteStream().withName("stream1").withNamespace("namespace1");
    final ConfiguredAirbyteStream configuredStream = new ConfiguredAirbyteStream().withStream(airbyteStream);
    final AirbyteStream airbyteStream2 = new AirbyteStream().withName("stream2");
    final ConfiguredAirbyteStream configuredStream2 = new ConfiguredAirbyteStream().withStream(airbyteStream2);
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
    assertFalse(configRepository.getConnectionHasAlphaOrBetaConnector(syncGa.getConnectionId()));

    final StandardSync syncAlpha = createStandardSync(sourceAlpha, destination1);
    standardSyncPersistence.writeStandardSync(syncAlpha);
    assertTrue(configRepository.getConnectionHasAlphaOrBetaConnector(syncAlpha.getConnectionId()));

    final StandardSync syncBeta = createStandardSync(source1, destinationBeta);
    standardSyncPersistence.writeStandardSync(syncBeta);
    assertTrue(configRepository.getConnectionHasAlphaOrBetaConnector(syncBeta.getConnectionId()));
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

    List<StandardSync> standardSyncs = configRepository.listStandardSyncsUsingOperation(operationId);
    assertEquals(1, standardSyncs.size());
    assertEquals(NonBreakingChangesPreference.PROPAGATE_COLUMNS, standardSyncs.get(0).getNonBreakingChangesPreference());

    standardSyncs = configRepository.listWorkspaceStandardSyncs(new ConfigRepository.StandardSyncQuery(
        workspaceId,
        null,
        null,
        true));
    assertEquals(1, standardSyncs.size());
    assertEquals(NonBreakingChangesPreference.PROPAGATE_COLUMNS, standardSyncs.get(0).getNonBreakingChangesPreference());

    standardSyncs = configRepository.listWorkspaceStandardSyncsPaginated(new ConfigRepository.StandardSyncsQueryPaginated(
        List.of(workspaceId),
        null,
        null,
        true,
        1000,
        0

    )).values().stream().findFirst().get();
    assertEquals(1, standardSyncs.size());
    assertEquals(NonBreakingChangesPreference.PROPAGATE_COLUMNS, standardSyncs.get(0).getNonBreakingChangesPreference());

    standardSyncs = configRepository.listConnectionsBySource(sync.getSourceId(), true);
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
            .equals(syncGa.getConnectionId())).map(record -> record.getNotificationType()).findFirst().get());
    assertFalse(notificationConfigurations.stream().filter(notificationConfigurationRecord -> notificationConfigurationRecord.getConnectionId()
        .equals(syncGa.getConnectionId())).map(record -> record.getEnabled()).findFirst().get());
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

  private Set<StandardSyncProtocolVersionFlag> getProtocolVersionFlagForSyncs(final List<StandardSync> standardSync) throws SQLException {
    return database.query(ctx -> ctx
        .select(CONNECTION.ID, CONNECTION.UNSUPPORTED_PROTOCOL_VERSION)
        .from(CONNECTION)
        .where(CONNECTION.ID.in(standardSync.stream().map(StandardSync::getConnectionId).toList()))
        .fetch())
        .stream()
        .map(r -> new StandardSyncProtocolVersionFlag(r.get(CONNECTION.ID), r.get(CONNECTION.UNSUPPORTED_PROTOCOL_VERSION)))
        .collect(Collectors.toSet());
  }

  private void setProtocolVersionFlagForSyncs(final List<StandardSyncProtocolVersionFlag> updates) throws SQLException {
    final List<UUID> setToTrue =
        updates.stream().filter(s -> s.unsupportedProtocolVersion).map(StandardSyncProtocolVersionFlag::standardSyncId).toList();
    final List<UUID> setToFalse =
        updates.stream().filter(s -> !s.unsupportedProtocolVersion).map(StandardSyncProtocolVersionFlag::standardSyncId).toList();
    database.query(ctx -> {
      if (!setToTrue.isEmpty()) {
        ctx.update(CONNECTION)
            .set(CONNECTION.UNSUPPORTED_PROTOCOL_VERSION, true)
            .where(CONNECTION.ID.in(setToTrue))
            .execute();
      }
      if (!setToFalse.isEmpty()) {
        ctx.update(CONNECTION)
            .set(CONNECTION.UNSUPPORTED_PROTOCOL_VERSION, false)
            .where(CONNECTION.ID.in(setToFalse))
            .execute();
      }
      return null;
    });
  }

  private void createBaseObjects() throws IOException, JsonValidationException {
    final StandardWorkspace workspace = new StandardWorkspace()
        .withWorkspaceId(workspaceId)
        .withName("Another Workspace")
        .withSlug("another-workspace")
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDefaultGeography(Geography.AUTO);
    configRepository.writeStandardWorkspaceNoSecrets(workspace);

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
      throws JsonValidationException, IOException {
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
        .withReleaseStage(releaseStage);
    configRepository.writeSourceDefinitionAndDefaultVersion(sourceDef, sourceDefVersion);
    return sourceDef;
  }

  private StandardDestinationDefinition createStandardDestDefinition(final String protocolVersion, final ReleaseStage releaseStage)
      throws JsonValidationException, IOException {
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
        .withReleaseStage(releaseStage);
    configRepository.writeDestinationDefinitionAndDefaultVersion(destDef, destDefVersion);
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
    configRepository.writeSourceConnectionNoSecrets(source);
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
    configRepository.writeDestinationConnectionNoSecrets(dest);
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
        .withNotifySchemaChanges(false)
        .withNotifySchemaChangesByEmail(false)
        .withBreakingChange(false);
    standardSyncPersistence.writeStandardSync(sync);
    return sync;
  }

  private SchemaManagementRecord getSchemaManagementByConnectionId(final UUID connectionId) throws IOException, SQLException {
    return database.query(ctx -> ctx.select(SCHEMA_MANAGEMENT.asterisk())
        .from(SCHEMA_MANAGEMENT)
        .where(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(connectionId))
        .fetchInto(SchemaManagementRecord.class)
        .stream().findFirst().get());
  }

  private UUID writeOperationForConnection(final UUID connectionId) throws SQLException, IOException {
    final UUID operationId = UUID.randomUUID();
    final UUID standardSyncOperationId = UUID.randomUUID();
    configRepository.writeStandardSyncOperation(new StandardSyncOperation()
        .withOperationId(standardSyncOperationId)
        .withName("name")
        .withWorkspaceId(workspaceId)
        .withOperatorType(StandardSyncOperation.OperatorType.DBT));

    database.transaction(ctx -> ctx.insertInto(CONNECTION_OPERATION)
        .set(CONNECTION_OPERATION.ID, operationId)
        .set(CONNECTION_OPERATION.CONNECTION_ID, connectionId)
        .set(CONNECTION_OPERATION.OPERATION_ID, standardSyncOperationId)
        .execute());

    return standardSyncOperationId;
  }

}
