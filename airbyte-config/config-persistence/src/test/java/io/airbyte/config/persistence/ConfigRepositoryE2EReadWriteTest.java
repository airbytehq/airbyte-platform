/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_CATALOG;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_CATALOG_FETCH_EVENT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_WORKSPACE_GRANT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION_OPERATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.impl.DSL.select;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorCatalog;
import io.airbyte.config.ActorCatalogFetchEvent;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.DestinationOAuthParameter;
import io.airbyte.config.Geography;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSourceDefinition.SourceType;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.CatalogService;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.OAuthService;
import io.airbyte.data.services.OperationService;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.CatalogServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OAuthServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.data.services.shared.DestinationAndDefinition;
import io.airbyte.data.services.shared.SourceAndDefinition;
import io.airbyte.data.services.shared.StandardSyncQuery;
import io.airbyte.db.Database;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.TestClient;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The tests in this class should be moved into separate test suites grouped by resource. Do NOT add
 * new tests here. Add them to resource based test suites (e.g. WorkspacePersistenceTest). If one
 * does not exist yet for that resource yet, create one and follow the pattern.
 */
@Deprecated
@SuppressWarnings({"PMD.CyclomaticComplexity", "PMD.NPathComplexity"})
class ConfigRepositoryE2EReadWriteTest extends BaseConfigDatabaseTest {

  private static final String DOCKER_IMAGE_TAG = "1.2.0";
  private static final String CONFIG_HASH = "ConfigHash";

  private CatalogService catalogService;
  private OAuthService oauthService;
  private ActorDefinitionService actorDefinitionService;
  private ConnectionService connectionService;
  private SourceService sourceService;
  private DestinationService destinationService;
  private WorkspaceService workspaceService;
  private OperationService operationService;

  @BeforeEach
  void setup() throws IOException, JsonValidationException, SQLException {
    truncateAllTables();

    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    when(featureFlagClient.stringVariation(eq(HeartbeatMaxSecondsBetweenMessages.INSTANCE), any(SourceDefinition.class))).thenReturn("3600");

    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    final ScopedConfigurationService scopedConfigurationService = mock(ScopedConfigurationService.class);
    final ConnectionTimelineEventService connectionTimelineEventService = mock(ConnectionTimelineEventService.class);

    connectionService = spy(new ConnectionServiceJooqImpl(database));
    actorDefinitionService = spy(new ActorDefinitionServiceJooqImpl(database));
    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater = new ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService,
        actorDefinitionService,
        scopedConfigurationService,
        connectionTimelineEventService);
    catalogService = spy(new CatalogServiceJooqImpl(database));
    OrganizationService organizationService = new OrganizationServiceJooqImpl(database);
    organizationService.writeOrganization(MockData.defaultOrganization());
    oauthService = spy(new OAuthServiceJooqImpl(database,
        featureFlagClient,
        secretsRepositoryReader,
        secretPersistenceConfigService));
    sourceService = spy(new SourceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater));
    destinationService = spy(new DestinationServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater));
    workspaceService = spy(new WorkspaceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService));
    operationService = spy(new OperationServiceJooqImpl(database));

    for (final StandardWorkspace workspace : MockData.standardWorkspaces()) {
      workspaceService.writeStandardWorkspaceNoSecrets(workspace);
    }
    for (final StandardSourceDefinition sourceDefinition : MockData.standardSourceDefinitions()) {
      final ActorDefinitionVersion actorDefinitionVersion = MockData.actorDefinitionVersion()
          .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
          .withVersionId(sourceDefinition.getDefaultVersionId());
      sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, Collections.emptyList());
    }
    for (final StandardDestinationDefinition destinationDefinition : MockData.standardDestinationDefinitions()) {
      final ActorDefinitionVersion actorDefinitionVersion = MockData.actorDefinitionVersion()
          .withActorDefinitionId(destinationDefinition.getDestinationDefinitionId())
          .withVersionId(destinationDefinition.getDefaultVersionId());
      destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, Collections.emptyList());
    }
    for (final SourceConnection source : MockData.sourceConnections()) {
      sourceService.writeSourceConnectionNoSecrets(source);
    }
    for (final DestinationConnection destination : MockData.destinationConnections()) {
      destinationService.writeDestinationConnectionNoSecrets(destination);
    }
    for (final StandardSyncOperation operation : MockData.standardSyncOperations()) {
      operationService.writeStandardSyncOperation(operation);
    }
    for (final StandardSync sync : MockData.standardSyncs()) {
      connectionService.writeStandardSync(sync);
    }

    for (final SourceOAuthParameter oAuthParameter : MockData.sourceOauthParameters()) {
      oauthService.writeSourceOAuthParam(oAuthParameter);
    }
    for (final DestinationOAuthParameter oAuthParameter : MockData.destinationOauthParameters()) {
      oauthService.writeDestinationOAuthParam(oAuthParameter);
    }

    database.transaction(ctx -> ctx.truncate(ACTOR_DEFINITION_WORKSPACE_GRANT).execute());
  }

  @Test
  void testWorkspaceCountConnections() throws IOException {
    final UUID workspaceId = MockData.standardWorkspaces().get(0).getWorkspaceId();
    assertEquals(3, workspaceService.countConnectionsForWorkspace(workspaceId));
    assertEquals(2, workspaceService.countDestinationsForWorkspace(workspaceId));
    assertEquals(2, workspaceService.countSourcesForWorkspace(workspaceId));
  }

  @Test
  void testWorkspaceCountConnectionsDeprecated() throws IOException {
    final UUID workspaceId = MockData.standardWorkspaces().get(1).getWorkspaceId();
    assertEquals(1, workspaceService.countConnectionsForWorkspace(workspaceId));
  }

  @Test
  void testFetchActorsUsingDefinition() throws IOException {
    final UUID destinationDefinitionId = MockData.publicDestinationDefinition().getDestinationDefinitionId();
    final UUID sourceDefinitionId = MockData.publicSourceDefinition().getSourceDefinitionId();
    final List<DestinationConnection> destinationConnections = destinationService.listDestinationsForDefinition(
        destinationDefinitionId);
    final List<SourceConnection> sourceConnections = sourceService.listSourcesForDefinition(
        sourceDefinitionId);

    final List<DestinationConnection> nullCreatedAtDestinationConnections = destinationConnections.stream()
        .map(destinationConnection -> destinationConnection.withCreatedAt(null)).toList();

    final List<SourceConnection> nullCreatedAtSourceConnections = sourceConnections.stream()
        .map(sourceConnection -> sourceConnection.withCreatedAt(null)).toList();

    assertThat(nullCreatedAtDestinationConnections)
        .containsExactlyElementsOf(MockData.destinationConnections().stream().filter(d -> d.getDestinationDefinitionId().equals(
            destinationDefinitionId) && !d.getTombstone()).collect(Collectors.toList()));
    assertThat(nullCreatedAtSourceConnections)
        .containsExactlyElementsOf(MockData.sourceConnections().stream().filter(d -> d.getSourceDefinitionId().equals(
            sourceDefinitionId) && !d.getTombstone()).collect(Collectors.toList()));
  }

  @Test
  void testReadActorCatalog() throws IOException {
    final String otherConfigHash = "OtherConfigHash";
    final StandardWorkspace workspace = MockData.standardWorkspaces().get(0);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withSourceType(SourceType.DATABASE)
        .withName("sourceDefinition");
    final ActorDefinitionVersion actorDefinitionVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersionId(sourceDefinition.getDefaultVersionId());
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, Collections.emptyList());

    final SourceConnection source = new SourceConnection()
        .withSourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withSourceId(UUID.randomUUID())
        .withName("SomeConnector")
        .withWorkspaceId(workspace.getWorkspaceId())
        .withConfiguration(Jsons.deserialize("{}"));
    sourceService.writeSourceConnectionNoSecrets(source);

    final AirbyteCatalog firstCatalog = CatalogHelpers.createAirbyteCatalog("product",
        Field.of("label", JsonSchemaType.STRING), Field.of("size", JsonSchemaType.NUMBER),
        Field.of("color", JsonSchemaType.STRING), Field.of("price", JsonSchemaType.NUMBER));
    catalogService.writeActorCatalogFetchEvent(firstCatalog, source.getSourceId(), DOCKER_IMAGE_TAG, CONFIG_HASH);

    final AirbyteCatalog secondCatalog = CatalogHelpers.createAirbyteCatalog("product",
        Field.of("size", JsonSchemaType.NUMBER), Field.of("label", JsonSchemaType.STRING),
        Field.of("color", JsonSchemaType.STRING), Field.of("price", JsonSchemaType.NUMBER));
    catalogService.writeActorCatalogFetchEvent(secondCatalog, source.getSourceId(), DOCKER_IMAGE_TAG, otherConfigHash);

    final String expectedCatalog =
        "{"
            + "\"streams\":["
            + "{"
            + "\"name\":\"product\","
            + "\"json_schema\":{"
            + "\"type\":\"object\","
            + "\"properties\":{"
            + "\"size\":{\"type\":\"number\"},"
            + "\"color\":{\"type\":\"string\"},"
            + "\"price\":{\"type\":\"number\"},"
            + "\"label\":{\"type\":\"string\"}"
            + "}"
            + "},"
            + "\"supported_sync_modes\":[\"full_refresh\"],"
            + "\"default_cursor_field\":[],"
            + "\"source_defined_primary_key\":[]"
            + "}"
            + "]"
            + "}";

    final Optional<ActorCatalog> catalogResult = catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, CONFIG_HASH);
    assertTrue(catalogResult.isPresent());
    assertEquals(expectedCatalog, Jsons.serialize(catalogResult.get().getCatalog()));
  }

  @Test
  void testWriteCanonicalHashActorCatalog() throws IOException, JsonValidationException, SQLException {
    final String canonicalConfigHash = "8ad32981";
    final StandardWorkspace workspace = MockData.standardWorkspaces().get(0);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withSourceType(SourceType.DATABASE)
        .withName("sourceDefinition");
    final ActorDefinitionVersion actorDefinitionVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersionId(sourceDefinition.getDefaultVersionId());
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, Collections.emptyList());

    final SourceConnection source = new SourceConnection()
        .withSourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withSourceId(UUID.randomUUID())
        .withName("SomeConnector")
        .withWorkspaceId(workspace.getWorkspaceId())
        .withConfiguration(Jsons.deserialize("{}"));
    sourceService.writeSourceConnectionNoSecrets(source);

    final AirbyteCatalog firstCatalog = CatalogHelpers.createAirbyteCatalog("product",
        Field.of("label", JsonSchemaType.STRING), Field.of("size", JsonSchemaType.NUMBER),
        Field.of("color", JsonSchemaType.STRING), Field.of("price", JsonSchemaType.NUMBER));
    catalogService.writeActorCatalogFetchEvent(firstCatalog, source.getSourceId(), DOCKER_IMAGE_TAG, CONFIG_HASH);

    final String expectedCatalog =
        "{"
            + "\"streams\":["
            + "{"
            + "\"default_cursor_field\":[],"
            + "\"json_schema\":{"
            + "\"properties\":{"
            + "\"color\":{\"type\":\"string\"},"
            + "\"label\":{\"type\":\"string\"},"
            + "\"price\":{\"type\":\"number\"},"
            + "\"size\":{\"type\":\"number\"}"
            + "},"
            + "\"type\":\"object\""
            + "},"
            + "\"name\":\"product\","
            + "\"source_defined_primary_key\":[],"
            + "\"supported_sync_modes\":[\"full_refresh\"]"
            + "}"
            + "]"
            + "}";

    final Optional<ActorCatalog> catalogResult = catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, CONFIG_HASH);
    assertTrue(catalogResult.isPresent());
    assertEquals(catalogResult.get().getCatalogHash(), canonicalConfigHash);
    assertEquals(expectedCatalog, Jsons.canonicalJsonSerialize(catalogResult.get().getCatalog()));
  }

  @Test
  void testSimpleInsertActorCatalog() throws IOException, SQLException {
    final String otherConfigHash = "OtherConfigHash";
    final StandardWorkspace workspace = MockData.standardWorkspaces().get(0);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withSourceType(SourceType.DATABASE)
        .withName("sourceDefinition");
    final ActorDefinitionVersion actorDefinitionVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersionId(sourceDefinition.getDefaultVersionId());
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, Collections.emptyList());

    final SourceConnection source = new SourceConnection()
        .withSourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withSourceId(UUID.randomUUID())
        .withName("SomeConnector")
        .withWorkspaceId(workspace.getWorkspaceId())
        .withConfiguration(Jsons.deserialize("{}"));
    sourceService.writeSourceConnectionNoSecrets(source);

    final AirbyteCatalog actorCatalog = CatalogHelpers.createAirbyteCatalog("clothes", Field.of("name", JsonSchemaType.STRING));
    final AirbyteCatalog expectedActorCatalog = CatalogHelpers.createAirbyteCatalog("clothes", Field.of("name", JsonSchemaType.STRING));
    catalogService.writeActorCatalogFetchEvent(
        actorCatalog, source.getSourceId(), DOCKER_IMAGE_TAG, CONFIG_HASH);

    final Optional<ActorCatalog> catalog =
        catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, CONFIG_HASH);
    assertTrue(catalog.isPresent());
    assertEquals(expectedActorCatalog, Jsons.object(catalog.get().getCatalog(), AirbyteCatalog.class));
    assertFalse(catalogService.getActorCatalog(source.getSourceId(), "1.3.0", CONFIG_HASH).isPresent());
    assertFalse(catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, otherConfigHash).isPresent());

    catalogService.writeActorCatalogFetchEvent(actorCatalog, source.getSourceId(), "1.3.0", CONFIG_HASH);
    final Optional<ActorCatalog> catalogNewConnectorVersion =
        catalogService.getActorCatalog(source.getSourceId(), "1.3.0", CONFIG_HASH);
    assertTrue(catalogNewConnectorVersion.isPresent());
    assertEquals(expectedActorCatalog, Jsons.object(catalogNewConnectorVersion.get().getCatalog(), AirbyteCatalog.class));

    catalogService.writeActorCatalogFetchEvent(actorCatalog, source.getSourceId(), "1.2.0", otherConfigHash);
    final Optional<ActorCatalog> catalogNewConfig =
        catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, otherConfigHash);
    assertTrue(catalogNewConfig.isPresent());
    assertEquals(expectedActorCatalog, Jsons.object(catalogNewConfig.get().getCatalog(), AirbyteCatalog.class));

    final int catalogDbEntry = database.query(ctx -> ctx.selectCount().from(ACTOR_CATALOG)).fetchOne().into(int.class);
    assertEquals(1, catalogDbEntry);

    // Writing the previous catalog with v1 data types
    catalogService.writeActorCatalogFetchEvent(expectedActorCatalog, source.getSourceId(), "1.2.0", otherConfigHash);
    final Optional<ActorCatalog> catalogV1NewConfig =
        catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, otherConfigHash);
    assertTrue(catalogV1NewConfig.isPresent());
    assertEquals(expectedActorCatalog, Jsons.object(catalogNewConfig.get().getCatalog(), AirbyteCatalog.class));

    catalogService.writeActorCatalogFetchEvent(expectedActorCatalog, source.getSourceId(), "1.4.0", otherConfigHash);
    final Optional<ActorCatalog> catalogV1again =
        catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, otherConfigHash);
    assertTrue(catalogV1again.isPresent());
    assertEquals(expectedActorCatalog, Jsons.object(catalogNewConfig.get().getCatalog(), AirbyteCatalog.class));

    final int catalogDbEntry2 = database.query(ctx -> ctx.selectCount().from(ACTOR_CATALOG)).fetchOne().into(int.class);
    // TODO this should be 2 once we re-enable datatypes v1
    assertEquals(1, catalogDbEntry2);
  }

  @Test
  void testListWorkspaceStandardSyncAll() throws IOException {
    final List<StandardSync> expectedSyncs = copyWithV1Types(MockData.standardSyncs().subList(0, 4));
    final List<StandardSync> actualSyncs = connectionService.listWorkspaceStandardSyncs(
        MockData.standardWorkspaces().get(0).getWorkspaceId(), true);

    assertSyncsMatch(expectedSyncs, actualSyncs);
  }

  @Test
  void testListWorkspaceStandardSyncWithAllFiltering() throws IOException {
    final UUID workspaceId = MockData.standardWorkspaces().get(0).getWorkspaceId();
    final StandardSyncQuery query = new StandardSyncQuery(workspaceId, List.of(MockData.SOURCE_ID_1), List.of(MockData.DESTINATION_ID_1), false);
    final List<StandardSync> expectedSyncs = copyWithV1Types(
        MockData.standardSyncs().subList(0, 3).stream()
            .filter(sync -> query.destinationId().contains(sync.getDestinationId()))
            .filter(sync -> query.sourceId().contains(sync.getSourceId()))
            .toList());
    final List<StandardSync> actualSyncs = connectionService.listWorkspaceStandardSyncs(query);

    assertSyncsMatch(expectedSyncs, actualSyncs);
  }

  @Test
  void testListWorkspaceStandardSyncDestinationFiltering() throws IOException {
    final UUID workspaceId = MockData.standardWorkspaces().get(0).getWorkspaceId();
    final StandardSyncQuery query = new StandardSyncQuery(workspaceId, null, List.of(MockData.DESTINATION_ID_1), false);
    final List<StandardSync> expectedSyncs = copyWithV1Types(
        MockData.standardSyncs().subList(0, 3).stream()
            .filter(sync -> query.destinationId().contains(sync.getDestinationId()))
            .toList());
    final List<StandardSync> actualSyncs = connectionService.listWorkspaceStandardSyncs(query);

    assertSyncsMatch(expectedSyncs, actualSyncs);
  }

  @Test
  void testListWorkspaceStandardSyncSourceFiltering() throws IOException {
    final UUID workspaceId = MockData.standardWorkspaces().get(0).getWorkspaceId();
    final StandardSyncQuery query = new StandardSyncQuery(workspaceId, List.of(MockData.SOURCE_ID_2), null, false);
    final List<StandardSync> expectedSyncs = copyWithV1Types(
        MockData.standardSyncs().subList(0, 3).stream()
            .filter(sync -> query.sourceId().contains(sync.getSourceId()))
            .toList());
    final List<StandardSync> actualSyncs = connectionService.listWorkspaceStandardSyncs(query);

    assertSyncsMatch(expectedSyncs, actualSyncs);
  }

  @Test
  void testListWorkspaceStandardSyncExcludeDeleted() throws IOException {
    final List<StandardSync> expectedSyncs = copyWithV1Types(MockData.standardSyncs().subList(0, 3));
    final List<StandardSync> actualSyncs = connectionService.listWorkspaceStandardSyncs(MockData.standardWorkspaces().get(0).getWorkspaceId(), false);

    assertSyncsMatch(expectedSyncs, actualSyncs);
  }

  @Test
  void testGetWorkspaceBySlug() throws IOException {
    final StandardWorkspace workspace = MockData.standardWorkspaces().get(0);
    final StandardWorkspace tombstonedWorkspace = MockData.standardWorkspaces().get(2);
    final Optional<StandardWorkspace> retrievedWorkspace = workspaceService.getWorkspaceBySlugOptional(workspace.getSlug(), false);
    final Optional<StandardWorkspace> retrievedTombstonedWorkspaceNoTombstone =
        workspaceService.getWorkspaceBySlugOptional(tombstonedWorkspace.getSlug(), false);
    final Optional<StandardWorkspace> retrievedTombstonedWorkspace = workspaceService.getWorkspaceBySlugOptional(tombstonedWorkspace.getSlug(), true);

    assertTrue(retrievedWorkspace.isPresent());

    assertThat(retrievedWorkspace.get())
        .usingRecursiveComparison()
        .ignoringFields("createdAt", "updatedAt")
        .isEqualTo(workspace);

    assertFalse(retrievedTombstonedWorkspaceNoTombstone.isPresent());
    assertTrue(retrievedTombstonedWorkspace.isPresent());

    assertThat(retrievedTombstonedWorkspace.get())
        .usingRecursiveComparison()
        .ignoringFields("createdAt", "updatedAt")
        .isEqualTo(tombstonedWorkspace);
  }

  @Test
  void testUpdateConnectionOperationIds() throws Exception {
    final StandardSync sync = MockData.standardSyncs().get(0);
    final List<UUID> existingOperationIds = sync.getOperationIds();
    final UUID connectionId = sync.getConnectionId();

    // this test only works as intended when there are multiple operationIds
    assertTrue(existingOperationIds.size() > 1);

    // first, remove all associated operations
    Set<UUID> expectedOperationIds = Collections.emptySet();
    operationService.updateConnectionOperationIds(connectionId, expectedOperationIds);
    Set<UUID> actualOperationIds = fetchOperationIdsForConnectionId(connectionId);
    assertEquals(expectedOperationIds, actualOperationIds);

    // now, add back one operation
    expectedOperationIds = Collections.singleton(existingOperationIds.get(0));
    operationService.updateConnectionOperationIds(connectionId, expectedOperationIds);
    actualOperationIds = fetchOperationIdsForConnectionId(connectionId);
    assertEquals(expectedOperationIds, actualOperationIds);

    // finally, remove the first operation while adding back in the rest
    expectedOperationIds = existingOperationIds.stream().skip(1).collect(Collectors.toSet());
    operationService.updateConnectionOperationIds(connectionId, expectedOperationIds);
    actualOperationIds = fetchOperationIdsForConnectionId(connectionId);
    assertEquals(expectedOperationIds, actualOperationIds);
  }

  private Set<UUID> fetchOperationIdsForConnectionId(final UUID connectionId) throws SQLException {
    return database.query(ctx -> ctx
        .selectFrom(CONNECTION_OPERATION)
        .where(CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
        .fetchSet(CONNECTION_OPERATION.OPERATION_ID));
  }

  @Test
  void testActorDefinitionWorkspaceGrantExists() throws IOException {
    final UUID workspaceId = MockData.standardWorkspaces().get(0).getWorkspaceId();
    final UUID definitionId = MockData.standardSourceDefinitions().get(0).getSourceDefinitionId();

    assertFalse(actorDefinitionService.actorDefinitionWorkspaceGrantExists(definitionId, workspaceId, ScopeType.WORKSPACE));

    actorDefinitionService.writeActorDefinitionWorkspaceGrant(definitionId, workspaceId, ScopeType.WORKSPACE);
    assertTrue(actorDefinitionService.actorDefinitionWorkspaceGrantExists(definitionId, workspaceId, ScopeType.WORKSPACE));

    actorDefinitionService.deleteActorDefinitionWorkspaceGrant(definitionId, workspaceId, ScopeType.WORKSPACE);
    assertFalse(actorDefinitionService.actorDefinitionWorkspaceGrantExists(definitionId, workspaceId, ScopeType.WORKSPACE));
  }

  @Test
  void testListPublicSourceDefinitions() throws IOException {
    final List<StandardSourceDefinition> actualDefinitions = sourceService.listPublicSourceDefinitions(false);
    assertEquals(List.of(MockData.publicSourceDefinition()), actualDefinitions);
  }

  @Test
  void testListWorkspaceSources() throws IOException {
    final UUID workspaceId = MockData.standardWorkspaces().get(1).getWorkspaceId();
    final List<SourceConnection> expectedSources = MockData.sourceConnections().stream()
        .filter(source -> source.getWorkspaceId().equals(workspaceId)).collect(Collectors.toList());
    final List<SourceConnection> sources = sourceService.listWorkspaceSourceConnection(workspaceId);
    final List<SourceConnection> nullCreatedAtSources = sources.stream().map(sourceConnection -> sourceConnection.withCreatedAt(null)).toList();
    assertThat(nullCreatedAtSources).hasSameElementsAs(expectedSources);
  }

  @Test
  void testListWorkspaceDestinations() throws IOException {
    final UUID workspaceId = MockData.standardWorkspaces().get(0).getWorkspaceId();
    final List<DestinationConnection> expectedDestinations = MockData.destinationConnections().stream()
        .filter(destination -> destination.getWorkspaceId().equals(workspaceId)).collect(Collectors.toList());
    final List<DestinationConnection> destinations = destinationService.listWorkspaceDestinationConnection(workspaceId);
    final List<DestinationConnection> nullCreatedAtDestinations =
        destinations.stream().map(destinationConnection -> destinationConnection.withCreatedAt(null)).toList();
    assertThat(nullCreatedAtDestinations).hasSameElementsAs(expectedDestinations);
  }

  @Test
  void testSourceDefinitionGrants() throws IOException {
    final UUID workspaceId = MockData.standardWorkspaces().get(0).getWorkspaceId();
    final StandardSourceDefinition grantableDefinition1 = MockData.grantableSourceDefinition1();
    final StandardSourceDefinition grantableDefinition2 = MockData.grantableSourceDefinition2();
    final StandardSourceDefinition customDefinition = MockData.customSourceDefinition();

    actorDefinitionService.writeActorDefinitionWorkspaceGrant(customDefinition.getSourceDefinitionId(), workspaceId, ScopeType.WORKSPACE);
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(grantableDefinition1.getSourceDefinitionId(), workspaceId, ScopeType.WORKSPACE);
    final List<StandardSourceDefinition> actualGrantedDefinitions = sourceService
        .listGrantedSourceDefinitions(workspaceId, false);
    assertThat(actualGrantedDefinitions).hasSameElementsAs(List.of(grantableDefinition1, customDefinition));

    final List<Entry<StandardSourceDefinition, Boolean>> actualGrantableDefinitions = sourceService
        .listGrantableSourceDefinitions(workspaceId, false);
    assertThat(actualGrantableDefinitions).hasSameElementsAs(List.of(
        Map.entry(grantableDefinition1, true),
        Map.entry(grantableDefinition2, false)));
  }

  // todo: testSourceDefinitionGrants for organization

  @Test
  void testListPublicDestinationDefinitions() throws IOException {
    final List<StandardDestinationDefinition> actualDefinitions = destinationService.listPublicDestinationDefinitions(false);
    assertEquals(List.of(MockData.publicDestinationDefinition()), actualDefinitions);
  }

  @Test
  void testDestinationDefinitionGrants() throws IOException {
    final UUID workspaceId = MockData.standardWorkspaces().get(0).getWorkspaceId();
    final StandardDestinationDefinition grantableDefinition1 = MockData.grantableDestinationDefinition1();
    final StandardDestinationDefinition grantableDefinition2 = MockData.grantableDestinationDefinition2();
    final StandardDestinationDefinition customDefinition = MockData.customDestinationDefinition();

    actorDefinitionService.writeActorDefinitionWorkspaceGrant(customDefinition.getDestinationDefinitionId(), workspaceId, ScopeType.WORKSPACE);
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(grantableDefinition1.getDestinationDefinitionId(), workspaceId, ScopeType.WORKSPACE);
    final List<StandardDestinationDefinition> actualGrantedDefinitions = destinationService
        .listGrantedDestinationDefinitions(workspaceId, false);
    assertThat(actualGrantedDefinitions).hasSameElementsAs(List.of(grantableDefinition1, customDefinition));

    final List<Entry<StandardDestinationDefinition, Boolean>> actualGrantableDefinitions = destinationService
        .listGrantableDestinationDefinitions(workspaceId, false);
    assertThat(actualGrantableDefinitions).hasSameElementsAs(List.of(
        Map.entry(grantableDefinition1, true),
        Map.entry(grantableDefinition2, false)));
  }

  // todo: testDestinationDefinitionGrants for organization

  @Test
  void testWorkspaceCanUseDefinition() throws IOException {
    final UUID workspaceId = MockData.standardWorkspaces().get(0).getWorkspaceId();
    final UUID otherWorkspaceId = MockData.standardWorkspaces().get(1).getWorkspaceId();
    final UUID publicDefinitionId = MockData.publicSourceDefinition().getSourceDefinitionId();
    final UUID grantableDefinition1Id = MockData.grantableSourceDefinition1().getSourceDefinitionId();
    final UUID grantableDefinition2Id = MockData.grantableSourceDefinition2().getSourceDefinitionId();
    final UUID customDefinitionId = MockData.customSourceDefinition().getSourceDefinitionId();

    // Can use public definitions
    assertTrue(workspaceService.workspaceCanUseDefinition(publicDefinitionId, workspaceId));

    // Can use granted definitions
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(grantableDefinition1Id, workspaceId, ScopeType.WORKSPACE);
    assertTrue(workspaceService.workspaceCanUseDefinition(grantableDefinition1Id, workspaceId));
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(customDefinitionId, workspaceId, ScopeType.WORKSPACE);
    assertTrue(workspaceService.workspaceCanUseDefinition(customDefinitionId, workspaceId));

    // Cannot use private definitions without grant
    assertFalse(workspaceService.workspaceCanUseDefinition(grantableDefinition2Id, workspaceId));

    // Cannot use other workspace's grants
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(grantableDefinition2Id, otherWorkspaceId, ScopeType.WORKSPACE);
    assertFalse(workspaceService.workspaceCanUseDefinition(grantableDefinition2Id, workspaceId));

    // Passing invalid IDs returns false
    assertFalse(workspaceService.workspaceCanUseDefinition(new UUID(0L, 0L), workspaceId));

    // workspaceCanUseCustomDefinition can only be true for custom definitions
    assertTrue(workspaceService.workspaceCanUseCustomDefinition(customDefinitionId, workspaceId));
    assertFalse(workspaceService.workspaceCanUseCustomDefinition(grantableDefinition1Id, workspaceId));

    // todo: add tests for organizations
    // to test orgs, need to somehow link org to workspace
  }

  @Test
  void testGetDestinationOAuthByDefinitionId() throws IOException {

    final DestinationOAuthParameter destinationOAuthParameter = MockData.destinationOauthParameters().get(0);
    final Optional<DestinationOAuthParameter> result = oauthService.getDestinationOAuthParamByDefinitionIdOptional(
        destinationOAuthParameter.getWorkspaceId(), destinationOAuthParameter.getDestinationDefinitionId());
    assertTrue(result.isPresent());
    assertEquals(destinationOAuthParameter, result.get());
  }

  @Test
  void testMissingDestinationOAuthByDefinitionId() throws IOException {
    final UUID missingId = UUID.fromString("fc59cfa0-06de-4c8b-850b-46d4cfb65629");
    final DestinationOAuthParameter destinationOAuthParameter = MockData.destinationOauthParameters().get(0);
    Optional<DestinationOAuthParameter> result =
        oauthService.getDestinationOAuthParamByDefinitionIdOptional(destinationOAuthParameter.getWorkspaceId(), missingId);
    assertFalse(result.isPresent());

    result = oauthService.getDestinationOAuthParamByDefinitionIdOptional(missingId, destinationOAuthParameter.getDestinationDefinitionId());
    assertFalse(result.isPresent());
  }

  @Test
  void testGetSourceOAuthByDefinitionId() throws IOException {
    final SourceOAuthParameter sourceOAuthParameter = MockData.sourceOauthParameters().get(0);
    final Optional<SourceOAuthParameter> result = oauthService.getSourceOAuthParamByDefinitionIdOptional(sourceOAuthParameter.getWorkspaceId(),
        sourceOAuthParameter.getSourceDefinitionId());
    assertTrue(result.isPresent());
    assertEquals(sourceOAuthParameter, result.get());
  }

  @Test
  void testMissingSourceOAuthByDefinitionId() throws IOException {
    final UUID missingId = UUID.fromString("fc59cfa0-06de-4c8b-850b-46d4cfb65629");
    final SourceOAuthParameter sourceOAuthParameter = MockData.sourceOauthParameters().get(0);
    Optional<SourceOAuthParameter> result =
        oauthService.getSourceOAuthParamByDefinitionIdOptional(sourceOAuthParameter.getWorkspaceId(), missingId);
    assertFalse(result.isPresent());

    result = oauthService.getSourceOAuthParamByDefinitionIdOptional(missingId, sourceOAuthParameter.getSourceDefinitionId());
    assertFalse(result.isPresent());
  }

  @Test
  void testGetStandardSyncUsingOperation() throws IOException {
    final UUID operationId = MockData.standardSyncOperations().get(0).getOperationId();
    final List<StandardSync> expectedSyncs = copyWithV1Types(MockData.standardSyncs().subList(0, 3));
    final List<StandardSync> actualSyncs = connectionService.listStandardSyncsUsingOperation(operationId);

    assertSyncsMatch(expectedSyncs, actualSyncs);
  }

  private List<StandardSync> copyWithV1Types(final List<StandardSync> syncs) {
    return syncs;
    // TODO adjust with data types feature flag testing
    // return syncs.stream()
    // .map(standardSync -> {
    // final StandardSync copiedStandardSync = Jsons.deserialize(Jsons.serialize(standardSync),
    // StandardSync.class);
    // copiedStandardSync.setCatalog(MockData.getConfiguredCatalogWithV1DataTypes());
    // return copiedStandardSync;
    // })
    // .toList();
  }

  private void assertSyncsMatch(final List<StandardSync> expectedSyncs, final List<StandardSync> actualSyncs) {
    assertEquals(expectedSyncs.size(), actualSyncs.size());

    for (final StandardSync expected : expectedSyncs) {

      final Optional<StandardSync> maybeActual = actualSyncs.stream().filter(s -> s.getConnectionId().equals(expected.getConnectionId())).findFirst();
      if (maybeActual.isEmpty()) {
        Assertions.fail(String.format("Expected to find connectionId %s in result, but actual connectionIds are %s",
            expected.getConnectionId(),
            actualSyncs.stream().map(StandardSync::getConnectionId).collect(Collectors.toList())));
      }
      final StandardSync actual = maybeActual.get();

      // operationIds can be ordered differently in the query result than in the mock data, so they need
      // to be verified separately
      // from the rest of the sync.
      assertThat(actual.getOperationIds()).hasSameElementsAs(expected.getOperationIds());

      // now, clear operationIds so the rest of the sync can be compared
      expected.setOperationIds(null);
      actual.setOperationIds(null);
      expected.setCreatedAt(null);
      actual.setCreatedAt(null);
      assertEquals(expected, actual);
    }
  }

  @Test
  void testDeleteStandardSyncOperation()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final UUID deletedOperationId = MockData.standardSyncOperations().get(0).getOperationId();
    final List<StandardSync> syncs = MockData.standardSyncs();
    operationService.deleteStandardSyncOperation(deletedOperationId);

    for (final StandardSync sync : syncs) {
      final StandardSync retrievedSync = connectionService.getStandardSync(sync.getConnectionId());
      for (final UUID operationId : sync.getOperationIds()) {
        if (operationId.equals(deletedOperationId)) {
          assertThat(retrievedSync.getOperationIds()).doesNotContain(deletedOperationId);
        } else {
          assertThat(retrievedSync.getOperationIds()).contains(operationId);
        }
      }
    }
  }

  @Test
  void testGetSourceAndDefinitionsFromSourceIds() throws IOException {
    final List<UUID> sourceIds = MockData.sourceConnections().subList(0, 2).stream().map(SourceConnection::getSourceId).toList();

    final List<SourceAndDefinition> expected = List.of(
        new SourceAndDefinition(MockData.sourceConnections().get(0), MockData.standardSourceDefinitions().get(0)),
        new SourceAndDefinition(MockData.sourceConnections().get(1), MockData.standardSourceDefinitions().get(1)));

    final List<SourceAndDefinition> actual = sourceService.getSourceAndDefinitionsFromSourceIds(sourceIds);
    final List<SourceAndDefinition> result = actual.stream().map(sourceAndDefinition -> {
      final SourceAndDefinition copy = new SourceAndDefinition(sourceAndDefinition.source(), sourceAndDefinition.definition());
      copy.source().setCreatedAt(null);
      return copy;
    }).toList();

    assertThat(result).hasSameElementsAs(expected);
  }

  @Test
  void testGetDestinationAndDefinitionsFromDestinationIds() throws IOException {
    final List<UUID> destinationIds = MockData.destinationConnections().subList(0, 2).stream().map(DestinationConnection::getDestinationId).toList();

    final List<DestinationAndDefinition> actual = destinationService.getDestinationAndDefinitionsFromDestinationIds(destinationIds);

    final List<DestinationAndDefinition> expected = List.of(
        new DestinationAndDefinition(MockData.destinationConnections().get(0), MockData.standardDestinationDefinitions().get(0)),
        new DestinationAndDefinition(MockData.destinationConnections().get(1), MockData.standardDestinationDefinitions().get(1)));

    final List<DestinationAndDefinition> result = actual.stream().map(destinationAndDefinition -> {
      final DestinationAndDefinition copy =
          new DestinationAndDefinition(destinationAndDefinition.destination(), destinationAndDefinition.definition());
      copy.destination().setCreatedAt(null);
      return copy;
    }).toList();

    assertThat(result).hasSameElementsAs(expected);
  }

  @Test
  void testGetGeographyForConnection() throws IOException {
    final StandardSync sync = MockData.standardSyncs().get(0);
    final Geography expected = sync.getGeography();
    final Geography actual = connectionService.getGeographyForConnection(sync.getConnectionId());

    assertEquals(expected, actual);
  }

  @Test
  void testGetGeographyForWorkspace() throws IOException {
    final StandardWorkspace workspace = MockData.standardWorkspaces().get(0);
    final Geography expected = workspace.getDefaultGeography();
    final Geography actual = workspaceService.getGeographyForWorkspace(workspace.getWorkspaceId());

    assertEquals(expected, actual);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  void testGetMostRecentActorCatalogFetchEventForSource() throws SQLException, IOException {
    for (final ActorCatalog actorCatalog : MockData.actorCatalogs()) {
      writeActorCatalog(database, Collections.singletonList(actorCatalog));
    }

    final OffsetDateTime now = OffsetDateTime.now();
    final OffsetDateTime yesterday = now.minusDays(1L);

    final List<ActorCatalogFetchEvent> fetchEvents = MockData.actorCatalogFetchEventsSameSource();
    final ActorCatalogFetchEvent fetchEvent1 = fetchEvents.get(0);
    final ActorCatalogFetchEvent fetchEvent2 = fetchEvents.get(1);

    database.transaction(ctx -> {
      insertCatalogFetchEvent(
          ctx,
          fetchEvent1.getActorId(),
          fetchEvent1.getActorCatalogId(),
          yesterday);
      insertCatalogFetchEvent(
          ctx,
          fetchEvent2.getActorId(),
          fetchEvent2.getActorCatalogId(),
          now);
      // Insert a second identical copy to verify that the query can handle duplicates since the records
      // are not guaranteed to be unique.
      insertCatalogFetchEvent(
          ctx,
          fetchEvent2.getActorId(),
          fetchEvent2.getActorCatalogId(),
          now);

      return null;
    });

    final Optional<ActorCatalogFetchEvent> result =
        catalogService.getMostRecentActorCatalogFetchEventForSource(fetchEvent1.getActorId());

    assertEquals(fetchEvent2.getActorCatalogId(), result.get().getActorCatalogId());
  }

  @Test
  void testGetMostRecentActorCatalogFetchEventForSources() throws SQLException, IOException {
    for (final ActorCatalog actorCatalog : MockData.actorCatalogs()) {
      writeActorCatalog(database, Collections.singletonList(actorCatalog));
    }

    database.transaction(ctx -> {
      MockData.actorCatalogFetchEventsForAggregationTest().forEach(actorCatalogFetchEvent -> insertCatalogFetchEvent(
          ctx,
          actorCatalogFetchEvent.getActorCatalogFetchEvent().getActorId(),
          actorCatalogFetchEvent.getActorCatalogFetchEvent().getActorCatalogId(),
          actorCatalogFetchEvent.getCreatedAt()));

      return null;
    });

    final Map<UUID, ActorCatalogFetchEvent> result =
        catalogService.getMostRecentActorCatalogFetchEventForSources(List.of(MockData.SOURCE_ID_1,
            MockData.SOURCE_ID_2));

    assertEquals(MockData.ACTOR_CATALOG_ID_1, result.get(MockData.SOURCE_ID_1).getActorCatalogId());
    assertEquals(MockData.ACTOR_CATALOG_ID_3, result.get(MockData.SOURCE_ID_2).getActorCatalogId());
    assertEquals(0, catalogService.getMostRecentActorCatalogFetchEventForSources(Collections.emptyList()).size());
  }

  @Test
  void testGetMostRecentActorCatalogFetchEventWithDuplicates() throws SQLException, IOException {
    // Tests that we can handle two fetch events in the db with the same actor id, actor catalog id, and
    // timestamp e.g., from duplicate discoveries.
    for (final ActorCatalog actorCatalog : MockData.actorCatalogs()) {
      writeActorCatalog(database, Collections.singletonList(actorCatalog));
    }

    database.transaction(ctx -> {
      // Insert the fetch events twice.
      MockData.actorCatalogFetchEventsForAggregationTest().forEach(actorCatalogFetchEvent -> {
        insertCatalogFetchEvent(
            ctx,
            actorCatalogFetchEvent.getActorCatalogFetchEvent().getActorId(),
            actorCatalogFetchEvent.getActorCatalogFetchEvent().getActorCatalogId(),
            actorCatalogFetchEvent.getCreatedAt());
        insertCatalogFetchEvent(
            ctx,
            actorCatalogFetchEvent.getActorCatalogFetchEvent().getActorId(),
            actorCatalogFetchEvent.getActorCatalogFetchEvent().getActorCatalogId(),
            actorCatalogFetchEvent.getCreatedAt());
      });
      return null;
    });

    final Map<UUID, ActorCatalogFetchEvent> result =
        catalogService.getMostRecentActorCatalogFetchEventForSources(List.of(MockData.SOURCE_ID_1,
            MockData.SOURCE_ID_2));

    assertEquals(MockData.ACTOR_CATALOG_ID_1, result.get(MockData.SOURCE_ID_1).getActorCatalogId());
    assertEquals(MockData.ACTOR_CATALOG_ID_3, result.get(MockData.SOURCE_ID_2).getActorCatalogId());
  }

  @Test
  void testGetActorDefinitionsInUseToProtocolVersion() throws IOException {
    final Set<UUID> actorDefinitionIds = new HashSet<>();
    actorDefinitionIds.addAll(MockData.sourceConnections().stream().map(SourceConnection::getSourceDefinitionId).toList());
    actorDefinitionIds.addAll(MockData.destinationConnections().stream().map(DestinationConnection::getDestinationDefinitionId).toList());
    assertEquals(actorDefinitionIds, actorDefinitionService.getActorDefinitionToProtocolVersionMap().keySet());
  }

  private void insertCatalogFetchEvent(final DSLContext ctx, final UUID sourceId, final UUID catalogId, final OffsetDateTime creationDate) {
    ctx.insertInto(ACTOR_CATALOG_FETCH_EVENT)
        .columns(
            ACTOR_CATALOG_FETCH_EVENT.ID,
            ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID,
            ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID,
            ACTOR_CATALOG_FETCH_EVENT.CONFIG_HASH,
            ACTOR_CATALOG_FETCH_EVENT.ACTOR_VERSION,
            ACTOR_CATALOG_FETCH_EVENT.CREATED_AT,
            ACTOR_CATALOG_FETCH_EVENT.MODIFIED_AT)
        .values(UUID.randomUUID(), sourceId, catalogId, "", "", creationDate, creationDate)
        .execute();
  }

  private static void writeActorCatalog(final Database database, final List<ActorCatalog> configs) throws SQLException {
    database.transaction(ctx -> {
      writeActorCatalog(configs, ctx);
      return null;
    });
  }

  private static void writeActorCatalog(final List<ActorCatalog> configs, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    configs.forEach((actorCatalog) -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(ACTOR_CATALOG)
          .where(ACTOR_CATALOG.ID.eq(actorCatalog.getId())));

      if (isExistingConfig) {
        ctx.update(ACTOR_CATALOG)
            .set(ACTOR_CATALOG.CATALOG, JSONB.valueOf(Jsons.serialize(actorCatalog.getCatalog())))
            .set(ACTOR_CATALOG.CATALOG_HASH, actorCatalog.getCatalogHash())
            .set(ACTOR_CATALOG.MODIFIED_AT, timestamp)
            .where(ACTOR_CATALOG.ID.eq(actorCatalog.getId()))
            .execute();
      } else {
        ctx.insertInto(ACTOR_CATALOG)
            .set(ACTOR_CATALOG.ID, actorCatalog.getId())
            .set(ACTOR_CATALOG.CATALOG, JSONB.valueOf(Jsons.serialize(actorCatalog.getCatalog())))
            .set(ACTOR_CATALOG.CATALOG_HASH, actorCatalog.getCatalogHash())
            .set(ACTOR_CATALOG.CREATED_AT, timestamp)
            .set(ACTOR_CATALOG.MODIFIED_AT, timestamp)
            .execute();
      }
    });
  }

  @Test
  void testGetEarlySyncJobs() throws IOException {
    // This test just verifies that the query can be run against configAPI DB.
    // The query has been tested locally against prod DB to verify the outputs.
    final Set<Long> earlySyncJobs = connectionService.listEarlySyncJobs(7, 30);
    assertNotNull(earlySyncJobs);
  }

}
