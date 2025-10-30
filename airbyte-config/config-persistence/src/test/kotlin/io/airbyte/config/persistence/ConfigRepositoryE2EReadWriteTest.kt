/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.json.Jsons.canonicalJsonSerialize
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.serialize
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.DestinationConnection
import io.airbyte.config.ScopeType
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.persistence.MockData.ACTOR_CATALOG_ID_1
import io.airbyte.config.persistence.MockData.ACTOR_CATALOG_ID_3
import io.airbyte.config.persistence.MockData.DESTINATION_ID_1
import io.airbyte.config.persistence.MockData.SOURCE_ID_1
import io.airbyte.config.persistence.MockData.SOURCE_ID_2
import io.airbyte.config.persistence.MockData.actorCatalogFetchEventsForAggregationTest
import io.airbyte.config.persistence.MockData.actorCatalogFetchEventsSameSource
import io.airbyte.config.persistence.MockData.actorCatalogs
import io.airbyte.config.persistence.MockData.actorDefinitionVersion
import io.airbyte.config.persistence.MockData.customDestinationDefinition
import io.airbyte.config.persistence.MockData.customSourceDefinition
import io.airbyte.config.persistence.MockData.defaultOrganization
import io.airbyte.config.persistence.MockData.destinationConnections
import io.airbyte.config.persistence.MockData.destinationOauthParameters
import io.airbyte.config.persistence.MockData.grantableDestinationDefinition1
import io.airbyte.config.persistence.MockData.grantableDestinationDefinition2
import io.airbyte.config.persistence.MockData.grantableSourceDefinition1
import io.airbyte.config.persistence.MockData.grantableSourceDefinition2
import io.airbyte.config.persistence.MockData.publicDestinationDefinition
import io.airbyte.config.persistence.MockData.publicSourceDefinition
import io.airbyte.config.persistence.MockData.sourceConnections
import io.airbyte.config.persistence.MockData.sourceOauthParameters
import io.airbyte.config.persistence.MockData.standardDestinationDefinitions
import io.airbyte.config.persistence.MockData.standardSourceDefinitions
import io.airbyte.config.persistence.MockData.standardSyncOperations
import io.airbyte.config.persistence.MockData.standardSyncs
import io.airbyte.config.persistence.MockData.standardWorkspaces
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.CatalogServiceJooqImpl
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.OAuthServiceJooqImpl
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.data.services.shared.DestinationAndDefinition
import io.airbyte.data.services.shared.SourceAndDefinition
import io.airbyte.data.services.shared.StandardSyncQuery
import io.airbyte.db.Database
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_WORKSPACE_GRANT
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorCatalogType
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.DestinationCatalog
import io.airbyte.protocol.models.v0.DestinationOperation
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.airbyte.protocol.models.v0.Field
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.assertj.core.api.Assertions.assertThat
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.spy
import java.time.OffsetDateTime
import java.util.Map
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer
import java.util.stream.Collectors
import org.mockito.Mockito.`when` as whenever

/**
 * The tests in this class should be moved into separate test suites grouped by resource. Do NOT add
 * new tests here. Add them to resource-based test suites (e.g. WorkspacePersistenceTest). If one
 * does not exist yet for that resource yet, create one and follow the pattern.
 */
@Deprecated("")
internal class ConfigRepositoryE2EReadWriteTest : BaseConfigDatabaseTest() {
  private lateinit var catalogService: CatalogService
  private lateinit var oauthService: OAuthService
  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var connectionService: ConnectionService
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var operationService: OperationService

  private lateinit var dataplaneGroupIds: MutableMap<String, UUID>

  @BeforeEach
  fun setup() {
    truncateAllTables()

    val featureFlagClient = mock(TestClient::class.java)
    whenever(
      featureFlagClient.stringVariation(org.mockito.kotlin.eq(HeartbeatMaxSecondsBetweenMessages), org.mockito.kotlin.any<SourceDefinition>()),
    ).thenReturn("3600")

    val metricClient = mock(MetricClient::class.java)
    val secretsRepositoryReader = mock(SecretsRepositoryReader::class.java)
    val secretsRepositoryWriter = mock(SecretsRepositoryWriter::class.java)
    val secretPersistenceConfigService = mock(SecretPersistenceConfigService::class.java)
    val scopedConfigurationService = mock(ScopedConfigurationService::class.java)
    val connectionTimelineEventService = mock(ConnectionTimelineEventService::class.java)
    val actorPaginationServiceHelper = mock(ActorServicePaginationHelper::class.java)

    val organizationService = OrganizationServiceJooqImpl(database!!)
    val defaultOrg = defaultOrganization()
    organizationService.writeOrganization(defaultOrg)

    val dataplaneGroupService = DataplaneGroupServiceTestJooqImpl(database!!)
    dataplaneGroupIds = mutableMapOf()
    dataplaneGroupService.writeDataplaneGroup(
      DataplaneGroup()
        .withId(DATAPLANE_GROUP_ID)
        .withOrganizationId(defaultOrg.organizationId)
        .withName("test")
        .withEnabled(true)
        .withTombstone(false),
    )

    connectionService = spy(ConnectionServiceJooqImpl(database!!))
    actorDefinitionService = spy(ActorDefinitionServiceJooqImpl(database!!))
    val actorDefinitionVersionUpdater =
      ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService,
        actorDefinitionService,
        scopedConfigurationService,
        connectionTimelineEventService,
      )
    catalogService = spy(CatalogServiceJooqImpl(database!!))

    workspaceService =
      spy(
        WorkspaceServiceJooqImpl(
          database!!,
          featureFlagClient,
          secretsRepositoryReader,
          secretsRepositoryWriter,
          secretPersistenceConfigService,
          metricClient,
        ),
      )

    oauthService =
      spy(
        OAuthServiceJooqImpl(
          database!!,
          featureFlagClient,
          secretsRepositoryReader,
          secretPersistenceConfigService,
          metricClient,
          workspaceService,
        ),
      )

    sourceService =
      spy(
        SourceServiceJooqImpl(
          database!!,
          featureFlagClient,
          secretPersistenceConfigService,
          connectionService,
          actorDefinitionVersionUpdater,
          metricClient,
          actorPaginationServiceHelper,
        ),
      )

    destinationService =
      spy(
        DestinationServiceJooqImpl(
          database!!,
          featureFlagClient,
          connectionService,
          actorDefinitionVersionUpdater,
          metricClient,
          actorPaginationServiceHelper,
        ),
      )

    operationService = spy(OperationServiceJooqImpl(database!!))

    for (workspace in standardWorkspaces()) {
      workspaceService.writeStandardWorkspaceNoSecrets(workspace!!)
    }

    for (sourceDefinition in standardSourceDefinitions()) {
      val actorDefinitionVersion =
        MockData
          .actorDefinitionVersion()!!
          .withActorDefinitionId(sourceDefinition!!.sourceDefinitionId)
          .withVersionId(sourceDefinition.defaultVersionId)
      sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, emptyList())
    }

    for (destinationDefinition in standardDestinationDefinitions()) {
      val actorDefinitionVersion =
        MockData
          .actorDefinitionVersion()!!
          .withActorDefinitionId(destinationDefinition!!.destinationDefinitionId)
          .withVersionId(destinationDefinition.defaultVersionId)
      destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, emptyList())
    }

    for (source in sourceConnections()) {
      sourceService.writeSourceConnectionNoSecrets(source!!)
    }

    for (destination in destinationConnections()) {
      destinationService.writeDestinationConnectionNoSecrets(destination!!)
    }

    for (operation in standardSyncOperations()) {
      operationService.writeStandardSyncOperation(operation!!)
    }

    for (sync in standardSyncs()) {
      connectionService.writeStandardSync(sync!!)
    }

    for (oAuthParameter in sourceOauthParameters()) {
      oauthService.writeSourceOAuthParam(oAuthParameter!!)
    }

    for (oAuthParameter in destinationOauthParameters()) {
      oauthService.writeDestinationOAuthParam(oAuthParameter!!)
    }

    database?.transaction(
      { ctx: DSLContext ->
        ctx.truncate(ACTOR_DEFINITION_WORKSPACE_GRANT).execute()
      },
    )
  }

  @Test
  fun testWorkspaceCountConnections() {
    val workspaceId = standardWorkspaces().get(0)!!.workspaceId
    assertEquals(3, workspaceService.countConnectionsForWorkspace(workspaceId))
    assertEquals(2, workspaceService.countDestinationsForWorkspace(workspaceId))
    assertEquals(2, workspaceService.countSourcesForWorkspace(workspaceId))
  }

  @Test
  fun testWorkspaceCountConnectionsDeprecated() {
    val workspaceId = standardWorkspaces().get(1)!!.workspaceId
    // One connection is active and one is locked
    assertEquals(2, workspaceService.countConnectionsForWorkspace(workspaceId))
  }

  @Test
  fun testFetchActorsUsingDefinition() {
    val destinationDefinitionId = publicDestinationDefinition()!!.destinationDefinitionId
    val sourceDefinitionId = publicSourceDefinition()!!.sourceDefinitionId
    val destinationConnections =
      destinationService
        .listDestinationsForDefinition(
          destinationDefinitionId,
        ).toMutableList()
    val sourceConnections =
      sourceService
        .listSourcesForDefinition(
          sourceDefinitionId,
        ).toMutableList()

    val nullCreatedAtDestinationConnections =
      destinationConnections
        .map { destinationConnection ->
          destinationConnection.withCreatedAt(null).withUpdatedAt(null)
        }

    val nullCreatedAtSourceConnections =
      sourceConnections
        .map { sourceConnection -> sourceConnection.withCreatedAt(null).withUpdatedAt(null) }

    assertThat(nullCreatedAtDestinationConnections)
      .containsExactlyElementsOf(
        destinationConnections()
          .filter { d: DestinationConnection? -> d!!.destinationDefinitionId == destinationDefinitionId && !d.getTombstone() }
          .toList(),
      )
    assertThat(nullCreatedAtSourceConnections)
      .containsExactlyElementsOf(
        sourceConnections()
          .filter { d: SourceConnection? -> d!!.sourceDefinitionId == sourceDefinitionId && !d.getTombstone() }
          .toList(),
      )
  }

  @Test
  fun testReadActorCatalog() {
    val otherConfigHash = "OtherConfigHash"
    val workspace = standardWorkspaces()[0]

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withSourceType(StandardSourceDefinition.SourceType.DATABASE)
        .withName("sourceDefinition")
    val actorDefinitionVersion =
      actorDefinitionVersion()!!
        .withActorDefinitionId(sourceDefinition.sourceDefinitionId)
        .withVersionId(sourceDefinition.defaultVersionId)
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf<ActorDefinitionBreakingChange>())

    val source =
      SourceConnection()
        .withSourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .withSourceId(UUID.randomUUID())
        .withName("SomeConnector")
        .withWorkspaceId(workspace!!.workspaceId)
        .withConfiguration(deserialize("{}"))
    sourceService.writeSourceConnectionNoSecrets(source)

    val firstCatalog =
      CatalogHelpers.createAirbyteCatalog(
        "product",
        Field.of("label", JsonSchemaType.STRING),
        Field.of("size", JsonSchemaType.NUMBER),
        Field.of("color", JsonSchemaType.STRING),
        Field.of("price", JsonSchemaType.NUMBER),
      )
    catalogService.writeActorCatalogWithFetchEvent(firstCatalog, source.sourceId, DOCKER_IMAGE_TAG, CONFIG_HASH)

    val secondCatalog =
      CatalogHelpers.createAirbyteCatalog(
        "product",
        Field.of("size", JsonSchemaType.NUMBER),
        Field.of("label", JsonSchemaType.STRING),
        Field.of("color", JsonSchemaType.STRING),
        Field.of("price", JsonSchemaType.NUMBER),
      )
    catalogService.writeActorCatalogWithFetchEvent(secondCatalog, source.sourceId, DOCKER_IMAGE_TAG, otherConfigHash)

    val expectedCatalog =
      (
        "{" +
          "\"streams\":[" +
          "{" +
          "\"name\":\"product\"," +
          "\"json_schema\":{" +
          "\"type\":\"object\"," +
          "\"properties\":{" +
          "\"size\":{\"type\":\"number\"}," +
          "\"color\":{\"type\":\"string\"}," +
          "\"price\":{\"type\":\"number\"}," +
          "\"label\":{\"type\":\"string\"}" +
          "}" +
          "}," +
          "\"supported_sync_modes\":[\"full_refresh\"]," +
          "\"default_cursor_field\":[]," +
          "\"source_defined_primary_key\":[]" +
          "}" +
          "]" +
          "}"
      )

    val catalogResult = catalogService.getActorCatalog(source.sourceId, DOCKER_IMAGE_TAG, CONFIG_HASH)
    assertTrue(catalogResult.isPresent)
    assertEquals(
      deserialize(expectedCatalog, AirbyteCatalog::class.java),
      Jsons.`object`(catalogResult.get().catalog, AirbyteCatalog::class.java),
    )
  }

  @Test
  fun testWriteCanonicalHashActorCatalog() {
    val canonicalConfigHash = "8129d38a"
    val workspace = standardWorkspaces()[0]

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withSourceType(StandardSourceDefinition.SourceType.DATABASE)
        .withName("sourceDefinition")
    val actorDefinitionVersion =
      actorDefinitionVersion()!!
        .withActorDefinitionId(sourceDefinition.sourceDefinitionId)
        .withVersionId(sourceDefinition.defaultVersionId)
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf())

    val source =
      SourceConnection()
        .withSourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .withSourceId(UUID.randomUUID())
        .withName("SomeConnector")
        .withWorkspaceId(workspace!!.workspaceId)
        .withConfiguration(deserialize("{}"))
    sourceService.writeSourceConnectionNoSecrets(source)

    val firstCatalog =
      CatalogHelpers.createAirbyteCatalog(
        "product",
        Field.of("label", JsonSchemaType.STRING),
        Field.of("size", JsonSchemaType.NUMBER),
        Field.of("color", JsonSchemaType.STRING),
        Field.of("price", JsonSchemaType.NUMBER),
      )
    catalogService.writeActorCatalogWithFetchEvent(firstCatalog, source.sourceId, DOCKER_IMAGE_TAG, CONFIG_HASH)

    val expectedCatalog =
      (
        "{" +
          "\"streams\":[" +
          "{" +
          "\"default_cursor_field\":[]," +
          "\"json_schema\":{" +
          "\"properties\":{" +
          "\"color\":{\"type\":\"string\"}," +
          "\"label\":{\"type\":\"string\"}," +
          "\"price\":{\"type\":\"number\"}," +
          "\"size\":{\"type\":\"number\"}" +
          "}," +
          "\"type\":\"object\"" +
          "}," +
          "\"name\":\"product\"," +
          "\"source_defined_primary_key\":[]," +
          "\"supported_sync_modes\":[\"full_refresh\"]" +
          "}" +
          "]" +
          "}"
      )

    val catalogResult = catalogService.getActorCatalog(source.sourceId, DOCKER_IMAGE_TAG, CONFIG_HASH)
    assertTrue(catalogResult.isPresent)
    assertEquals(canonicalConfigHash, catalogResult.get().catalogHash)
    assertEquals(expectedCatalog, canonicalJsonSerialize(catalogResult.get().catalog))
  }

  @Test
  fun testSimpleInsertActorCatalog() {
    val otherConfigHash = "OtherConfigHash"
    val workspace = standardWorkspaces()[0]

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withSourceType(StandardSourceDefinition.SourceType.DATABASE)
        .withName("sourceDefinition")
    val actorDefinitionVersion =
      actorDefinitionVersion()!!
        .withActorDefinitionId(sourceDefinition.sourceDefinitionId)
        .withVersionId(sourceDefinition.defaultVersionId)
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf())

    val source =
      SourceConnection()
        .withSourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .withSourceId(UUID.randomUUID())
        .withName("SomeConnector")
        .withWorkspaceId(workspace!!.workspaceId)
        .withConfiguration(deserialize("{}"))
    sourceService.writeSourceConnectionNoSecrets(source)

    val actorCatalog = CatalogHelpers.createAirbyteCatalog("clothes", Field.of("name", JsonSchemaType.STRING))
    val expectedActorCatalog = CatalogHelpers.createAirbyteCatalog("clothes", Field.of("name", JsonSchemaType.STRING))
    catalogService.writeActorCatalogWithFetchEvent(
      actorCatalog,
      source.sourceId,
      DOCKER_IMAGE_TAG,
      CONFIG_HASH,
    )

    val catalog =
      catalogService.getActorCatalog(source.sourceId, DOCKER_IMAGE_TAG, CONFIG_HASH)
    assertTrue(catalog.isPresent)
    assertEquals(expectedActorCatalog, Jsons.`object`(catalog.get().catalog, AirbyteCatalog::class.java))
    assertFalse(catalogService.getActorCatalog(source.sourceId, OTHER_DOCKER_IMAGE_TAG, CONFIG_HASH).isPresent)
    assertFalse(catalogService.getActorCatalog(source.sourceId, DOCKER_IMAGE_TAG, otherConfigHash).isPresent)

    catalogService.writeActorCatalogWithFetchEvent(actorCatalog, source.sourceId, OTHER_DOCKER_IMAGE_TAG, CONFIG_HASH)
    val catalogNewConnectorVersion =
      catalogService.getActorCatalog(source.sourceId, OTHER_DOCKER_IMAGE_TAG, CONFIG_HASH)
    assertTrue(catalogNewConnectorVersion.isPresent)
    assertEquals(
      expectedActorCatalog,
      Jsons.`object`(catalogNewConnectorVersion.get().catalog, AirbyteCatalog::class.java),
    )

    catalogService.writeActorCatalogWithFetchEvent(actorCatalog, source.sourceId, DOCKER_IMAGE_TAG, otherConfigHash)
    val catalogNewConfig =
      catalogService.getActorCatalog(source.sourceId, DOCKER_IMAGE_TAG, otherConfigHash)
    assertTrue(catalogNewConfig.isPresent)
    assertEquals(
      expectedActorCatalog,
      Jsons.`object`(catalogNewConfig.get().catalog, AirbyteCatalog::class.java),
    )

    val catalogDbEntry =
      database
        ?.query(
          { ctx: DSLContext ->
            ctx.selectCount().from(Tables.ACTOR_CATALOG)
          },
        )?.fetchOne()!!
        .into(Int::class.javaPrimitiveType)
    assertEquals(1, catalogDbEntry)

    // Writing the previous catalog with v1 data types
    catalogService.writeActorCatalogWithFetchEvent(expectedActorCatalog, source.sourceId, DOCKER_IMAGE_TAG, otherConfigHash)
    val catalogV1NewConfig =
      catalogService.getActorCatalog(source.sourceId, DOCKER_IMAGE_TAG, otherConfigHash)
    assertTrue(catalogV1NewConfig.isPresent)
    assertEquals(
      expectedActorCatalog,
      Jsons.`object`(catalogNewConfig.get().catalog, AirbyteCatalog::class.java),
    )

    catalogService.writeActorCatalogWithFetchEvent(expectedActorCatalog, source.sourceId, "1.4.0", otherConfigHash)
    val catalogV1again =
      catalogService.getActorCatalog(source.sourceId, DOCKER_IMAGE_TAG, otherConfigHash)
    assertTrue(catalogV1again.isPresent)
    assertEquals(
      expectedActorCatalog,
      Jsons.`object`(catalogNewConfig.get().catalog, AirbyteCatalog::class.java),
    )

    val catalogDbEntry2 =
      database
        ?.query(
          { ctx: DSLContext ->
            ctx.selectCount().from(Tables.ACTOR_CATALOG)
          },
        )?.fetchOne()!!
        .into(Int::class.javaPrimitiveType)
    // TODO this should be 2 once we re-enable datatypes v1
    assertEquals(1, catalogDbEntry2)
  }

  @Test
  fun testSimpleInsertDestinationActorCatalog() {
    val otherConfigHash = "OtherConfigHash"
    val workspace = standardWorkspaces().get(0)

    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withName("destinationDefinition")
    val actorDefinitionVersion =
      actorDefinitionVersion()!!
        .withActorDefinitionId(destinationDefinition.destinationDefinitionId)
        .withVersionId(destinationDefinition.defaultVersionId)
    destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, mutableListOf())

    val destination =
      DestinationConnection()
        .withDestinationDefinitionId(destinationDefinition.destinationDefinitionId)
        .withDestinationId(UUID.randomUUID())
        .withName("SomeDestinationConnector")
        .withWorkspaceId(workspace!!.workspaceId)
        .withConfiguration(deserialize("{}"))
    destinationService.writeDestinationConnectionNoSecrets(destination)

    val destinationCatalog =
      DestinationCatalog().withOperations(
        listOf<DestinationOperation?>(
          DestinationOperation().withObjectName("test_object").withSyncMode(DestinationSyncMode.APPEND).withJsonSchema(emptyObject()),
        ),
      )
    catalogService.writeActorCatalogWithFetchEvent(destinationCatalog, destination.destinationId, DOCKER_IMAGE_TAG, CONFIG_HASH)

    val catalog =
      catalogService.getActorCatalog(destination.destinationId, DOCKER_IMAGE_TAG, CONFIG_HASH)
    assertTrue(catalog.isPresent)
    assertEquals(destinationCatalog, Jsons.`object`(catalog.get().catalog, DestinationCatalog::class.java))
    assertFalse(catalogService.getActorCatalog(destination.destinationId, OTHER_DOCKER_IMAGE_TAG, CONFIG_HASH).isPresent)
    assertFalse(catalogService.getActorCatalog(destination.destinationId, DOCKER_IMAGE_TAG, otherConfigHash).isPresent)

    catalogService.writeActorCatalogWithFetchEvent(destinationCatalog, destination.destinationId, OTHER_DOCKER_IMAGE_TAG, CONFIG_HASH)
    val catalogNewConnectorVersion =
      catalogService.getActorCatalog(destination.destinationId, OTHER_DOCKER_IMAGE_TAG, CONFIG_HASH)
    assertTrue(catalogNewConnectorVersion.isPresent)
    assertEquals(
      destinationCatalog,
      Jsons.`object`(catalogNewConnectorVersion.get().catalog, DestinationCatalog::class.java),
    )

    catalogService.writeActorCatalogWithFetchEvent(destinationCatalog, destination.destinationId, DOCKER_IMAGE_TAG, otherConfigHash)
    val catalogNewConfig =
      catalogService.getActorCatalog(destination.destinationId, DOCKER_IMAGE_TAG, otherConfigHash)
    assertTrue(catalogNewConfig.isPresent)
    assertEquals(
      destinationCatalog,
      Jsons.`object`(catalogNewConfig.get().catalog, DestinationCatalog::class.java),
    )

    val catalogDbEntry =
      database
        ?.query(
          { ctx: DSLContext ->
            ctx.selectCount().from(Tables.ACTOR_CATALOG)
          },
        )?.fetchOne()!!
        .into(Int::class.javaPrimitiveType)
    assertEquals(1, catalogDbEntry)
  }

  @Test
  fun testListWorkspaceStandardSyncAll() {
    val expectedSyncs = standardSyncs().subList(0, 4).filterNotNull().toMutableList()
    val actualSyncs =
      connectionService
        .listWorkspaceStandardSyncs(
          standardWorkspaces()[0]!!.workspaceId,
          true,
        ).toMutableList()

    assertSyncsMatch(expectedSyncs, actualSyncs)
  }

  @Test
  fun testListWorkspaceStandardSyncWithAllFiltering() {
    val workspaceId = standardWorkspaces()[0]!!.workspaceId
    val query = StandardSyncQuery(workspaceId, listOf(SOURCE_ID_1), listOf(DESTINATION_ID_1), false)
    val expectedSyncs =
      standardSyncs()
        .subList(0, 3)
        .stream()
        .filter { sync: StandardSync? -> query.destinationId!!.contains(sync!!.destinationId) }
        .filter { sync: StandardSync? -> query.sourceId!!.contains(sync!!.sourceId) }
        .toList()
        .filterNotNull()
        .toMutableList()
    val actualSyncs = connectionService.listWorkspaceStandardSyncs(query).toMutableList()

    assertSyncsMatch(expectedSyncs, actualSyncs)
  }

  @Test
  fun testListWorkspaceStandardSyncDestinationFiltering() {
    val workspaceId = standardWorkspaces()[0]!!.workspaceId
    val query = StandardSyncQuery(workspaceId, null, listOf(DESTINATION_ID_1), false)
    val expectedSyncs =
      standardSyncs()
        .subList(0, 3)
        .filter { sync: StandardSync? -> query.destinationId!!.contains(sync!!.destinationId) }
        .toList()
        .filterNotNull()
        .toMutableList()
    val actualSyncs = connectionService.listWorkspaceStandardSyncs(query).toMutableList()

    assertSyncsMatch(expectedSyncs, actualSyncs)
  }

  @Test
  fun testListWorkspaceStandardSyncSourceFiltering() {
    val workspaceId = standardWorkspaces().get(0)!!.workspaceId
    val query = StandardSyncQuery(workspaceId, listOf(SOURCE_ID_2), null, false)
    val expectedSyncs =
      standardSyncs()
        .subList(0, 3)
        .filter { sync: StandardSync? -> query.sourceId!!.contains(sync!!.sourceId) }
        .toList()
        .filterNotNull()
        .toMutableList()
    val actualSyncs = connectionService.listWorkspaceStandardSyncs(query).toMutableList()

    assertSyncsMatch(expectedSyncs, actualSyncs)
  }

  @Test
  fun testListWorkspaceStandardSyncExcludeDeleted() {
    val expectedSyncs =
      standardSyncs()
        .subList(0, 3)
        .toList()
        .filterNotNull()
        .toMutableList()
    val actualSyncs =
      connectionService.listWorkspaceStandardSyncs(standardWorkspaces().get(0)!!.workspaceId, false).toMutableList()

    assertSyncsMatch(expectedSyncs, actualSyncs)
  }

  @Test
  fun testGetWorkspaceBySlug() {
    val workspace =
      standardWorkspaces()[0]

    val tombstonedWorkspace =
      standardWorkspaces()[2]
    val retrievedWorkspace = workspaceService.getWorkspaceBySlugOptional(workspace!!.slug, false)
    val retrievedTombstonedWorkspaceNoTombstone =
      workspaceService.getWorkspaceBySlugOptional(tombstonedWorkspace!!.slug, false)
    val retrievedTombstonedWorkspace =
      workspaceService.getWorkspaceBySlugOptional(tombstonedWorkspace.slug, true)

    assertTrue(retrievedWorkspace.isPresent)

    assertThat(retrievedWorkspace.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(workspace)

    assertFalse(retrievedTombstonedWorkspaceNoTombstone.isPresent)
    assertTrue(retrievedTombstonedWorkspace.isPresent)

    assertThat(retrievedTombstonedWorkspace.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(tombstonedWorkspace)
  }

  @Test
  fun testUpdateConnectionOperationIds() {
    val sync = standardSyncs()[0]
    val existingOperationIds = sync!!.operationIds
    val connectionId = sync.connectionId

    // this test only works as intended when there are multiple operationIds
    assertTrue(existingOperationIds.size > 1)

    // first, remove all associated operations
    var expectedOperationIds = mutableSetOf<UUID>()
    operationService.updateConnectionOperationIds(connectionId, expectedOperationIds)
    var actualOperationIds = fetchOperationIdsForConnectionId(connectionId)
    assertEquals(expectedOperationIds, actualOperationIds)

    // now, add back one operation
    expectedOperationIds = mutableSetOf(existingOperationIds.get(0))
    operationService.updateConnectionOperationIds(connectionId, expectedOperationIds)
    actualOperationIds = fetchOperationIdsForConnectionId(connectionId)
    assertEquals(expectedOperationIds, actualOperationIds)

    // finally, remove the first operation while adding back in the rest
    expectedOperationIds =
      existingOperationIds
        .stream()
        .skip(1)
        .collect(Collectors.toSet())
        .toMutableSet()
    operationService.updateConnectionOperationIds(connectionId, expectedOperationIds)
    actualOperationIds = fetchOperationIdsForConnectionId(connectionId)
    assertEquals(expectedOperationIds, actualOperationIds)
  }

  private fun fetchOperationIdsForConnectionId(connectionId: UUID?): MutableSet<UUID> =
    database
      ?.query(
        { ctx: DSLContext ->
          ctx
            .selectFrom(Tables.CONNECTION_OPERATION)
            .where(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
            .fetchSet(Tables.CONNECTION_OPERATION.OPERATION_ID)
        },
      )?.toMutableSet() ?: mutableSetOf()

  @Test
  fun testActorDefinitionWorkspaceGrantExists() {
    val workspaceId = standardWorkspaces()[0]!!.workspaceId
    val definitionId = standardSourceDefinitions()[0]!!.sourceDefinitionId

    assertFalse(actorDefinitionService.actorDefinitionWorkspaceGrantExists(definitionId, workspaceId, ScopeType.WORKSPACE))

    actorDefinitionService.writeActorDefinitionWorkspaceGrant(definitionId, workspaceId, ScopeType.WORKSPACE)
    assertTrue(actorDefinitionService.actorDefinitionWorkspaceGrantExists(definitionId, workspaceId, ScopeType.WORKSPACE))

    actorDefinitionService.deleteActorDefinitionWorkspaceGrant(definitionId, workspaceId, ScopeType.WORKSPACE)
    assertFalse(actorDefinitionService.actorDefinitionWorkspaceGrantExists(definitionId, workspaceId, ScopeType.WORKSPACE))
  }

  @Test
  fun testListPublicSourceDefinitions() {
    val actualDefinitions = sourceService.listPublicSourceDefinitions(false)
    assertEquals(listOf(publicSourceDefinition()), actualDefinitions)
  }

  @Test
  fun testListWorkspaceSources() {
    val workspaceId = standardWorkspaces()[1]!!.workspaceId
    val expectedSources =
      sourceConnections()
        .filter { source: SourceConnection? -> source!!.workspaceId == workspaceId }
        .toList()
    val sources = sourceService.listWorkspaceSourceConnection(workspaceId)
    val nullCreatedAtSources =
      sources
        .map { sourceConnection: SourceConnection? -> sourceConnection!!.withCreatedAt(null).withUpdatedAt(null) }
        .toList()
    assertThat(nullCreatedAtSources)
      .hasSameElementsAs(expectedSources)
  }

  @Test
  fun testListWorkspaceDestinations() {
    val workspaceId = standardWorkspaces()[0]!!.workspaceId
    val expectedDestinations =
      destinationConnections()
        .filter { destination: DestinationConnection? -> destination!!.workspaceId == workspaceId }
        .toList()
    val destinations = destinationService.listWorkspaceDestinationConnection(workspaceId)
    val nullCreatedAtDestinations =
      destinations
        .map { destinationConnection: DestinationConnection? ->
          destinationConnection!!.withCreatedAt(null).withUpdatedAt(null)
        }.toList()
    assertThat(nullCreatedAtDestinations)
      .hasSameElementsAs(expectedDestinations)
  }

  @Test
  fun testSourceDefinitionGrants() {
    val workspaceId = standardWorkspaces()[0]!!.workspaceId
    val grantableDefinition1 = grantableSourceDefinition1()
    val grantableDefinition2 = grantableSourceDefinition2()
    val customDefinition = customSourceDefinition()

    actorDefinitionService.writeActorDefinitionWorkspaceGrant(customDefinition!!.sourceDefinitionId, workspaceId, ScopeType.WORKSPACE)
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(grantableDefinition1!!.sourceDefinitionId, workspaceId, ScopeType.WORKSPACE)
    val actualGrantedDefinitions =
      sourceService
        .listGrantedSourceDefinitions(workspaceId, false)
    assertThat<StandardSourceDefinition?>(actualGrantedDefinitions).hasSameElementsAs(
      listOf<StandardSourceDefinition?>(grantableDefinition1, customDefinition),
    )

    val actualGrantableDefinitions =
      sourceService
        .listGrantableSourceDefinitions(workspaceId, false)
    val expectedEntries =
      listOf(
        Map.entry(grantableDefinition1, true),
        Map.entry(grantableDefinition2, false),
      )
    assertThat(actualGrantableDefinitions.toList())
      .hasSameElementsAs(expectedEntries)
  }

  // todo: testSourceDefinitionGrants for organization
  @Test
  fun testListPublicDestinationDefinitions() {
    val actualDefinitions = destinationService.listPublicDestinationDefinitions(false)
    assertEquals(listOf(publicDestinationDefinition()), actualDefinitions)
  }

  @Test
  fun testDestinationDefinitionGrants() {
    val workspaceId = standardWorkspaces()[0]!!.workspaceId
    val grantableDefinition1 = grantableDestinationDefinition1()
    val grantableDefinition2 = grantableDestinationDefinition2()
    val customDefinition = customDestinationDefinition()

    actorDefinitionService.writeActorDefinitionWorkspaceGrant(customDefinition!!.destinationDefinitionId, workspaceId, ScopeType.WORKSPACE)
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(
      grantableDefinition1!!.destinationDefinitionId,
      workspaceId,
      ScopeType.WORKSPACE,
    )
    val actualGrantedDefinitions =
      destinationService
        .listGrantedDestinationDefinitions(workspaceId, false)
    assertThat<StandardDestinationDefinition?>(actualGrantedDefinitions)
      .hasSameElementsAs(
        listOf<StandardDestinationDefinition?>(grantableDefinition1, customDefinition),
      )

    val actualGrantableDefinitions =
      destinationService
        .listGrantableDestinationDefinitions(workspaceId, false)
    val expectedEntries =
      listOf(
        Map.entry(grantableDefinition1, true),
        Map.entry(grantableDefinition2, false),
      )
    org.assertj.core.api.Assertions
      .assertThat(actualGrantableDefinitions.toList())
      .hasSameElementsAs(expectedEntries)
  }

  // todo: testDestinationDefinitionGrants for organization
  @Test
  fun testWorkspaceCanUseDefinition() {
    val workspaceId = standardWorkspaces()[0]!!.workspaceId
    val otherWorkspaceId = standardWorkspaces()[1]!!.workspaceId
    val publicDefinitionId = publicSourceDefinition()!!.sourceDefinitionId
    val grantableDefinition1Id = grantableSourceDefinition1()!!.sourceDefinitionId
    val grantableDefinition2Id = grantableSourceDefinition2()!!.sourceDefinitionId
    val customDefinitionId = customSourceDefinition()!!.sourceDefinitionId

    // Can use public definitions
    assertTrue(workspaceService.workspaceCanUseDefinition(publicDefinitionId, workspaceId))

    // Can use granted definitions
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(grantableDefinition1Id, workspaceId, ScopeType.WORKSPACE)
    assertTrue(workspaceService.workspaceCanUseDefinition(grantableDefinition1Id, workspaceId))
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(customDefinitionId, workspaceId, ScopeType.WORKSPACE)
    assertTrue(workspaceService.workspaceCanUseDefinition(customDefinitionId, workspaceId))

    // Cannot use private definitions without grant
    assertFalse(workspaceService.workspaceCanUseDefinition(grantableDefinition2Id, workspaceId))

    // Cannot use other workspace's grants
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(grantableDefinition2Id, otherWorkspaceId, ScopeType.WORKSPACE)
    assertFalse(workspaceService.workspaceCanUseDefinition(grantableDefinition2Id, workspaceId))

    // Passing invalid IDs returns false
    assertFalse(workspaceService.workspaceCanUseDefinition(UUID(0L, 0L), workspaceId))

    // workspaceCanUseCustomDefinition can only be true for custom definitions
    assertTrue(workspaceService.workspaceCanUseCustomDefinition(customDefinitionId, workspaceId))
    assertFalse(workspaceService.workspaceCanUseCustomDefinition(grantableDefinition1Id, workspaceId))

    // todo: add tests for organizations
    // to test orgs, need to somehow link org to workspace
  }

  @Test
  fun testGetDestinationOAuthByDefinitionIdAndWorkspaceId() {
    val destinationOAuthParameter = destinationOauthParameters()[0]
    val result =
      oauthService.getDestinationOAuthParamByDefinitionIdOptional(
        Optional.of(destinationOAuthParameter!!.workspaceId),
        Optional.empty<UUID>(),
        destinationOAuthParameter.destinationDefinitionId,
      )
    assertTrue(result.isPresent)
    assertEquals(destinationOAuthParameter, result.get())
  }

  @Test
  fun testGetDestinationOAuthByDefinitionIdAndOrganizationId() {
    val destinationOAuthParameter = destinationOauthParameters()[2]
    val result =
      oauthService.getDestinationOAuthParamByDefinitionIdOptional(
        Optional.empty<UUID>(),
        Optional.of(destinationOAuthParameter!!.organizationId),
        destinationOAuthParameter.destinationDefinitionId,
      )
    assertTrue(result.isPresent)
    assertEquals(destinationOAuthParameter, result.get())
  }

  @Test
  fun testGetDestinationOAuthByDefinitionIdAndNullWorkspaceIdOrganizationId() {
    val destinationOAuthParameter = destinationOauthParameters()[3]
    val result =
      oauthService.getDestinationOAuthParamByDefinitionIdOptional(
        Optional.empty<UUID>(),
        Optional.empty<UUID>(),
        destinationOAuthParameter!!.destinationDefinitionId,
      )
    assertTrue(result.isPresent)
    assertEquals(destinationOAuthParameter, result.get())
  }

  @Test
  fun testMissingDestinationOAuthByDefinitionId() {
    val missingId = UUID.fromString("fc59cfa0-06de-4c8b-850b-46d4cfb65629")
    val destinationOAuthParameter = destinationOauthParameters()[0]
    var result =
      oauthService.getDestinationOAuthParamByDefinitionIdOptional(
        Optional.of(destinationOAuthParameter!!.workspaceId),
        Optional.empty<UUID>(),
        missingId,
      )
    assertFalse(result.isPresent)

    result =
      oauthService.getDestinationOAuthParamByDefinitionIdOptional(
        Optional.of(missingId),
        Optional.empty<UUID>(),
        destinationOAuthParameter.destinationDefinitionId,
      )
    assertFalse(result.isPresent)
  }

  @Test
  fun testGetSourceOAuthByDefinitionIdAndWorkspaceId() {
    val sourceOAuthParameter = sourceOauthParameters()[0]
    val result =
      oauthService.getSourceOAuthParamByDefinitionIdOptional(
        Optional.of(sourceOAuthParameter!!.workspaceId),
        Optional.empty<UUID>(),
        sourceOAuthParameter.sourceDefinitionId,
      )
    assertTrue(result.isPresent)
    assertEquals(sourceOAuthParameter, result.get())
  }

  @Test
  fun testGetSourceOAuthByDefinitionIdAndOrganizationId() {
    val sourceOAuthParameter = sourceOauthParameters()[2]
    val result =
      oauthService.getSourceOAuthParamByDefinitionIdOptional(
        Optional.empty<UUID>(),
        Optional.of(sourceOAuthParameter!!.organizationId),
        sourceOAuthParameter.sourceDefinitionId,
      )
    assertTrue(result.isPresent)
    assertEquals(sourceOAuthParameter, result.get())
  }

  @Test
  fun testGetSourceOAuthByDefinitionIdAndNullWorkspaceIdAndOrganizationId() {
    val sourceOAuthParameter = sourceOauthParameters()[3]
    val result =
      oauthService.getSourceOAuthParamByDefinitionIdOptional(
        Optional.empty<UUID>(),
        Optional.empty<UUID>(),
        sourceOAuthParameter!!.sourceDefinitionId,
      )
    assertTrue(result.isPresent)
    assertEquals(sourceOAuthParameter, result.get())
  }

  @Test
  fun testMissingSourceOAuthByDefinitionId() {
    val missingId = UUID.fromString("fc59cfa0-06de-4c8b-850b-46d4cfb65629")
    val sourceOAuthParameter = sourceOauthParameters()[0]
    var result =
      oauthService.getSourceOAuthParamByDefinitionIdOptional(
        Optional.of(sourceOAuthParameter!!.workspaceId),
        Optional.empty<UUID>(),
        missingId,
      )
    assertFalse(result.isPresent)

    result =
      oauthService.getSourceOAuthParamByDefinitionIdOptional(
        Optional.of(missingId),
        Optional.empty<UUID>(),
        sourceOAuthParameter.sourceDefinitionId,
      )
    assertFalse(result.isPresent)
  }

  @Test
  fun testGetStandardSyncUsingOperation() {
    val operationId = standardSyncOperations()[0]!!.operationId
    val expectedSyncs =
      standardSyncs()
        .subList(0, 3)
        .stream()
        .toList()
        .filterNotNull()
        .toMutableList()
    val actualSyncs = connectionService.listStandardSyncsUsingOperation(operationId).toMutableList()

    assertSyncsMatch(expectedSyncs, actualSyncs)
  }

  private fun assertSyncsMatch(
    expectedSyncs: MutableList<StandardSync>,
    actualSyncs: MutableList<StandardSync>,
  ) {
    assertEquals(expectedSyncs.size, actualSyncs.size)

    for (expected in expectedSyncs) {
      val maybeActual =
        actualSyncs.stream().filter { s: StandardSync -> s.connectionId == expected.connectionId }.findFirst()
      if (maybeActual.isEmpty) {
        fail<Any?>(
          "Expected to find connectionId ${expected.connectionId} in result, but actual connectionIds are ${actualSyncs.map { obj: StandardSync ->
            obj.connectionId
          }}",
        )
      }
      val actual = maybeActual.get()

      // operationIds can be ordered differently in the query result than in the mock data, so they need
      // to be verified separately
      // from the rest of the sync.
      assertThat(actual.operationIds)
        .hasSameElementsAs(expected.operationIds)

      // now, clear operationIds so the rest of the sync can be compared
      expected.operationIds = null
      actual.operationIds = null
      expected.createdAt = null
      actual.createdAt = null
      assertEquals(expected, actual)
    }
  }

  @Test
  fun testDeleteStandardSyncOperation() {
    val deletedOperationId = standardSyncOperations()[0]!!.operationId
    val syncs = standardSyncs()
    operationService.deleteStandardSyncOperation(deletedOperationId)

    for (sync in syncs) {
      val retrievedSync = connectionService.getStandardSync(sync!!.connectionId)
      for (operationId in sync.operationIds) {
        if (operationId == deletedOperationId) {
          assertThat(retrievedSync.operationIds)
            .doesNotContain(deletedOperationId)
        } else {
          assertThat(retrievedSync.operationIds)
            .contains(operationId)
        }
      }
    }
  }

  @Test
  fun testGetSourceAndDefinitionsFromSourceIds() {
    val sourceIds =
      sourceConnections()
        .subList(0, 2)
        .map { obj: SourceConnection? -> obj!!.sourceId }
        .toList()

    val expected =
      listOf<SourceAndDefinition?>(
        SourceAndDefinition(sourceConnections()[0]!!, standardSourceDefinitions()[0]!!),
        SourceAndDefinition(sourceConnections()[1]!!, standardSourceDefinitions()[1]!!),
      )

    val actual = sourceService.getSourceAndDefinitionsFromSourceIds(sourceIds)
    val result =
      actual
        .map { sourceAndDefinition: SourceAndDefinition? ->
          val copy = SourceAndDefinition(sourceAndDefinition!!.source, sourceAndDefinition.definition)
          copy.source.setCreatedAt(null)
          copy.source.setUpdatedAt(null)
          copy
        }.toList()

    assertThat(result)
      .hasSameElementsAs(expected)
  }

  @Test
  fun testGetDestinationAndDefinitionsFromDestinationIds() {
    val destinationIds =
      destinationConnections()
        .subList(0, 2)
        .map { obj: DestinationConnection? -> obj!!.destinationId }
        .toList()

    val actual = destinationService.getDestinationAndDefinitionsFromDestinationIds(destinationIds)

    val expected =
      listOf<DestinationAndDefinition?>(
        DestinationAndDefinition(destinationConnections()[0]!!, standardDestinationDefinitions()[0]!!),
        DestinationAndDefinition(destinationConnections()[1]!!, standardDestinationDefinitions()[1]!!),
      )

    val result =
      actual
        .map { destinationAndDefinition: DestinationAndDefinition? ->
          val copy =
            DestinationAndDefinition(destinationAndDefinition!!.destination, destinationAndDefinition.definition)
          copy.destination.createdAt = null
          copy.destination.updatedAt = null
          copy
        }.toList()

    assertThat(result)
      .hasSameElementsAs(expected)
  }

  @Test
  fun testGetMostRecentActorCatalogFetchEventForSource() {
    for (actorCatalog in actorCatalogs()) {
      writeActorCatalog(database!!, mutableListOf<ActorCatalog?>(actorCatalog))
    }

    val now = OffsetDateTime.now()
    val yesterday = now.minusDays(1L)

    val fetchEvents = actorCatalogFetchEventsSameSource()
    val fetchEvent1 = fetchEvents[0]
    val fetchEvent2 = fetchEvents[1]

    database!!.transaction(
      { ctx: DSLContext ->
        insertCatalogFetchEvent(
          ctx,
          fetchEvent1!!.actorId,
          fetchEvent1.actorCatalogId,
          yesterday,
        )
        insertCatalogFetchEvent(
          ctx,
          fetchEvent2!!.actorId,
          fetchEvent2.actorCatalogId,
          now,
        )
        // Insert a second identical copy to verify that the query can handle duplicates since the records
        // are not guaranteed to be unique.
        insertCatalogFetchEvent(
          ctx,
          fetchEvent2.actorId,
          fetchEvent2.actorCatalogId,
          now,
        )
        null
      },
    )

    val result =
      catalogService.getMostRecentActorCatalogFetchEventForSource(fetchEvent1!!.actorId)

    assertEquals(fetchEvent2!!.actorCatalogId, result.get().actorCatalogId)
  }

  @Test
  fun testGetMostRecentActorCatalogFetchEventForSources() {
    for (actorCatalog in actorCatalogs()) {
      Companion.writeActorCatalog(database!!, mutableListOf<ActorCatalog?>(actorCatalog))
    }

    database!!.transaction(
      { ctx: DSLContext ->
        actorCatalogFetchEventsForAggregationTest().forEach(
          Consumer { actorCatalogFetchEvent: MockData.ActorCatalogFetchEventWithCreationDate? ->
            insertCatalogFetchEvent(
              ctx,
              actorCatalogFetchEvent!!.actorCatalogFetchEvent!!.actorId,
              actorCatalogFetchEvent.actorCatalogFetchEvent.actorCatalogId,
              actorCatalogFetchEvent.createdAt,
            )
          },
        )
        null
      },
    )

    val result =
      catalogService.getMostRecentActorCatalogFetchEventForSources(
        listOf(
          SOURCE_ID_1,
          SOURCE_ID_2,
        ),
      )

    assertEquals(ACTOR_CATALOG_ID_1, result.get(SOURCE_ID_1)!!.actorCatalogId)
    assertEquals(ACTOR_CATALOG_ID_3, result.get(SOURCE_ID_2)!!.actorCatalogId)
    assertEquals(0, catalogService.getMostRecentActorCatalogFetchEventForSources(mutableListOf()).size)
  }

  @Test
  fun testGetMostRecentActorCatalogFetchEventWithDuplicates() {
    // Tests that we can handle two fetch events in the db with the same actor id, actor catalog id, and
    // timestamp e.g., from duplicate discoveries.
    for (actorCatalog in actorCatalogs()) {
      Companion.writeActorCatalog(database!!, mutableListOf<ActorCatalog?>(actorCatalog))
    }

    database!!.transaction(
      { ctx: DSLContext ->
        // Insert the fetch events twice.
        actorCatalogFetchEventsForAggregationTest().forEach(
          Consumer { actorCatalogFetchEvent: MockData.ActorCatalogFetchEventWithCreationDate? ->
            insertCatalogFetchEvent(
              ctx,
              actorCatalogFetchEvent!!.actorCatalogFetchEvent!!.actorId,
              actorCatalogFetchEvent.actorCatalogFetchEvent.actorCatalogId,
              actorCatalogFetchEvent.createdAt,
            )
            insertCatalogFetchEvent(
              ctx,
              actorCatalogFetchEvent.actorCatalogFetchEvent.actorId,
              actorCatalogFetchEvent.actorCatalogFetchEvent.actorCatalogId,
              actorCatalogFetchEvent.createdAt,
            )
          },
        )
        null
      },
    )

    val result =
      catalogService.getMostRecentActorCatalogFetchEventForSources(
        listOf(
          SOURCE_ID_1,
          SOURCE_ID_2,
        ),
      )

    assertEquals(ACTOR_CATALOG_ID_1, result.get(SOURCE_ID_1)!!.actorCatalogId)
    assertEquals(ACTOR_CATALOG_ID_3, result.get(SOURCE_ID_2)!!.actorCatalogId)
  }

  @Test
  fun testGetActorDefinitionsInUseToProtocolVersion() {
    val actorDefinitionIds: MutableSet<UUID?> = HashSet()
    actorDefinitionIds.addAll(sourceConnections().map { obj: SourceConnection? -> obj!!.sourceDefinitionId }.toList())
    actorDefinitionIds.addAll(
      destinationConnections()
        .map { obj: DestinationConnection? -> obj!!.destinationDefinitionId }
        .toList(),
    )
    assertEquals(actorDefinitionIds, actorDefinitionService.getActorDefinitionToProtocolVersionMap().keys)
  }

  private fun insertCatalogFetchEvent(
    ctx: DSLContext,
    sourceId: UUID?,
    catalogId: UUID?,
    creationDate: OffsetDateTime?,
  ) {
    ctx
      .insertInto(Tables.ACTOR_CATALOG_FETCH_EVENT)
      .columns(
        Tables.ACTOR_CATALOG_FETCH_EVENT.ID,
        Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID,
        Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID,
        Tables.ACTOR_CATALOG_FETCH_EVENT.CONFIG_HASH,
        Tables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_VERSION,
        Tables.ACTOR_CATALOG_FETCH_EVENT.CREATED_AT,
        Tables.ACTOR_CATALOG_FETCH_EVENT.MODIFIED_AT,
      ).values(UUID.randomUUID(), sourceId, catalogId, "", "", creationDate, creationDate)
      .execute()
  }

  @Test
  fun testGetEarlySyncJobs() {
    // This test just verifies that the query can be run against configAPI DB.
    // The query has been tested locally against prod DB to verify the outputs.
    val earlySyncJobs = connectionService.listEarlySyncJobs(7, 30)
    assertNotNull(earlySyncJobs)
  }

  companion object {
    private const val DOCKER_IMAGE_TAG = "1.2.0"
    private const val OTHER_DOCKER_IMAGE_TAG = "1.3.0"
    private const val CONFIG_HASH = "ConfigHash"
    private val DATAPLANE_GROUP_ID: UUID = UUID.randomUUID()

    private fun writeActorCatalog(
      database: Database,
      configs: MutableList<ActorCatalog?>,
    ) {
      database.transaction(
        { ctx: DSLContext ->
          Companion.writeActorCatalog(configs, ctx)
          null
        },
      )
    }

    private fun writeActorCatalog(
      configs: MutableList<ActorCatalog?>,
      ctx: DSLContext,
    ) {
      val timestamp = OffsetDateTime.now()
      configs.forEach(
        Consumer { actorCatalog: ActorCatalog? ->
          val isExistingConfig =
            ctx.fetchExists(
              DSL
                .select()
                .from(Tables.ACTOR_CATALOG)
                .where(Tables.ACTOR_CATALOG.ID.eq(actorCatalog!!.id)),
            )
          if (isExistingConfig) {
            ctx
              .update(Tables.ACTOR_CATALOG)
              .set(Tables.ACTOR_CATALOG.CATALOG, JSONB.valueOf(serialize<JsonNode?>(actorCatalog.catalog)))
              .set(Tables.ACTOR_CATALOG.CATALOG_HASH, actorCatalog.catalogHash)
              .set(
                Tables.ACTOR_CATALOG.CATALOG_TYPE,
                ActorCatalogType.valueOf(actorCatalog.catalogType.toString()),
              ).set(Tables.ACTOR_CATALOG.MODIFIED_AT, timestamp)
              .where(Tables.ACTOR_CATALOG.ID.eq(actorCatalog.id))
              .execute()
          } else {
            ctx
              .insertInto(Tables.ACTOR_CATALOG)
              .set(Tables.ACTOR_CATALOG.ID, actorCatalog.id)
              .set(Tables.ACTOR_CATALOG.CATALOG, JSONB.valueOf(serialize<JsonNode?>(actorCatalog.catalog)))
              .set(Tables.ACTOR_CATALOG.CATALOG_HASH, actorCatalog.catalogHash)
              .set(
                Tables.ACTOR_CATALOG.CATALOG_TYPE,
                ActorCatalogType.valueOf(actorCatalog.catalogType.toString()),
              ).set(Tables.ACTOR_CATALOG.CREATED_AT, timestamp)
              .set(Tables.ACTOR_CATALOG.MODIFIED_AT, timestamp)
              .execute()
          }
        },
      )
    }
  }
}
