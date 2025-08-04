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
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.MockData.ActorCatalogFetchEventWithCreationDate
import io.airbyte.config.persistence.MockData.actorCatalogFetchEventsForAggregationTest
import io.airbyte.config.persistence.MockData.actorCatalogFetchEventsSameSource
import io.airbyte.config.persistence.MockData.actorCatalogs
import io.airbyte.config.persistence.MockData.actorDefinitionVersion
import io.airbyte.config.persistence.MockData.customDestinationDefinition
import io.airbyte.config.persistence.MockData.customSourceDefinition
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
import io.airbyte.data.ConfigNotFoundException
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
import io.airbyte.db.ContextQueryFunction
import io.airbyte.db.Database
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_WORKSPACE_GRANT
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorCatalogType
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorCatalogFetchEventRecord
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorCatalogRecord
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
import io.airbyte.validation.json.JsonValidationException
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.spy
import java.io.IOException
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util.Map
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer
import java.util.stream.Collectors
import org.mockito.Mockito.`when` as whenever

/**
 * The tests in this class should be moved into separate test suites grouped by resource. Do NOT add
 * new tests here. Add them to resource based test suites (e.g. WorkspacePersistenceTest). If one
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
  @Throws(IOException::class, JsonValidationException::class, SQLException::class)
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
    val defaultOrg = MockData.defaultOrganization()
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

    for (workspace in MockData.standardWorkspaces()) {
      workspaceService.writeStandardWorkspaceNoSecrets(workspace!!)
    }

    for (sourceDefinition in MockData.standardSourceDefinitions()) {
      val actorDefinitionVersion =
        MockData
          .actorDefinitionVersion()!!
          .withActorDefinitionId(sourceDefinition!!.sourceDefinitionId)
          .withVersionId(sourceDefinition.defaultVersionId)
      sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, emptyList())
    }

    for (destinationDefinition in MockData.standardDestinationDefinitions()) {
      val actorDefinitionVersion =
        MockData
          .actorDefinitionVersion()!!
          .withActorDefinitionId(destinationDefinition!!.destinationDefinitionId)
          .withVersionId(destinationDefinition.defaultVersionId)
      destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, emptyList())
    }

    for (source in MockData.sourceConnections()) {
      sourceService.writeSourceConnectionNoSecrets(source!!)
    }

    for (destination in MockData.destinationConnections()) {
      destinationService.writeDestinationConnectionNoSecrets(destination!!)
    }

    for (operation in MockData.standardSyncOperations()) {
      operationService.writeStandardSyncOperation(operation!!)
    }

    for (sync in MockData.standardSyncs()) {
      connectionService.writeStandardSync(sync!!)
    }

    for (oAuthParameter in MockData.sourceOauthParameters()) {
      oauthService.writeSourceOAuthParam(oAuthParameter!!)
    }

    for (oAuthParameter in MockData.destinationOauthParameters()) {
      oauthService.writeDestinationOAuthParam(oAuthParameter!!)
    }

    database?.transaction(
      ContextQueryFunction { ctx: DSLContext ->
        ctx.truncate(ACTOR_DEFINITION_WORKSPACE_GRANT).execute()
      },
    )
  }

  @Test
  @Throws(IOException::class)
  fun testWorkspaceCountConnections() {
    val workspaceId = standardWorkspaces().get(0)!!.getWorkspaceId()
    Assertions.assertEquals(3, workspaceService.countConnectionsForWorkspace(workspaceId))
    Assertions.assertEquals(2, workspaceService.countDestinationsForWorkspace(workspaceId))
    Assertions.assertEquals(2, workspaceService.countSourcesForWorkspace(workspaceId))
  }

  @Test
  @Throws(IOException::class)
  fun testWorkspaceCountConnectionsDeprecated() {
    val workspaceId = standardWorkspaces().get(1)!!.getWorkspaceId()
    Assertions.assertEquals(1, workspaceService.countConnectionsForWorkspace(workspaceId))
  }

  @Test
  @Throws(IOException::class)
  fun testFetchActorsUsingDefinition() {
    val destinationDefinitionId = publicDestinationDefinition()!!.getDestinationDefinitionId()
    val sourceDefinitionId = publicSourceDefinition()!!.getSourceDefinitionId()
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
          destinationConnection!!.withCreatedAt(null).withUpdatedAt(null)
        }

    val nullCreatedAtSourceConnections =
      sourceConnections
        .map { sourceConnection -> sourceConnection!!.withCreatedAt(null).withUpdatedAt(null) }

    org.assertj.core.api.Assertions
      .assertThat<DestinationConnection?>(nullCreatedAtDestinationConnections)
      .containsExactlyElementsOf(
        destinationConnections()
          .stream()
          .filter { d: DestinationConnection? -> d!!.getDestinationDefinitionId() == destinationDefinitionId && !d.getTombstone() }
          .collect(
            Collectors.toList(),
          ),
      )
    org.assertj.core.api.Assertions
      .assertThat<SourceConnection?>(nullCreatedAtSourceConnections)
      .containsExactlyElementsOf(
        sourceConnections()
          .stream()
          .filter { d: SourceConnection? -> d!!.getSourceDefinitionId() == sourceDefinitionId && !d.getTombstone() }
          .collect(
            Collectors.toList(),
          ),
      )
  }

  @Test
  @Throws(IOException::class)
  fun testReadActorCatalog() {
    val otherConfigHash = "OtherConfigHash"
    val workspace = standardWorkspaces().get(0)

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withSourceType(StandardSourceDefinition.SourceType.DATABASE)
        .withName("sourceDefinition")
    val actorDefinitionVersion =
      actorDefinitionVersion()!!
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersionId(sourceDefinition.getDefaultVersionId())
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf<ActorDefinitionBreakingChange>())

    val source =
      SourceConnection()
        .withSourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withSourceId(UUID.randomUUID())
        .withName("SomeConnector")
        .withWorkspaceId(workspace!!.getWorkspaceId())
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
    catalogService.writeActorCatalogWithFetchEvent(firstCatalog, source.getSourceId(), DOCKER_IMAGE_TAG, CONFIG_HASH)

    val secondCatalog =
      CatalogHelpers.createAirbyteCatalog(
        "product",
        Field.of("size", JsonSchemaType.NUMBER),
        Field.of("label", JsonSchemaType.STRING),
        Field.of("color", JsonSchemaType.STRING),
        Field.of("price", JsonSchemaType.NUMBER),
      )
    catalogService.writeActorCatalogWithFetchEvent(secondCatalog, source.getSourceId(), DOCKER_IMAGE_TAG, otherConfigHash)

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

    val catalogResult = catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, CONFIG_HASH)
    Assertions.assertTrue(catalogResult.isPresent())
    Assertions.assertEquals(
      Jsons.deserialize(expectedCatalog, AirbyteCatalog::class.java),
      Jsons.`object`(catalogResult.get()!!.getCatalog(), AirbyteCatalog::class.java),
    )
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, SQLException::class)
  fun testWriteCanonicalHashActorCatalog() {
    val canonicalConfigHash = "8ad32981"
    val workspace = standardWorkspaces().get(0)

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withSourceType(StandardSourceDefinition.SourceType.DATABASE)
        .withName("sourceDefinition")
    val actorDefinitionVersion =
      actorDefinitionVersion()!!
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersionId(sourceDefinition.getDefaultVersionId())
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf<ActorDefinitionBreakingChange>())

    val source =
      SourceConnection()
        .withSourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withSourceId(UUID.randomUUID())
        .withName("SomeConnector")
        .withWorkspaceId(workspace!!.getWorkspaceId())
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
    catalogService.writeActorCatalogWithFetchEvent(firstCatalog, source.getSourceId(), DOCKER_IMAGE_TAG, CONFIG_HASH)

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

    val catalogResult = catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, CONFIG_HASH)
    Assertions.assertTrue(catalogResult.isPresent())
    Assertions.assertEquals(catalogResult.get().getCatalogHash(), canonicalConfigHash)
    Assertions.assertEquals(expectedCatalog, canonicalJsonSerialize(catalogResult.get().getCatalog()))
  }

  @Test
  @Throws(IOException::class, SQLException::class)
  fun testSimpleInsertActorCatalog() {
    val otherConfigHash = "OtherConfigHash"
    val workspace = standardWorkspaces().get(0)

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withSourceType(StandardSourceDefinition.SourceType.DATABASE)
        .withName("sourceDefinition")
    val actorDefinitionVersion =
      actorDefinitionVersion()!!
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersionId(sourceDefinition.getDefaultVersionId())
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf<ActorDefinitionBreakingChange>())

    val source =
      SourceConnection()
        .withSourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withSourceId(UUID.randomUUID())
        .withName("SomeConnector")
        .withWorkspaceId(workspace!!.getWorkspaceId())
        .withConfiguration(deserialize("{}"))
    sourceService.writeSourceConnectionNoSecrets(source)

    val actorCatalog = CatalogHelpers.createAirbyteCatalog("clothes", Field.of("name", JsonSchemaType.STRING))
    val expectedActorCatalog = CatalogHelpers.createAirbyteCatalog("clothes", Field.of("name", JsonSchemaType.STRING))
    catalogService.writeActorCatalogWithFetchEvent(
      actorCatalog,
      source.getSourceId(),
      DOCKER_IMAGE_TAG,
      CONFIG_HASH,
    )

    val catalog =
      catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, CONFIG_HASH)
    Assertions.assertTrue(catalog.isPresent())
    Assertions.assertEquals(expectedActorCatalog, Jsons.`object`(catalog.get()!!.getCatalog(), AirbyteCatalog::class.java))
    Assertions.assertFalse(catalogService.getActorCatalog(source.getSourceId(), OTHER_DOCKER_IMAGE_TAG, CONFIG_HASH).isPresent())
    Assertions.assertFalse(catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, otherConfigHash).isPresent())

    catalogService.writeActorCatalogWithFetchEvent(actorCatalog, source.getSourceId(), OTHER_DOCKER_IMAGE_TAG, CONFIG_HASH)
    val catalogNewConnectorVersion =
      catalogService.getActorCatalog(source.getSourceId(), OTHER_DOCKER_IMAGE_TAG, CONFIG_HASH)
    Assertions.assertTrue(catalogNewConnectorVersion.isPresent())
    Assertions.assertEquals(
      expectedActorCatalog,
      Jsons.`object`(catalogNewConnectorVersion.get()!!.getCatalog(), AirbyteCatalog::class.java),
    )

    catalogService.writeActorCatalogWithFetchEvent(actorCatalog, source.getSourceId(), DOCKER_IMAGE_TAG, otherConfigHash)
    val catalogNewConfig =
      catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, otherConfigHash)
    Assertions.assertTrue(catalogNewConfig.isPresent())
    Assertions.assertEquals(
      expectedActorCatalog,
      Jsons.`object`(catalogNewConfig.get()!!.getCatalog(), AirbyteCatalog::class.java),
    )

    val catalogDbEntry =
      database
        ?.query(
          ContextQueryFunction { ctx: org.jooq.DSLContext ->
            ctx.selectCount().from(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_CATALOG)
          },
        )?.fetchOne()!!
        .into(Int::class.javaPrimitiveType)
    Assertions.assertEquals(1, catalogDbEntry)

    // Writing the previous catalog with v1 data types
    catalogService.writeActorCatalogWithFetchEvent(expectedActorCatalog, source.getSourceId(), DOCKER_IMAGE_TAG, otherConfigHash)
    val catalogV1NewConfig =
      catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, otherConfigHash)
    Assertions.assertTrue(catalogV1NewConfig.isPresent())
    Assertions.assertEquals(
      expectedActorCatalog,
      Jsons.`object`(catalogNewConfig.get()!!.getCatalog(), AirbyteCatalog::class.java),
    )

    catalogService.writeActorCatalogWithFetchEvent(expectedActorCatalog, source.getSourceId(), "1.4.0", otherConfigHash)
    val catalogV1again =
      catalogService.getActorCatalog(source.getSourceId(), DOCKER_IMAGE_TAG, otherConfigHash)
    Assertions.assertTrue(catalogV1again.isPresent())
    Assertions.assertEquals(
      expectedActorCatalog,
      Jsons.`object`(catalogNewConfig.get()!!.getCatalog(), AirbyteCatalog::class.java),
    )

    val catalogDbEntry2 =
      database
        ?.query(
          ContextQueryFunction { ctx: org.jooq.DSLContext ->
            ctx.selectCount().from(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_CATALOG)
          },
        )?.fetchOne()!!
        .into(Int::class.javaPrimitiveType)
    // TODO this should be 2 once we re-enable datatypes v1
    Assertions.assertEquals(1, catalogDbEntry2)
  }

  @Test
  @Throws(IOException::class)
  fun testSimpleInsertDestinationActorCatalog() {
    val otherConfigHash = "OtherConfigHash"
    val workspace = standardWorkspaces().get(0)

    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withName("destinationDefinition")
    val actorDefinitionVersion =
      actorDefinitionVersion()!!
        .withActorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .withVersionId(destinationDefinition.getDefaultVersionId())
    destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, mutableListOf<ActorDefinitionBreakingChange>())

    val destination =
      DestinationConnection()
        .withDestinationDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .withDestinationId(UUID.randomUUID())
        .withName("SomeDestinationConnector")
        .withWorkspaceId(workspace!!.getWorkspaceId())
        .withConfiguration(deserialize("{}"))
    destinationService.writeDestinationConnectionNoSecrets(destination)

    val destinationCatalog =
      DestinationCatalog().withOperations(
        listOf<DestinationOperation?>(
          DestinationOperation().withObjectName("test_object").withSyncMode(DestinationSyncMode.APPEND).withJsonSchema(emptyObject()),
        ),
      )
    catalogService.writeActorCatalogWithFetchEvent(destinationCatalog, destination.getDestinationId(), DOCKER_IMAGE_TAG, CONFIG_HASH)

    val catalog =
      catalogService.getActorCatalog(destination.getDestinationId(), DOCKER_IMAGE_TAG, CONFIG_HASH)
    Assertions.assertTrue(catalog.isPresent())
    Assertions.assertEquals(destinationCatalog, Jsons.`object`(catalog.get()!!.getCatalog(), DestinationCatalog::class.java))
    Assertions.assertFalse(catalogService.getActorCatalog(destination.getDestinationId(), OTHER_DOCKER_IMAGE_TAG, CONFIG_HASH).isPresent())
    Assertions.assertFalse(catalogService.getActorCatalog(destination.getDestinationId(), DOCKER_IMAGE_TAG, otherConfigHash).isPresent())

    catalogService.writeActorCatalogWithFetchEvent(destinationCatalog, destination.getDestinationId(), OTHER_DOCKER_IMAGE_TAG, CONFIG_HASH)
    val catalogNewConnectorVersion =
      catalogService.getActorCatalog(destination.getDestinationId(), OTHER_DOCKER_IMAGE_TAG, CONFIG_HASH)
    Assertions.assertTrue(catalogNewConnectorVersion.isPresent())
    Assertions.assertEquals(
      destinationCatalog,
      Jsons.`object`(catalogNewConnectorVersion.get()!!.getCatalog(), DestinationCatalog::class.java),
    )

    catalogService.writeActorCatalogWithFetchEvent(destinationCatalog, destination.getDestinationId(), DOCKER_IMAGE_TAG, otherConfigHash)
    val catalogNewConfig =
      catalogService.getActorCatalog(destination.getDestinationId(), DOCKER_IMAGE_TAG, otherConfigHash)
    Assertions.assertTrue(catalogNewConfig.isPresent())
    Assertions.assertEquals(
      destinationCatalog,
      Jsons.`object`(catalogNewConfig.get()!!.getCatalog(), DestinationCatalog::class.java),
    )

    val catalogDbEntry =
      database
        ?.query(
          ContextQueryFunction { ctx: org.jooq.DSLContext ->
            ctx.selectCount().from(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_CATALOG)
          },
        )?.fetchOne()!!
        .into(Int::class.javaPrimitiveType)
    Assertions.assertEquals(1, catalogDbEntry)
  }

  @Test
  @Throws(IOException::class)
  fun testListWorkspaceStandardSyncAll() {
    val expectedSyncs = standardSyncs().subList(0, 4).filterNotNull().toMutableList()
    val actualSyncs =
      connectionService
        .listWorkspaceStandardSyncs(
          standardWorkspaces().get(0)!!.getWorkspaceId(),
          true,
        ).filterNotNull()
        .toMutableList()

    assertSyncsMatch(expectedSyncs, actualSyncs)
  }

  @Test
  @Throws(IOException::class)
  fun testListWorkspaceStandardSyncWithAllFiltering() {
    val workspaceId = standardWorkspaces().get(0)!!.getWorkspaceId()
    val query = StandardSyncQuery(workspaceId, listOf(MockData.SOURCE_ID_1), listOf(MockData.DESTINATION_ID_1), false)
    val expectedSyncs =
      standardSyncs()
        .subList(0, 3)
        .stream()
        .filter { sync: StandardSync? -> query.destinationId!!.contains(sync!!.getDestinationId()) }
        .filter { sync: StandardSync? -> query.sourceId!!.contains(sync!!.getSourceId()) }
        .toList()
        .filterNotNull()
        .toMutableList()
    val actualSyncs = connectionService.listWorkspaceStandardSyncs(query).filterNotNull().toMutableList()

    assertSyncsMatch(expectedSyncs, actualSyncs)
  }

  @Test
  @Throws(IOException::class)
  fun testListWorkspaceStandardSyncDestinationFiltering() {
    val workspaceId = standardWorkspaces().get(0)!!.getWorkspaceId()
    val query = StandardSyncQuery(workspaceId, null, listOf(MockData.DESTINATION_ID_1), false)
    val expectedSyncs =
      standardSyncs()
        .subList(0, 3)
        .stream()
        .filter { sync: StandardSync? -> query.destinationId!!.contains(sync!!.getDestinationId()) }
        .toList()
        .filterNotNull()
        .toMutableList()
    val actualSyncs = connectionService.listWorkspaceStandardSyncs(query).filterNotNull().toMutableList()

    assertSyncsMatch(expectedSyncs, actualSyncs)
  }

  @Test
  @Throws(IOException::class)
  fun testListWorkspaceStandardSyncSourceFiltering() {
    val workspaceId = standardWorkspaces().get(0)!!.getWorkspaceId()
    val query = StandardSyncQuery(workspaceId, listOf(MockData.SOURCE_ID_2), null, false)
    val expectedSyncs =
      standardSyncs()
        .subList(0, 3)
        .stream()
        .filter { sync: StandardSync? -> query.sourceId!!.contains(sync!!.getSourceId()) }
        .toList()
        .filterNotNull()
        .toMutableList()
    val actualSyncs = connectionService.listWorkspaceStandardSyncs(query).filterNotNull().toMutableList()

    assertSyncsMatch(expectedSyncs, actualSyncs)
  }

  @Test
  @Throws(IOException::class)
  fun testListWorkspaceStandardSyncExcludeDeleted() {
    val expectedSyncs =
      standardSyncs()
        .subList(0, 3)
        .stream()
        .toList()
        .filterNotNull()
        .toMutableList()
    val actualSyncs =
      connectionService.listWorkspaceStandardSyncs(standardWorkspaces().get(0)!!.getWorkspaceId(), false).filterNotNull().toMutableList()

    assertSyncsMatch(expectedSyncs, actualSyncs)
  }

  @Test
  @Throws(IOException::class)
  fun testGetWorkspaceBySlug() {
    val workspace =
      standardWorkspaces().get(0)

    val tombstonedWorkspace =
      standardWorkspaces().get(2)
    val retrievedWorkspace = workspaceService.getWorkspaceBySlugOptional(workspace!!.getSlug(), false)
    val retrievedTombstonedWorkspaceNoTombstone =
      workspaceService.getWorkspaceBySlugOptional(tombstonedWorkspace!!.getSlug(), false)
    val retrievedTombstonedWorkspace =
      workspaceService.getWorkspaceBySlugOptional(tombstonedWorkspace.getSlug(), true)

    Assertions.assertTrue(retrievedWorkspace.isPresent())

    org.assertj.core.api.Assertions
      .assertThat<StandardWorkspace?>(retrievedWorkspace.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(workspace)

    Assertions.assertFalse(retrievedTombstonedWorkspaceNoTombstone.isPresent())
    Assertions.assertTrue(retrievedTombstonedWorkspace.isPresent())

    org.assertj.core.api.Assertions
      .assertThat<StandardWorkspace?>(retrievedTombstonedWorkspace.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(tombstonedWorkspace)
  }

  @Test
  @Throws(Exception::class)
  fun testUpdateConnectionOperationIds() {
    val sync = standardSyncs().get(0)
    val existingOperationIds = sync!!.getOperationIds()
    val connectionId = sync.getConnectionId()

    // this test only works as intended when there are multiple operationIds
    Assertions.assertTrue(existingOperationIds.size > 1)

    // first, remove all associated operations
    var expectedOperationIds = mutableSetOf<UUID>()
    operationService.updateConnectionOperationIds(connectionId, expectedOperationIds)
    var actualOperationIds = fetchOperationIdsForConnectionId(connectionId)
    Assertions.assertEquals(expectedOperationIds, actualOperationIds)

    // now, add back one operation
    expectedOperationIds = mutableSetOf<UUID>(existingOperationIds.get(0))
    operationService.updateConnectionOperationIds(connectionId, expectedOperationIds)
    actualOperationIds = fetchOperationIdsForConnectionId(connectionId)
    Assertions.assertEquals(expectedOperationIds, actualOperationIds)

    // finally, remove the first operation while adding back in the rest
    expectedOperationIds =
      existingOperationIds
        .stream()
        .skip(1)
        .collect(Collectors.toSet())
        .toMutableSet()
    operationService.updateConnectionOperationIds(connectionId, expectedOperationIds)
    actualOperationIds = fetchOperationIdsForConnectionId(connectionId)
    Assertions.assertEquals(expectedOperationIds, actualOperationIds)
  }

  @Throws(SQLException::class)
  private fun fetchOperationIdsForConnectionId(connectionId: UUID?): MutableSet<UUID> =
    database
      ?.query(
        ContextQueryFunction { ctx: DSLContext ->
          ctx
            .selectFrom(Tables.CONNECTION_OPERATION)
            .where(Tables.CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
            .fetchSet(Tables.CONNECTION_OPERATION.OPERATION_ID)
        },
      )?.toMutableSet() ?: mutableSetOf()

  @Test
  @Throws(IOException::class)
  fun testActorDefinitionWorkspaceGrantExists() {
    val workspaceId = standardWorkspaces().get(0)!!.getWorkspaceId()
    val definitionId = standardSourceDefinitions().get(0)!!.getSourceDefinitionId()

    Assertions.assertFalse(actorDefinitionService.actorDefinitionWorkspaceGrantExists(definitionId, workspaceId, ScopeType.WORKSPACE))

    actorDefinitionService.writeActorDefinitionWorkspaceGrant(definitionId, workspaceId, ScopeType.WORKSPACE)
    Assertions.assertTrue(actorDefinitionService.actorDefinitionWorkspaceGrantExists(definitionId, workspaceId, ScopeType.WORKSPACE))

    actorDefinitionService.deleteActorDefinitionWorkspaceGrant(definitionId, workspaceId, ScopeType.WORKSPACE)
    Assertions.assertFalse(actorDefinitionService.actorDefinitionWorkspaceGrantExists(definitionId, workspaceId, ScopeType.WORKSPACE))
  }

  @Test
  @Throws(IOException::class)
  fun testListPublicSourceDefinitions() {
    val actualDefinitions = sourceService.listPublicSourceDefinitions(false)
    Assertions.assertEquals(listOf<StandardSourceDefinition?>(publicSourceDefinition()), actualDefinitions)
  }

  @Test
  @Throws(IOException::class)
  fun testListWorkspaceSources() {
    val workspaceId = standardWorkspaces().get(1)!!.getWorkspaceId()
    val expectedSources =
      sourceConnections()
        .filter { source: SourceConnection? -> source!!.getWorkspaceId() == workspaceId }
        .toList()
    val sources = sourceService.listWorkspaceSourceConnection(workspaceId)
    val nullCreatedAtSources =
      sources
        .stream()
        .map<SourceConnection?> { sourceConnection: SourceConnection? -> sourceConnection!!.withCreatedAt(null).withUpdatedAt(null) }
        .toList()
    org.assertj.core.api.Assertions
      .assertThat<SourceConnection?>(nullCreatedAtSources)
      .hasSameElementsAs(expectedSources)
  }

  @Test
  @Throws(IOException::class)
  fun testListWorkspaceDestinations() {
    val workspaceId = standardWorkspaces().get(0)!!.getWorkspaceId()
    val expectedDestinations =
      destinationConnections()
        .filter { destination: DestinationConnection? -> destination!!.getWorkspaceId() == workspaceId }
        .toList()
    val destinations = destinationService.listWorkspaceDestinationConnection(workspaceId)
    val nullCreatedAtDestinations =
      destinations
        .stream()
        .map<DestinationConnection?> { destinationConnection: DestinationConnection? ->
          destinationConnection!!.withCreatedAt(null).withUpdatedAt(null)
        }.toList()
    org.assertj.core.api.Assertions
      .assertThat<DestinationConnection?>(nullCreatedAtDestinations)
      .hasSameElementsAs(expectedDestinations)
  }

  @Test
  @Throws(IOException::class)
  fun testSourceDefinitionGrants() {
    val workspaceId = standardWorkspaces().get(0)!!.getWorkspaceId()
    val grantableDefinition1 = grantableSourceDefinition1()
    val grantableDefinition2 = grantableSourceDefinition2()
    val customDefinition = customSourceDefinition()

    actorDefinitionService.writeActorDefinitionWorkspaceGrant(customDefinition!!.getSourceDefinitionId(), workspaceId, ScopeType.WORKSPACE)
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(grantableDefinition1!!.getSourceDefinitionId(), workspaceId, ScopeType.WORKSPACE)
    val actualGrantedDefinitions =
      sourceService
        .listGrantedSourceDefinitions(workspaceId, false)
    org.assertj.core.api.Assertions.assertThat<StandardSourceDefinition?>(actualGrantedDefinitions).hasSameElementsAs(
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
    org.assertj.core.api.Assertions
      .assertThat(actualGrantableDefinitions.toList())
      .hasSameElementsAs(expectedEntries)
  }

  // todo: testSourceDefinitionGrants for organization
  @Test
  @Throws(IOException::class)
  fun testListPublicDestinationDefinitions() {
    val actualDefinitions = destinationService.listPublicDestinationDefinitions(false)
    Assertions.assertEquals(listOf(publicDestinationDefinition()), actualDefinitions)
  }

  @Test
  @Throws(IOException::class)
  fun testDestinationDefinitionGrants() {
    val workspaceId = standardWorkspaces().get(0)!!.getWorkspaceId()
    val grantableDefinition1 = grantableDestinationDefinition1()
    val grantableDefinition2 = grantableDestinationDefinition2()
    val customDefinition = customDestinationDefinition()

    actorDefinitionService.writeActorDefinitionWorkspaceGrant(customDefinition!!.getDestinationDefinitionId(), workspaceId, ScopeType.WORKSPACE)
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(
      grantableDefinition1!!.getDestinationDefinitionId(),
      workspaceId,
      ScopeType.WORKSPACE,
    )
    val actualGrantedDefinitions =
      destinationService
        .listGrantedDestinationDefinitions(workspaceId, false)
    org.assertj.core.api.Assertions
      .assertThat<StandardDestinationDefinition?>(actualGrantedDefinitions)
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
  @Throws(IOException::class)
  fun testWorkspaceCanUseDefinition() {
    val workspaceId = standardWorkspaces().get(0)!!.getWorkspaceId()
    val otherWorkspaceId = standardWorkspaces().get(1)!!.getWorkspaceId()
    val publicDefinitionId = publicSourceDefinition()!!.getSourceDefinitionId()
    val grantableDefinition1Id = grantableSourceDefinition1()!!.getSourceDefinitionId()
    val grantableDefinition2Id = grantableSourceDefinition2()!!.getSourceDefinitionId()
    val customDefinitionId = customSourceDefinition()!!.getSourceDefinitionId()

    // Can use public definitions
    Assertions.assertTrue(workspaceService.workspaceCanUseDefinition(publicDefinitionId, workspaceId))

    // Can use granted definitions
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(grantableDefinition1Id, workspaceId, ScopeType.WORKSPACE)
    Assertions.assertTrue(workspaceService.workspaceCanUseDefinition(grantableDefinition1Id, workspaceId))
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(customDefinitionId, workspaceId, ScopeType.WORKSPACE)
    Assertions.assertTrue(workspaceService.workspaceCanUseDefinition(customDefinitionId, workspaceId))

    // Cannot use private definitions without grant
    Assertions.assertFalse(workspaceService.workspaceCanUseDefinition(grantableDefinition2Id, workspaceId))

    // Cannot use other workspace's grants
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(grantableDefinition2Id, otherWorkspaceId, ScopeType.WORKSPACE)
    Assertions.assertFalse(workspaceService.workspaceCanUseDefinition(grantableDefinition2Id, workspaceId))

    // Passing invalid IDs returns false
    Assertions.assertFalse(workspaceService.workspaceCanUseDefinition(UUID(0L, 0L), workspaceId))

    // workspaceCanUseCustomDefinition can only be true for custom definitions
    Assertions.assertTrue(workspaceService.workspaceCanUseCustomDefinition(customDefinitionId, workspaceId))
    Assertions.assertFalse(workspaceService.workspaceCanUseCustomDefinition(grantableDefinition1Id, workspaceId))

    // todo: add tests for organizations
    // to test orgs, need to somehow link org to workspace
  }

  @Test
  @Throws(IOException::class)
  fun testGetDestinationOAuthByDefinitionIdAndWorkspaceId() {
    val destinationOAuthParameter = destinationOauthParameters().get(0)
    val result =
      oauthService.getDestinationOAuthParamByDefinitionIdOptional(
        Optional.of<UUID>(destinationOAuthParameter!!.getWorkspaceId()),
        Optional.empty<UUID>(),
        destinationOAuthParameter.getDestinationDefinitionId(),
      )
    Assertions.assertTrue(result.isPresent())
    Assertions.assertEquals(destinationOAuthParameter, result.get())
  }

  @Test
  @Throws(IOException::class)
  fun testGetDestinationOAuthByDefinitionIdAndOrganizationId() {
    val destinationOAuthParameter = destinationOauthParameters().get(2)
    val result =
      oauthService.getDestinationOAuthParamByDefinitionIdOptional(
        Optional.empty<UUID>(),
        Optional.of<UUID>(destinationOAuthParameter!!.getOrganizationId()),
        destinationOAuthParameter.getDestinationDefinitionId(),
      )
    Assertions.assertTrue(result.isPresent())
    Assertions.assertEquals(destinationOAuthParameter, result.get())
  }

  @Test
  @Throws(IOException::class)
  fun testGetDestinationOAuthByDefinitionIdAndNullWorkspaceIdOrganizationId() {
    val destinationOAuthParameter = destinationOauthParameters().get(3)
    val result =
      oauthService.getDestinationOAuthParamByDefinitionIdOptional(
        Optional.empty<UUID>(),
        Optional.empty<UUID>(),
        destinationOAuthParameter!!.getDestinationDefinitionId(),
      )
    Assertions.assertTrue(result.isPresent())
    Assertions.assertEquals(destinationOAuthParameter, result.get())
  }

  @Test
  @Throws(IOException::class)
  fun testMissingDestinationOAuthByDefinitionId() {
    val missingId = UUID.fromString("fc59cfa0-06de-4c8b-850b-46d4cfb65629")
    val destinationOAuthParameter = destinationOauthParameters().get(0)
    var result =
      oauthService.getDestinationOAuthParamByDefinitionIdOptional(
        Optional.of<UUID>(destinationOAuthParameter!!.getWorkspaceId()),
        Optional.empty<UUID>(),
        missingId,
      )
    Assertions.assertFalse(result.isPresent())

    result =
      oauthService.getDestinationOAuthParamByDefinitionIdOptional(
        Optional.of<UUID>(missingId),
        Optional.empty<UUID>(),
        destinationOAuthParameter.getDestinationDefinitionId(),
      )
    Assertions.assertFalse(result.isPresent())
  }

  @Test
  @Throws(IOException::class)
  fun testGetSourceOAuthByDefinitionIdAndWorkspaceId() {
    val sourceOAuthParameter = sourceOauthParameters().get(0)
    val result =
      oauthService.getSourceOAuthParamByDefinitionIdOptional(
        Optional.of<UUID>(sourceOAuthParameter!!.getWorkspaceId()),
        Optional.empty<UUID>(),
        sourceOAuthParameter.getSourceDefinitionId(),
      )
    Assertions.assertTrue(result.isPresent())
    Assertions.assertEquals(sourceOAuthParameter, result.get())
  }

  @Test
  @Throws(IOException::class)
  fun testGetSourceOAuthByDefinitionIdAndOrganizationId() {
    val sourceOAuthParameter = sourceOauthParameters().get(2)
    val result =
      oauthService.getSourceOAuthParamByDefinitionIdOptional(
        Optional.empty<UUID>(),
        Optional.of<UUID>(sourceOAuthParameter!!.getOrganizationId()),
        sourceOAuthParameter.getSourceDefinitionId(),
      )
    Assertions.assertTrue(result.isPresent())
    Assertions.assertEquals(sourceOAuthParameter, result.get())
  }

  @Test
  @Throws(IOException::class)
  fun testGetSourceOAuthByDefinitionIdAndNullWorkspaceIdAndOrganizationId() {
    val sourceOAuthParameter = sourceOauthParameters().get(3)
    val result =
      oauthService.getSourceOAuthParamByDefinitionIdOptional(
        Optional.empty<UUID>(),
        Optional.empty<UUID>(),
        sourceOAuthParameter!!.getSourceDefinitionId(),
      )
    Assertions.assertTrue(result.isPresent())
    Assertions.assertEquals(sourceOAuthParameter, result.get())
  }

  @Test
  @Throws(IOException::class)
  fun testMissingSourceOAuthByDefinitionId() {
    val missingId = UUID.fromString("fc59cfa0-06de-4c8b-850b-46d4cfb65629")
    val sourceOAuthParameter = sourceOauthParameters().get(0)
    var result =
      oauthService.getSourceOAuthParamByDefinitionIdOptional(
        Optional.of<UUID>(sourceOAuthParameter!!.getWorkspaceId()),
        Optional.empty<UUID>(),
        missingId,
      )
    Assertions.assertFalse(result.isPresent())

    result =
      oauthService.getSourceOAuthParamByDefinitionIdOptional(
        Optional.of<UUID>(missingId),
        Optional.empty<UUID>(),
        sourceOAuthParameter.getSourceDefinitionId(),
      )
    Assertions.assertFalse(result.isPresent())
  }

  @Test
  @Throws(IOException::class)
  fun testGetStandardSyncUsingOperation() {
    val operationId = standardSyncOperations().get(0)!!.getOperationId()
    val expectedSyncs =
      standardSyncs()
        .subList(0, 3)
        .stream()
        .toList()
        .filterNotNull()
        .toMutableList()
    val actualSyncs = connectionService.listStandardSyncsUsingOperation(operationId).filterNotNull().toMutableList()

    assertSyncsMatch(expectedSyncs, actualSyncs)
  }

  private fun assertSyncsMatch(
    expectedSyncs: MutableList<StandardSync>,
    actualSyncs: MutableList<StandardSync>,
  ) {
    Assertions.assertEquals(expectedSyncs.size, actualSyncs.size)

    for (expected in expectedSyncs) {
      val maybeActual =
        actualSyncs.stream().filter { s: StandardSync -> s.getConnectionId() == expected.getConnectionId() }.findFirst()
      if (maybeActual.isEmpty()) {
        Assertions.fail<Any?>(
          String.format(
            "Expected to find connectionId %s in result, but actual connectionIds are %s",
            expected.getConnectionId(),
            actualSyncs.stream().map<UUID?> { obj: StandardSync -> obj.getConnectionId() }.collect(Collectors.toList()),
          ),
        )
      }
      val actual = maybeActual.get()

      // operationIds can be ordered differently in the query result than in the mock data, so they need
      // to be verified separately
      // from the rest of the sync.
      org.assertj.core.api.Assertions
        .assertThat<UUID?>(actual.getOperationIds())
        .hasSameElementsAs(expected.getOperationIds())

      // now, clear operationIds so the rest of the sync can be compared
      expected.setOperationIds(null)
      actual.setOperationIds(null)
      expected.setCreatedAt(null)
      actual.setCreatedAt(null)
      Assertions.assertEquals(expected, actual)
    }
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testDeleteStandardSyncOperation() {
    val deletedOperationId = standardSyncOperations().get(0)!!.getOperationId()
    val syncs = standardSyncs()
    operationService.deleteStandardSyncOperation(deletedOperationId)

    for (sync in syncs) {
      val retrievedSync = connectionService.getStandardSync(sync!!.getConnectionId())
      for (operationId in sync.getOperationIds()) {
        if (operationId == deletedOperationId) {
          org.assertj.core.api.Assertions
            .assertThat<UUID?>(retrievedSync.getOperationIds())
            .doesNotContain(deletedOperationId)
        } else {
          org.assertj.core.api.Assertions
            .assertThat<UUID?>(retrievedSync.getOperationIds())
            .contains(operationId)
        }
      }
    }
  }

  @Test
  @Throws(IOException::class)
  fun testGetSourceAndDefinitionsFromSourceIds() {
    val sourceIds =
      sourceConnections()
        .subList(0, 2)
        .stream()
        .map<UUID?> { obj: SourceConnection? -> obj!!.getSourceId() }
        .toList()

    val expected =
      listOf<SourceAndDefinition?>(
        SourceAndDefinition(sourceConnections().get(0)!!, standardSourceDefinitions().get(0)!!),
        SourceAndDefinition(sourceConnections().get(1)!!, standardSourceDefinitions().get(1)!!),
      )

    val actual = sourceService.getSourceAndDefinitionsFromSourceIds(sourceIds)
    val result =
      actual
        .stream()
        .map<SourceAndDefinition?> { sourceAndDefinition: SourceAndDefinition? ->
          val copy = SourceAndDefinition(sourceAndDefinition!!.source, sourceAndDefinition.definition)
          copy.source.setCreatedAt(null)
          copy.source.setUpdatedAt(null)
          copy
        }.toList()

    org.assertj.core.api.Assertions
      .assertThat<SourceAndDefinition?>(result)
      .hasSameElementsAs(expected)
  }

  @Test
  @Throws(IOException::class)
  fun testGetDestinationAndDefinitionsFromDestinationIds() {
    val destinationIds =
      destinationConnections()
        .subList(0, 2)
        .stream()
        .map<UUID?> { obj: DestinationConnection? -> obj!!.getDestinationId() }
        .toList()

    val actual = destinationService.getDestinationAndDefinitionsFromDestinationIds(destinationIds)

    val expected =
      listOf<DestinationAndDefinition?>(
        DestinationAndDefinition(destinationConnections().get(0)!!, standardDestinationDefinitions().get(0)!!),
        DestinationAndDefinition(destinationConnections().get(1)!!, standardDestinationDefinitions().get(1)!!),
      )

    val result =
      actual
        .stream()
        .map<DestinationAndDefinition?> { destinationAndDefinition: DestinationAndDefinition? ->
          val copy =
            DestinationAndDefinition(destinationAndDefinition!!.destination, destinationAndDefinition.definition)
          copy.destination.setCreatedAt(null)
          copy.destination.setUpdatedAt(null)
          copy
        }.toList()

    org.assertj.core.api.Assertions
      .assertThat<DestinationAndDefinition?>(result)
      .hasSameElementsAs(expected)
  }

  @Test
  @Throws(SQLException::class, IOException::class)
  fun testGetMostRecentActorCatalogFetchEventForSource() {
    for (actorCatalog in actorCatalogs()) {
      Companion.writeActorCatalog(database!!, mutableListOf<ActorCatalog?>(actorCatalog))
    }

    val now = OffsetDateTime.now()
    val yesterday = now.minusDays(1L)

    val fetchEvents = actorCatalogFetchEventsSameSource()
    val fetchEvent1 = fetchEvents.get(0)
    val fetchEvent2 = fetchEvents.get(1)

    database!!.transaction(
      ContextQueryFunction { ctx: DSLContext ->
        insertCatalogFetchEvent(
          ctx,
          fetchEvent1!!.getActorId(),
          fetchEvent1.getActorCatalogId(),
          yesterday,
        )
        insertCatalogFetchEvent(
          ctx,
          fetchEvent2!!.getActorId(),
          fetchEvent2!!.getActorCatalogId(),
          now,
        )
        // Insert a second identical copy to verify that the query can handle duplicates since the records
        // are not guaranteed to be unique.
        insertCatalogFetchEvent(
          ctx,
          fetchEvent2!!.getActorId(),
          fetchEvent2!!.getActorCatalogId(),
          now,
        )
        null
      },
    )

    val result =
      catalogService.getMostRecentActorCatalogFetchEventForSource(fetchEvent1!!.getActorId())

    Assertions.assertEquals(fetchEvent2!!.getActorCatalogId(), result.get().getActorCatalogId())
  }

  @Test
  @Throws(SQLException::class, IOException::class)
  fun testGetMostRecentActorCatalogFetchEventForSources() {
    for (actorCatalog in actorCatalogs()) {
      Companion.writeActorCatalog(database!!, mutableListOf<ActorCatalog?>(actorCatalog))
    }

    database!!.transaction(
      ContextQueryFunction { ctx: DSLContext ->
        actorCatalogFetchEventsForAggregationTest().forEach(
          Consumer { actorCatalogFetchEvent: ActorCatalogFetchEventWithCreationDate? ->
            insertCatalogFetchEvent(
              ctx,
              actorCatalogFetchEvent!!.actorCatalogFetchEvent!!.getActorId(),
              actorCatalogFetchEvent.actorCatalogFetchEvent.getActorCatalogId(),
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
          MockData.SOURCE_ID_1,
          MockData.SOURCE_ID_2,
        ),
      )

    Assertions.assertEquals(MockData.ACTOR_CATALOG_ID_1, result.get(MockData.SOURCE_ID_1)!!.getActorCatalogId())
    Assertions.assertEquals(MockData.ACTOR_CATALOG_ID_3, result.get(MockData.SOURCE_ID_2)!!.getActorCatalogId())
    Assertions.assertEquals(0, catalogService.getMostRecentActorCatalogFetchEventForSources(mutableListOf<UUID>()).size)
  }

  @Test
  @Throws(SQLException::class, IOException::class)
  fun testGetMostRecentActorCatalogFetchEventWithDuplicates() {
    // Tests that we can handle two fetch events in the db with the same actor id, actor catalog id, and
    // timestamp e.g., from duplicate discoveries.
    for (actorCatalog in actorCatalogs()) {
      Companion.writeActorCatalog(database!!, mutableListOf<ActorCatalog?>(actorCatalog))
    }

    database!!.transaction(
      ContextQueryFunction { ctx: DSLContext ->
        // Insert the fetch events twice.
        actorCatalogFetchEventsForAggregationTest().forEach(
          Consumer { actorCatalogFetchEvent: ActorCatalogFetchEventWithCreationDate? ->
            insertCatalogFetchEvent(
              ctx,
              actorCatalogFetchEvent!!.actorCatalogFetchEvent!!.getActorId(),
              actorCatalogFetchEvent.actorCatalogFetchEvent.getActorCatalogId(),
              actorCatalogFetchEvent.createdAt,
            )
            insertCatalogFetchEvent(
              ctx,
              actorCatalogFetchEvent.actorCatalogFetchEvent.getActorId(),
              actorCatalogFetchEvent.actorCatalogFetchEvent.getActorCatalogId(),
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
          MockData.SOURCE_ID_1,
          MockData.SOURCE_ID_2,
        ),
      )

    Assertions.assertEquals(MockData.ACTOR_CATALOG_ID_1, result.get(MockData.SOURCE_ID_1)!!.getActorCatalogId())
    Assertions.assertEquals(MockData.ACTOR_CATALOG_ID_3, result.get(MockData.SOURCE_ID_2)!!.getActorCatalogId())
  }

  @Test
  @Throws(IOException::class)
  fun testGetActorDefinitionsInUseToProtocolVersion() {
    val actorDefinitionIds: MutableSet<UUID?> = HashSet<UUID?>()
    actorDefinitionIds.addAll(sourceConnections().stream().map<UUID?> { obj: SourceConnection? -> obj!!.getSourceDefinitionId() }.toList())
    actorDefinitionIds.addAll(
      destinationConnections()
        .stream()
        .map<UUID?> { obj: DestinationConnection? -> obj!!.getDestinationDefinitionId() }
        .toList(),
    )
    Assertions.assertEquals(actorDefinitionIds, actorDefinitionService.getActorDefinitionToProtocolVersionMap().keys)
  }

  private fun insertCatalogFetchEvent(
    ctx: DSLContext,
    sourceId: UUID?,
    catalogId: UUID?,
    creationDate: OffsetDateTime?,
  ) {
    ctx
      .insertInto<ActorCatalogFetchEventRecord?>(Tables.ACTOR_CATALOG_FETCH_EVENT)
      .columns<UUID?, UUID?, UUID?, String?, String?, OffsetDateTime?, OffsetDateTime?>(
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
  @Throws(IOException::class)
  fun testGetEarlySyncJobs() {
    // This test just verifies that the query can be run against configAPI DB.
    // The query has been tested locally against prod DB to verify the outputs.
    val earlySyncJobs = connectionService.listEarlySyncJobs(7, 30)
    Assertions.assertNotNull(earlySyncJobs)
  }

  companion object {
    private const val DOCKER_IMAGE_TAG = "1.2.0"
    private const val OTHER_DOCKER_IMAGE_TAG = "1.3.0"
    private const val CONFIG_HASH = "ConfigHash"
    private val DATAPLANE_GROUP_ID: UUID = UUID.randomUUID()

    @Throws(SQLException::class)
    private fun writeActorCatalog(
      database: Database,
      configs: MutableList<ActorCatalog?>,
    ) {
      database!!.transaction(
        ContextQueryFunction { ctx: DSLContext ->
          Companion.writeActorCatalog(configs, ctx!!)
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
                .where(Tables.ACTOR_CATALOG.ID.eq(actorCatalog!!.getId())),
            )
          if (isExistingConfig) {
            ctx
              .update<ActorCatalogRecord?>(Tables.ACTOR_CATALOG)
              .set<JSONB?>(Tables.ACTOR_CATALOG.CATALOG, JSONB.valueOf(serialize<JsonNode?>(actorCatalog.getCatalog())))
              .set<String?>(Tables.ACTOR_CATALOG.CATALOG_HASH, actorCatalog.getCatalogHash())
              .set<ActorCatalogType?>(
                Tables.ACTOR_CATALOG.CATALOG_TYPE,
                ActorCatalogType.valueOf(actorCatalog.getCatalogType().toString()),
              ).set<OffsetDateTime?>(Tables.ACTOR_CATALOG.MODIFIED_AT, timestamp)
              .where(Tables.ACTOR_CATALOG.ID.eq(actorCatalog.getId()))
              .execute()
          } else {
            ctx
              .insertInto<ActorCatalogRecord?>(Tables.ACTOR_CATALOG)
              .set<UUID?>(Tables.ACTOR_CATALOG.ID, actorCatalog.getId())
              .set<JSONB?>(Tables.ACTOR_CATALOG.CATALOG, JSONB.valueOf(serialize<JsonNode?>(actorCatalog.getCatalog())))
              .set<String?>(Tables.ACTOR_CATALOG.CATALOG_HASH, actorCatalog.getCatalogHash())
              .set<ActorCatalogType?>(
                Tables.ACTOR_CATALOG.CATALOG_TYPE,
                ActorCatalogType.valueOf(actorCatalog.getCatalogType().toString()),
              ).set<OffsetDateTime?>(Tables.ACTOR_CATALOG.CREATED_AT, timestamp)
              .set<OffsetDateTime?>(Tables.ACTOR_CATALOG.MODIFIED_AT, timestamp)
              .execute()
          }
        },
      )
    }
  }
}
