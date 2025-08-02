/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.api.model.generated.ActorStatus
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationConnection
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.config.StreamDescriptorForDestination
import io.airbyte.config.Tag
import io.airbyte.config.helpers.CatalogHelpers
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.shared.ConnectionJobStatus
import io.airbyte.data.services.shared.ConnectionWithJobInfo
import io.airbyte.data.services.shared.Cursor
import io.airbyte.data.services.shared.Filters
import io.airbyte.data.services.shared.SortKey
import io.airbyte.data.services.shared.StandardSyncQuery
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination.Companion.fromValues
import io.airbyte.db.ContextQueryFunction
import io.airbyte.db.Database
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.SupportLevel
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorDefinitionRecord
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorDefinitionVersionRecord
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorRecord
import io.airbyte.db.instance.jobs.jooq.generated.Tables
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.airbyte.db.instance.jobs.jooq.generated.tables.records.JobsRecord
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.Field
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.airbyte.test.utils.Databases
import io.airbyte.validation.json.JsonValidationException
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.SQLDialect
import org.jooq.SortField
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.testcontainers.containers.PostgreSQLContainer
import java.io.IOException
import java.sql.SQLException
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Locale
import java.util.UUID
import java.util.stream.Collectors
import java.util.stream.Stream
import javax.sql.DataSource

internal class ConnectionServiceJooqImplTest : BaseConfigDatabaseTest() {
  private val connectionServiceJooqImpl: ConnectionServiceJooqImpl
  private var jobDatabase: Database? = null
  private var dataSource: DataSource? = null
  private var dslContext: DSLContext? = null

  init {
    this.connectionServiceJooqImpl = ConnectionServiceJooqImpl(database)
  }

  @ParameterizedTest
  @MethodSource("actorSyncsStreamTestProvider")
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testActorSyncsAnyListedStream(
    mockActorConnections: MutableList<MutableList<String?>>,
    streamsToCheck: MutableList<String?>,
    actorShouldSyncAnyListedStream: Boolean,
  ) {
    val jooqTestDbSetupHelper = JooqTestDbSetupHelper()
    jooqTestDbSetupHelper.setupForVersionUpgradeTest()

    val destination = jooqTestDbSetupHelper.destination
    val source = jooqTestDbSetupHelper.source

    // Create connections
    for (streamsForConnection in mockActorConnections) {
      val configuredStreams =
        streamsForConnection
          .stream()
          .map<ConfiguredAirbyteStream?> { streamName: String? ->
            catalogHelpers.createConfiguredAirbyteStream(
              streamName!!,
              "namespace",
              Field.of("field_name", JsonSchemaType.STRING),
            )
          }.collect(Collectors.toList())
      val sync = createStandardSync(source!!, destination!!, configuredStreams)
      connectionServiceJooqImpl.writeStandardSync(sync)
    }

    // Assert both source and destination are flagged as syncing
    for (actorId in mutableListOf<UUID>(destination!!.destinationId, source!!.sourceId)) {
      val actorSyncsAnyListedStream = connectionServiceJooqImpl.actorSyncsAnyListedStream(actorId, streamsToCheck.filterNotNull())
      Assertions.assertEquals(actorShouldSyncAnyListedStream, actorSyncsAnyListedStream)
    }
  }

  private fun createStandardSync(
    source: SourceConnection,
    destination: DestinationConnection,
    streams: MutableList<ConfiguredAirbyteStream?>,
  ): StandardSync {
    val connectionId = UUID.randomUUID()
    return StandardSync()
      .withConnectionId(connectionId)
      .withSourceId(source.sourceId)
      .withDestinationId(destination.destinationId)
      .withName("standard-sync-" + connectionId)
      .withCatalog(ConfiguredAirbyteCatalog().withStreams(streams.filterNotNull()))
      .withManual(true)
      .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
      .withBreakingChange(false)
      .withStatus(StandardSync.Status.ACTIVE)
      .withTags(mutableListOf<Tag?>())
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, SQLException::class)
  fun testCreateConnectionWithTags() {
    val jooqTestDbSetupHelper = JooqTestDbSetupHelper()
    jooqTestDbSetupHelper.setUpDependencies()

    val destination = jooqTestDbSetupHelper.destination
    val source = jooqTestDbSetupHelper.source
    val streams =
      mutableListOf<ConfiguredAirbyteStream?>(
        catalogHelpers.createConfiguredAirbyteStream(
          "stream_a",
          "namespace",
          Field.of("field_name", JsonSchemaType.STRING),
        ),
      )
    val tags = jooqTestDbSetupHelper.tags

    val standardSyncToCreate = createStandardSync(source!!, destination!!, streams)

    standardSyncToCreate.setTags(tags)

    connectionServiceJooqImpl.writeStandardSync(standardSyncToCreate)

    val standardSyncPersisted = connectionServiceJooqImpl.getStandardSync(standardSyncToCreate.connectionId)

    Assertions.assertEquals(tags, standardSyncPersisted.tags)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, SQLException::class)
  fun testUpdateConnectionWithTags() {
    val jooqTestDbSetupHelper = JooqTestDbSetupHelper()
    jooqTestDbSetupHelper.setUpDependencies()

    val destination = jooqTestDbSetupHelper.destination
    val source = jooqTestDbSetupHelper.source
    val streams =
      mutableListOf<ConfiguredAirbyteStream?>(
        catalogHelpers.createConfiguredAirbyteStream(
          "stream_a",
          "namespace",
          Field.of("field_name", JsonSchemaType.STRING),
        ),
      )
    val tags = jooqTestDbSetupHelper.tags

    val standardSyncToCreate = createStandardSync(source!!, destination!!, streams)

    standardSyncToCreate.setTags(tags)

    connectionServiceJooqImpl.writeStandardSync(standardSyncToCreate)

    // update the connection with only the third tag
    val updatedTags = mutableListOf<Tag?>(tags!![2])
    standardSyncToCreate.setTags(updatedTags)
    connectionServiceJooqImpl.writeStandardSync(standardSyncToCreate)

    val standardSyncPersisted = connectionServiceJooqImpl.getStandardSync(standardSyncToCreate.connectionId)

    Assertions.assertEquals(updatedTags, standardSyncPersisted.tags)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, SQLException::class)
  fun testUpdateConnectionWithTagsFromMultipleWorkspaces() {
    val jooqTestDbSetupHelper = JooqTestDbSetupHelper()
    jooqTestDbSetupHelper.setUpDependencies()

    val destination = jooqTestDbSetupHelper.destination
    val source = jooqTestDbSetupHelper.source
    val streams =
      mutableListOf<ConfiguredAirbyteStream?>(
        catalogHelpers.createConfiguredAirbyteStream(
          "stream_a",
          "namespace",
          Field.of("field_name", JsonSchemaType.STRING),
        ),
      )

    val standardSyncToCreate = createStandardSync(source!!, destination!!, streams)

    val tags = jooqTestDbSetupHelper.tags
    val tagsFromAnotherWorkspace = jooqTestDbSetupHelper.tagsFromAnotherWorkspace
    val tagsFromMultipleWorkspaces = Stream.concat<Tag?>(tags!!.stream(), tagsFromAnotherWorkspace!!.stream()).toList()

    standardSyncToCreate.setTags(tagsFromMultipleWorkspaces)
    connectionServiceJooqImpl.writeStandardSync(standardSyncToCreate)
    val standardSyncPersisted = connectionServiceJooqImpl.getStandardSync(standardSyncToCreate.connectionId)

    Assertions.assertNotEquals(tagsFromMultipleWorkspaces, standardSyncPersisted.tags)
    Assertions.assertEquals(tags, standardSyncPersisted.tags)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testGetStreamsForDestination() {
    val jooqTestDbSetupHelper = JooqTestDbSetupHelper()
    jooqTestDbSetupHelper.setUpDependencies()

    val destination = jooqTestDbSetupHelper.destination
    val source = jooqTestDbSetupHelper.source

    // Create a connection with multiple streams in different states
    val streams =
      mutableListOf<ConfiguredAirbyteStream?>( // Selected stream
        catalogHelpers.createConfiguredAirbyteStream(
          "stream_a",
          "namespace_1",
          Field.of("field_1", JsonSchemaType.STRING),
        ), // Selected stream with different namespace
        catalogHelpers.createConfiguredAirbyteStream("stream_b", "namespace_2", Field.of("field_1", JsonSchemaType.STRING)),
      )

    val standardSync =
      createStandardSync(source!!, destination!!, streams)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withPrefix("prefix_")
        .withNamespaceFormat("\${SOURCE_NAMESPACE}")

    connectionServiceJooqImpl.writeStandardSync(standardSync)

    // Create another connection that's inactive
    val inactiveStreams =
      mutableListOf<ConfiguredAirbyteStream?>(
        catalogHelpers.createConfiguredAirbyteStream("stream_d", "namespace_3", Field.of("field_1", JsonSchemaType.STRING)),
      )

    val inactiveSync =
      createStandardSync(source, destination, inactiveStreams)
        .withStatus(StandardSync.Status.INACTIVE)

    connectionServiceJooqImpl.writeStandardSync(inactiveSync)

    // Get streams for destination
    val streamConfigs: List<StreamDescriptorForDestination> =
      connectionServiceJooqImpl.listStreamsForDestination(destination!!.destinationId, null)

    // Should only return selected streams from active connections
    Assertions.assertEquals(2, streamConfigs.size)

    // Verify first stream
    val streamConfigA =
      streamConfigs
        .stream()
        .filter { s: StreamDescriptorForDestination? -> "stream_a" == s!!.streamName }
        .findFirst()
        .orElseThrow()
    Assertions.assertEquals("namespace_1", streamConfigA.getStreamNamespace())
    Assertions.assertEquals(JobSyncConfig.NamespaceDefinitionType.SOURCE, streamConfigA.getNamespaceDefinition())
    Assertions.assertEquals("\${SOURCE_NAMESPACE}", streamConfigA.getNamespaceFormat())
    Assertions.assertEquals("prefix_", streamConfigA.getPrefix())

    // Verify second stream
    val streamConfigC =
      streamConfigs
        .stream()
        .filter { s: StreamDescriptorForDestination? -> "stream_b" == s!!.streamName }
        .findFirst()
        .orElseThrow()
    Assertions.assertEquals("namespace_2", streamConfigC.getStreamNamespace())
    Assertions.assertEquals(JobSyncConfig.NamespaceDefinitionType.SOURCE, streamConfigC.getNamespaceDefinition())
    Assertions.assertEquals("\${SOURCE_NAMESPACE}", streamConfigC.getNamespaceFormat())
    Assertions.assertEquals("prefix_", streamConfigC.getPrefix())
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testGetStreamsForDestinationWithMultipleConnections() {
    val jooqTestDbSetupHelper = JooqTestDbSetupHelper()
    jooqTestDbSetupHelper.setUpDependencies()

    val destination = jooqTestDbSetupHelper.destination
    val source = jooqTestDbSetupHelper.source

    // Create first connection with custom namespace
    val streams1 =
      mutableListOf<ConfiguredAirbyteStream?>(
        catalogHelpers.createConfiguredAirbyteStream("stream_a", "namespace_1", Field.of("field_1", JsonSchemaType.STRING)),
      )

    val sync1 =
      createStandardSync(source!!, destination!!, streams1)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT)
        .withNamespaceFormat("custom_\${SOURCE_NAMESPACE}")
        .withPrefix("prefix1_")

    connectionServiceJooqImpl.writeStandardSync(sync1)

    // Create second connection with destination namespace
    val streams2 =
      mutableListOf<ConfiguredAirbyteStream?>(
        catalogHelpers.createConfiguredAirbyteStream("stream_b", "namespace_2", Field.of("field_1", JsonSchemaType.STRING)),
      )

    val sync2 =
      createStandardSync(source!!, destination!!, streams2)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.DESTINATION)
        .withPrefix("prefix2_")

    connectionServiceJooqImpl.writeStandardSync(sync2)

    // Get streams for destination
    val streamConfigs =
      connectionServiceJooqImpl.listStreamsForDestination(destination!!.destinationId, null)

    Assertions.assertEquals(2, streamConfigs.size)

    // Verify first stream
    val streamConfigA =
      streamConfigs
        .stream()
        .filter { s: StreamDescriptorForDestination? -> "stream_a" == s!!.streamName }
        .findFirst()
        .orElseThrow()
    Assertions.assertEquals("namespace_1", streamConfigA.getStreamNamespace())
    Assertions.assertEquals(JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT, streamConfigA.getNamespaceDefinition())
    Assertions.assertEquals("custom_\${SOURCE_NAMESPACE}", streamConfigA.getNamespaceFormat())
    Assertions.assertEquals("prefix1_", streamConfigA.getPrefix())

    // Verify second stream
    val streamConfigB =
      streamConfigs
        .stream()
        .filter { s: StreamDescriptorForDestination? -> "stream_b" == s!!.streamName }
        .findFirst()
        .orElseThrow()
    Assertions.assertEquals("namespace_2", streamConfigB.getStreamNamespace())
    Assertions.assertEquals(JobSyncConfig.NamespaceDefinitionType.DESTINATION, streamConfigB.getNamespaceDefinition())
    Assertions.assertNull(streamConfigB.getNamespaceFormat())
    Assertions.assertEquals("prefix2_", streamConfigB.getPrefix())
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testGetConnectionStatusCounts() {
    val jooqTestDbSetupHelper = JooqTestDbSetupHelper()
    jooqTestDbSetupHelper.setUpDependencies()

    val destination = jooqTestDbSetupHelper.destination
    val source = jooqTestDbSetupHelper.source
    val workspaceId = jooqTestDbSetupHelper.workspace!!.workspaceId

    // Create connections with different statuses
    val streams =
      mutableListOf<ConfiguredAirbyteStream?>(
        catalogHelpers.createConfiguredAirbyteStream("stream_a", "namespace", Field.of("field_name", JsonSchemaType.STRING)),
      )

    // Connection 1: Active with running job
    val runningConnection =
      createStandardSync(source!!, destination!!, streams)
        .withStatus(StandardSync.Status.ACTIVE)
    connectionServiceJooqImpl.writeStandardSync(runningConnection)

    // Connection 2: Active with successful latest job
    val healthyConnection =
      createStandardSync(source!!, destination!!, streams)
        .withStatus(StandardSync.Status.ACTIVE)
    connectionServiceJooqImpl.writeStandardSync(healthyConnection)

    // Connection 3: Active with failed latest job
    val failedConnection =
      createStandardSync(source!!, destination!!, streams)
        .withStatus(StandardSync.Status.ACTIVE)
    connectionServiceJooqImpl.writeStandardSync(failedConnection)

    // Connection 4: Inactive (paused)
    val pausedConnection =
      createStandardSync(source!!, destination!!, streams)
        .withStatus(StandardSync.Status.INACTIVE)
    connectionServiceJooqImpl.writeStandardSync(pausedConnection)

    // Connection 5: Active with cancelled latest job (should count as failed)
    val cancelledConnection =
      createStandardSync(source!!, destination!!, streams)
        .withStatus(StandardSync.Status.ACTIVE)
    connectionServiceJooqImpl.writeStandardSync(cancelledConnection)

    // Connection 6: Active with incomplete latest job (should count as failed)
    val incompleteConnection =
      createStandardSync(source!!, destination!!, streams)
        .withStatus(StandardSync.Status.ACTIVE)
    connectionServiceJooqImpl.writeStandardSync(incompleteConnection)

    // Connection 7: Deprecated (should be excluded from counts)
    val deprecatedConnection =
      createStandardSync(source!!, destination!!, streams)
        .withStatus(StandardSync.Status.DEPRECATED)
    connectionServiceJooqImpl.writeStandardSync(deprecatedConnection)

    // Connection 8: Active but no sync jobs (should count as not synced)
    val notSyncedConnection =
      createStandardSync(source!!, destination!!, streams)
        .withStatus(StandardSync.Status.ACTIVE)
    connectionServiceJooqImpl.writeStandardSync(notSyncedConnection)

    // Connection 4: Inactive but last job succeeded
    val pausedWithJobSucceededConnection =
      createStandardSync(source!!, destination!!, streams)
        .withStatus(StandardSync.Status.INACTIVE)
    connectionServiceJooqImpl.writeStandardSync(pausedWithJobSucceededConnection)

    // Insert job records using raw SQL since we need to work with the jobs database
    database!!.query<Any?>(
      ContextQueryFunction { ctx: DSLContext? ->
        val now = OffsetDateTime.now()
        // Job for running connection - currently running
        ctx!!
          .insertInto<JobsRecord?>(Tables.JOBS)
          .set<Long?>(Tables.JOBS.ID, 1L)
          .set<JobConfigType?>(
            Tables.JOBS.CONFIG_TYPE,
            JobConfigType.sync,
          ).set<String?>(Tables.JOBS.SCOPE, runningConnection.connectionId.toString())
          .set<JobStatus?>(Tables.JOBS.STATUS, JobStatus.running)
          .set<OffsetDateTime?>(Tables.JOBS.CREATED_AT, now.minusHours(1))
          .set<OffsetDateTime?>(Tables.JOBS.UPDATED_AT, now.minusHours(1))
          .set<JSONB?>(Tables.JOBS.CONFIG, DSL.field<JSONB?>("'{}'::jsonb", JSONB::class.java))
          .execute()

        // Job for healthy connection - succeeded
        ctx
          .insertInto<JobsRecord?>(Tables.JOBS)
          .set<Long?>(Tables.JOBS.ID, 2L)
          .set<JobConfigType?>(
            Tables.JOBS.CONFIG_TYPE,
            JobConfigType.sync,
          ).set<String?>(Tables.JOBS.SCOPE, healthyConnection.connectionId.toString())
          .set<JobStatus?>(Tables.JOBS.STATUS, JobStatus.succeeded)
          .set<OffsetDateTime?>(Tables.JOBS.CREATED_AT, now.minusHours(2))
          .set<OffsetDateTime?>(Tables.JOBS.UPDATED_AT, now.minusHours(2))
          .set<JSONB?>(Tables.JOBS.CONFIG, DSL.field<JSONB?>("'{}'::jsonb", JSONB::class.java))
          .execute()

        // Job for failed connection - failed
        ctx
          .insertInto<JobsRecord?>(Tables.JOBS)
          .set<Long?>(Tables.JOBS.ID, 3L)
          .set<JobConfigType?>(
            Tables.JOBS.CONFIG_TYPE,
            JobConfigType.sync,
          ).set<String?>(Tables.JOBS.SCOPE, failedConnection.connectionId.toString())
          .set<JobStatus?>(Tables.JOBS.STATUS, JobStatus.failed)
          .set<OffsetDateTime?>(Tables.JOBS.CREATED_AT, now.minusHours(3))
          .set<OffsetDateTime?>(Tables.JOBS.UPDATED_AT, now.minusHours(3))
          .set<JSONB?>(Tables.JOBS.CONFIG, DSL.field<JSONB?>("'{}'::jsonb", JSONB::class.java))
          .execute()

        // Job for cancelled connection - cancelled (should count as failed)
        ctx
          .insertInto<JobsRecord?>(Tables.JOBS)
          .set<Long?>(Tables.JOBS.ID, 4L)
          .set<JobConfigType?>(
            Tables.JOBS.CONFIG_TYPE,
            JobConfigType.sync,
          ).set<String?>(Tables.JOBS.SCOPE, cancelledConnection.connectionId.toString())
          .set<JobStatus?>(Tables.JOBS.STATUS, JobStatus.cancelled)
          .set<OffsetDateTime?>(Tables.JOBS.CREATED_AT, now.minusHours(4))
          .set<OffsetDateTime?>(Tables.JOBS.UPDATED_AT, now.minusHours(4))
          .set<JSONB?>(Tables.JOBS.CONFIG, DSL.field<JSONB?>("'{}'::jsonb", JSONB::class.java))
          .execute()

        // Job for incomplete connection - incomplete (should count as failed)
        ctx
          .insertInto<JobsRecord?>(Tables.JOBS)
          .set<Long?>(Tables.JOBS.ID, 5L)
          .set<JobConfigType?>(
            Tables.JOBS.CONFIG_TYPE,
            JobConfigType.sync,
          ).set<String?>(Tables.JOBS.SCOPE, incompleteConnection.connectionId.toString())
          .set<JobStatus?>(Tables.JOBS.STATUS, JobStatus.incomplete)
          .set<OffsetDateTime?>(Tables.JOBS.CREATED_AT, now.minusHours(5))
          .set<OffsetDateTime?>(Tables.JOBS.UPDATED_AT, now.minusHours(5))
          .set<JSONB?>(Tables.JOBS.CONFIG, DSL.field<JSONB?>("'{}'::jsonb", JSONB::class.java))
          .execute()

        // Job for deprecated connection - should be excluded
        ctx
          .insertInto<JobsRecord?>(Tables.JOBS)
          .set<Long?>(Tables.JOBS.ID, 6L)
          .set<JobConfigType?>(
            Tables.JOBS.CONFIG_TYPE,
            JobConfigType.sync,
          ).set<String?>(Tables.JOBS.SCOPE, deprecatedConnection.connectionId.toString())
          .set<JobStatus?>(Tables.JOBS.STATUS, JobStatus.succeeded)
          .set<OffsetDateTime?>(Tables.JOBS.CREATED_AT, now.minusHours(6))
          .set<OffsetDateTime?>(Tables.JOBS.UPDATED_AT, now.minusHours(6))
          .set<JSONB?>(Tables.JOBS.CONFIG, DSL.field<JSONB?>("'{}'::jsonb", JSONB::class.java))
          .execute()

        // Add older job for healthy connection to ensure we get the latest
        ctx
          .insertInto<JobsRecord?>(Tables.JOBS)
          .set<Long?>(Tables.JOBS.ID, 7L)
          .set<JobConfigType?>(
            Tables.JOBS.CONFIG_TYPE,
            JobConfigType.sync,
          ).set<String?>(Tables.JOBS.SCOPE, healthyConnection.connectionId.toString())
          .set<JobStatus?>(Tables.JOBS.STATUS, JobStatus.failed)
          .set<OffsetDateTime?>(Tables.JOBS.CREATED_AT, now.minusHours(10))
          .set<OffsetDateTime?>(Tables.JOBS.UPDATED_AT, now.minusHours(10))
          .set<JSONB?>(Tables.JOBS.CONFIG, DSL.field<JSONB?>("'{}'::jsonb", JSONB::class.java))
          .execute()

        // Job for successful sync but paused connection
        ctx
          .insertInto<JobsRecord?>(Tables.JOBS)
          .set<Long?>(Tables.JOBS.ID, 8L)
          .set<JobConfigType?>(
            Tables.JOBS.CONFIG_TYPE,
            JobConfigType.sync,
          ).set<String?>(Tables.JOBS.SCOPE, pausedWithJobSucceededConnection.connectionId.toString())
          .set<JobStatus?>(Tables.JOBS.STATUS, JobStatus.succeeded)
          .set<OffsetDateTime?>(Tables.JOBS.CREATED_AT, now.minusHours(2))
          .set<OffsetDateTime?>(Tables.JOBS.UPDATED_AT, now.minusHours(2))
          .set<JSONB?>(Tables.JOBS.CONFIG, DSL.field<JSONB?>("'{}'::jsonb", JSONB::class.java))
          .execute()
        null
      },
    )

    val result = connectionServiceJooqImpl.getConnectionStatusCounts(workspaceId)

    Assertions.assertEquals(1, result.running)
    Assertions.assertEquals(1, result.healthy)
    // failedConnection + cancelledConnection + incompleteConnection
    Assertions.assertEquals(3, result.failed)
    Assertions.assertEquals(2, result.paused)
    // notSyncedConnection (active connection with no jobs)
    Assertions.assertEquals(1, result.notSynced)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testGetConnectionStatusCountsNoJobs() {
    val jooqTestDbSetupHelper = JooqTestDbSetupHelper()
    jooqTestDbSetupHelper.setUpDependencies()

    val destination = jooqTestDbSetupHelper.destination
    val source = jooqTestDbSetupHelper.source
    val workspaceId = jooqTestDbSetupHelper.workspace!!.workspaceId

    val streams =
      mutableListOf<ConfiguredAirbyteStream?>(
        catalogHelpers.createConfiguredAirbyteStream("stream_a", "namespace", Field.of("field_name", JsonSchemaType.STRING)),
      )

    val activeConnection =
      createStandardSync(source!!, destination!!, streams)
        .withStatus(StandardSync.Status.ACTIVE)
    connectionServiceJooqImpl.writeStandardSync(activeConnection)

    val inactiveConnection =
      createStandardSync(source!!, destination!!, streams)
        .withStatus(StandardSync.Status.INACTIVE)
    connectionServiceJooqImpl.writeStandardSync(inactiveConnection)

    val result = connectionServiceJooqImpl.getConnectionStatusCounts(workspaceId)

    Assertions.assertEquals(0, result.running)
    Assertions.assertEquals(0, result.healthy)
    Assertions.assertEquals(0, result.failed)
    Assertions.assertEquals(1, result.paused)
    // activeConnection (active connection with no jobs)
    Assertions.assertEquals(1, result.notSynced)
  }

  @Test
  @Throws(IOException::class)
  fun testGetConnectionStatusCountsEmptyWorkspace() {
    val nonExistentWorkspaceId = UUID.randomUUID()

    val result = connectionServiceJooqImpl.getConnectionStatusCounts(nonExistentWorkspaceId)

    Assertions.assertEquals(0, result.running)
    Assertions.assertEquals(0, result.healthy)
    Assertions.assertEquals(0, result.failed)
    Assertions.assertEquals(0, result.paused)
    Assertions.assertEquals(0, result.notSynced)
  }

  @ParameterizedTest
  @MethodSource("orderByTestProvider")
  fun testBuildOrderByClause(
    sortKey: SortKey,
    ascending: Boolean,
    expectedFirstField: SortField<*>?,
    expectedLastField: SortField<*>?,
  ) {
    val cursor = Cursor(sortKey, null, null, null, null, null, null, null, ascending, null)
    val fields = connectionServiceJooqImpl.buildOrderByClause(cursor)

    Assertions.assertEquals(expectedFirstField, fields.get(0))
    Assertions.assertEquals(expectedLastField, fields.get(fields.size - 1))
  }

  @ParameterizedTest
  @MethodSource("sortKeyTestProvider")
  fun testBuildCursorConditionAscending(
    sortKey: SortKey,
    connectionName: String?,
    sourceName: String?,
    destinationName: String?,
    lastSync: Long?,
    connectionId: UUID?,
  ) {
    val cursor =
      Cursor(
        sortKey,
        connectionName,
        sourceName,
        null,
        destinationName,
        null,
        lastSync,
        connectionId,
        true,
        null,
      )

    val condition = connectionServiceJooqImpl.buildCursorCondition(cursor)
    Assertions.assertTrue(condition.toString().contains(" > "))
  }

  @ParameterizedTest
  @MethodSource("stateFiltersTestProvider")
  fun testApplyStateFilters(
    input: MutableList<ActorStatus?>,
    expectedStrings: MutableList<String>,
  ) {
    val condition = connectionServiceJooqImpl.applyStateFilters(input.filterNotNull())
    val conditionStr = condition.toString()

    for (expectedString in expectedStrings) {
      Assertions.assertTrue(
        conditionStr.contains(expectedString),
        "Expected condition to contain '" + expectedString + "' but was: " + conditionStr,
      )
    }
  }

  @ParameterizedTest
  @MethodSource("statusFiltersTestProvider")
  fun testApplyStatusFilters(
    input: MutableList<ConnectionJobStatus?>,
    expectedStrings: MutableList<String>,
  ) {
    val condition = connectionServiceJooqImpl.applyStatusFilters(input.filterNotNull())
    val conditionStr = condition.toString()

    for (expectedString in expectedStrings) {
      Assertions.assertTrue(
        conditionStr.contains(expectedString),
        "Expected condition to contain '" + expectedString + "' but was: " + conditionStr,
      )
    }
  }

  @ParameterizedTest
  @MethodSource("connectionFilterConditionsTestProvider")
  fun testBuildConnectionFilterConditions(
    query: StandardSyncQuery,
    cursor: Cursor?,
    expectedStrings: MutableList<String>,
    description: String?,
  ) {
    val filters = if (cursor != null) cursor.filters else null
    val condition = connectionServiceJooqImpl.buildConnectionFilterConditions(query, filters)
    val conditionStr = condition.toString()

    for (expectedString in expectedStrings) {
      Assertions.assertTrue(
        conditionStr.contains(expectedString),
        description + " - Expected condition to contain '" + expectedString + "' but was: " + conditionStr,
      )
    }
  }

  @ParameterizedTest
  @MethodSource("buildCursorConditionLastSyncDescTestProvider")
  fun testBuildCursorConditionLastSyncDesc(
    cursor: Cursor?,
    shouldReturnNoCondition: Boolean,
    description: String?,
  ) {
    // When
    val condition = connectionServiceJooqImpl.buildCursorConditionLastSyncDesc(cursor)

    // Then
    if (shouldReturnNoCondition) {
      Assertions.assertEquals(DSL.noCondition().toString(), condition.toString(), description)
    } else {
      Assertions.assertNotEquals(DSL.noCondition().toString(), condition.toString(), description)

      // Verify the condition contains the expected structure
      val conditionString = condition.toString()

      if (cursor != null && cursor.cursorId != null) {
        // Should contain connection ID comparison
        Assertions.assertTrue(
          conditionString.contains(cursor.cursorId.toString()),
          description + ": Expected connection ID in condition",
        )

        if (cursor.lastSync != null) {
          // Should contain time comparison logic
          Assertions.assertTrue(
            conditionString.contains("latest_jobs.created_at") ||
              conditionString.contains("is not null") ||
              conditionString.contains("is null"),
            description + ": Expected time comparison logic in condition",
          )
        } else {
          // Should contain null check logic
          Assertions.assertTrue(
            conditionString.contains("is null"),
            description + ": Expected null check logic in condition",
          )
        }
      }
    }
  }

  @Test
  @Throws(Exception::class)
  fun testListWorkspaceStandardSyncsCursorPaginated() {
    val setupHelper = JooqTestDbSetupHelper()
    setupHelper.setUpDependencies()

    val workspaceId = setupHelper.workspace!!.getWorkspaceId()
    val destination = setupHelper.destination
    val source = setupHelper.source

    val sync = createStandardSync(source!!, destination!!, mutableListOf<ConfiguredAirbyteStream?>())
    connectionServiceJooqImpl.writeStandardSync(sync)

    val pagination =
      WorkspaceResourceCursorPagination(
        Cursor(
          SortKey.CONNECTION_NAME,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          true,
          null,
        ),
        10,
      )

    val result =
      connectionServiceJooqImpl.listWorkspaceStandardSyncsCursorPaginated(
        StandardSyncQuery(workspaceId, null, null, false),
        pagination,
      )

    Assertions.assertEquals(1, result.size)
    Assertions.assertEquals(sync.connectionId, result.first().connection().connectionId)
  }

  @Test
  @Throws(Exception::class)
  fun testCountWorkspaceStandardSyncs() {
    val setupHelper = JooqTestDbSetupHelper()
    setupHelper.setUpDependencies()

    val workspaceId = setupHelper.workspace!!.getWorkspaceId()
    val destination = setupHelper.destination
    val source = setupHelper.source

    val sync = createStandardSync(source!!, destination!!, mutableListOf<ConfiguredAirbyteStream?>())
    connectionServiceJooqImpl.writeStandardSync(sync)

    val count =
      connectionServiceJooqImpl.countWorkspaceStandardSyncs(
        StandardSyncQuery(workspaceId, null, null, false),
        null,
      )

    Assertions.assertEquals(1, count)
  }

  @Test
  @Throws(Exception::class)
  fun testBuildCursorPaginationNoCursor() {
    val setupHelper = JooqTestDbSetupHelper()
    setupHelper.setUpDependencies()

    val workspaceId = setupHelper.workspace!!.getWorkspaceId()
    val query = StandardSyncQuery(workspaceId, null, null, false)
    val pageSize = 20

    val result =
      connectionServiceJooqImpl.buildCursorPagination(
        null,
        SortKey.CONNECTION_NAME,
        null,
        query,
        true,
        pageSize,
      )

    Assertions.assertNotNull(result)
    Assertions.assertEquals(pageSize, result.pageSize)
    Assertions.assertNotNull(result.cursor)
    Assertions.assertEquals(SortKey.CONNECTION_NAME, result.cursor!!.sortKey)
    Assertions.assertTrue(result.cursor!!.ascending)
    Assertions.assertNull(result.cursor!!.connectionName)
    Assertions.assertNull(result.cursor!!.sourceName)
    Assertions.assertNull(result.cursor!!.destinationName)
    Assertions.assertNull(result.cursor!!.lastSync)
    Assertions.assertNull(result.cursor!!.cursorId)
    Assertions.assertNull(result.cursor!!.filters)
  }

  @Test
  @Throws(Exception::class)
  fun testBuildCursorPaginationWithCursor() {
    val setupHelper = JooqTestDbSetupHelper()
    setupHelper.setUpDependencies()

    val workspaceId = setupHelper.workspace!!.getWorkspaceId()
    val destination = setupHelper.destination
    val source = setupHelper.source
    val sync = createStandardSync(source!!, destination!!, mutableListOf<ConfiguredAirbyteStream?>())
    connectionServiceJooqImpl.writeStandardSync(sync)

    val query = StandardSyncQuery(workspaceId, null, null, false)
    val pageSize = 20

    val result =
      connectionServiceJooqImpl.buildCursorPagination(
        sync.connectionId,
        SortKey.CONNECTION_NAME,
        null,
        query,
        true,
        pageSize,
      )

    Assertions.assertNotNull(result)
    Assertions.assertEquals(pageSize, result.pageSize)
    Assertions.assertNotNull(result.cursor)
    Assertions.assertEquals(SortKey.CONNECTION_NAME, result.cursor!!.sortKey)
    Assertions.assertTrue(result.cursor!!.ascending)
    Assertions.assertNull(result.cursor!!.filters)
  }

  @ParameterizedTest
  @MethodSource("paginationTestProvider")
  @Throws(Exception::class)
  fun testListWorkspaceStandardSyncsCursorPaginatedComprehensive(
    sortKey: SortKey,
    ascending: Boolean,
    filters: Filters?,
    testDescription: String?,
  ) {
    setupJobsDatabase()
    val testData = createComprehensiveTestData()
    val pageSize = 3

    val initialCursor =
      Cursor(
        sortKey,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        ascending,
        filters,
      )

    val allResults: MutableList<ConnectionWithJobInfo> = ArrayList<ConnectionWithJobInfo>()
    val seenConnectionIds: MutableSet<UUID?> = HashSet<UUID?>()
    var currentCursor: Cursor? = initialCursor
    var iterations = 0
    val maxIterations = 20 // Safety check to prevent infinite loops

    val seenPageSizes: MutableList<Int?> = ArrayList<Int?>()
    // Paginate through all results
    while (iterations < maxIterations) {
      val pagination = WorkspaceResourceCursorPagination(currentCursor, pageSize)
      val query = StandardSyncQuery(testData.workspaceId, null, null, false)
      val pageResults =
        connectionServiceJooqImpl.listWorkspaceStandardSyncsCursorPaginated(
          query,
          pagination,
        )

      seenPageSizes.add(pageResults.size)

      if (pageResults.isEmpty()) {
        break
      }

      // Verify no overlap with previous results
      for (result in pageResults) {
        val connectionId = result.connection().connectionId
        Assertions.assertFalse(
          seenConnectionIds.contains(connectionId),
          testDescription + " - " + seenPageSizes + " - Found duplicate connection ID: " + connectionId + " in iteration " + iterations,
        )
        seenConnectionIds.add(connectionId)
      }

      allResults.addAll(pageResults)

      // Create cursor from last result for next page
      val lastResult: ConnectionWithJobInfo = pageResults.last()
      currentCursor =
        connectionServiceJooqImpl
          .buildCursorPagination(
            lastResult.connection().connectionId,
            sortKey,
            filters,
            query,
            ascending,
            pageSize,
          ).cursor
      iterations++
    }

    Assertions.assertTrue(iterations < maxIterations, testDescription + " - Too many iterations, possible infinite loop")

    // Get count with same filters for comparison
    val totalCount =
      connectionServiceJooqImpl.countWorkspaceStandardSyncs(
        StandardSyncQuery(testData.workspaceId, null, null, false),
        filters,
      )

    Assertions.assertEquals(
      totalCount,
      allResults.size,
      testDescription + " - Pagination result count " + seenPageSizes + " should match total count",
    )
    verifyResultsSorted(allResults, sortKey, ascending, testDescription)
    verifyResultsMatchFilters(allResults.toMutableList(), filters, testDescription)
  }

  @ParameterizedTest
  @MethodSource("paginationTestProvider")
  @Throws(Exception::class)
  fun testCountWorkspaceStandardSyncsComprehensive(
    sortKey: SortKey,
    ascending: Boolean,
    filters: Filters?,
    testDescription: String?,
  ) {
    setupJobsDatabase()
    val testData = createComprehensiveTestData()

    val count =
      connectionServiceJooqImpl.countWorkspaceStandardSyncs(
        StandardSyncQuery(testData.workspaceId, null, null, false),
        filters,
      )

    // Verify count is reasonable based on test data
    Assertions.assertTrue(
      count >= 0 && count <= testData.expectedTotalConnections,
      testDescription + " - Count should be between 0 and " + testData.expectedTotalConnections + " but was: " + count,
    )

    // Get actual results to verify count accuracy
    val allResults =
      connectionServiceJooqImpl.listWorkspaceStandardSyncsCursorPaginated(
        StandardSyncQuery(testData.workspaceId, null, null, false),
        WorkspaceResourceCursorPagination(
          Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters),
          100,
        ),
      )

    Assertions.assertEquals(
      allResults.size,
      count,
      testDescription + " - Count should match actual result size",
    )
    verifyResultsMatchFilters(allResults.toMutableList(), filters, testDescription)
  }

  private class ComprehensiveTestData(
    val workspaceId: UUID,
    val connectionIds: MutableList<UUID?>?,
    val tagIds: MutableList<UUID?>?,
    val sourceDefinitionIds: MutableList<UUID?>?,
    val destinationDefinitionIds: MutableList<UUID?>?,
    val expectedTotalConnections: Int,
  )

  private fun setupJobsDatabase() {
    if (jobDatabase == null) {
      try {
        dataSource = Databases.createDataSource(container!!)
        dslContext = DSLContextFactory.create(dataSource!!, SQLDialect.POSTGRES)
        val databaseProviders = TestDatabaseProviders(dataSource!!, dslContext!!)
        jobDatabase = databaseProviders.turnOffMigration().createNewJobsDatabase()
      } catch (e: Exception) {
        throw RuntimeException("Failed to setup jobs database", e)
      }
    }
  }

  @Throws(Exception::class)
  private fun createComprehensiveTestData(): ComprehensiveTestData {
    val setupHelper = JooqTestDbSetupHelper()
    setupHelper.setUpDependencies()

    val workspaceId = setupHelper.workspace!!.getWorkspaceId()
    val tags = setupHelper.tags
    val tagIds = tags!!.stream().map<UUID?> { obj: Tag? -> obj!!.tagId }.collect(Collectors.toList())

    // Create deterministic definition IDs for testing
    val sourceDefId1 = UUID.fromString("11111111-1111-1111-1111-111111111111")
    val sourceDefId2 = UUID.fromString("22222222-2222-2222-2222-222222222222")
    val destDefId1 = UUID.fromString("33333333-3333-3333-3333-333333333333")
    val destDefId2 = UUID.fromString("44444444-4444-4444-4444-444444444444")

    // Create additional source and destination definitions
    createSourceDefinition(sourceDefId1, "test-source-definition-1")
    createSourceDefinition(sourceDefId2, "test-source-definition-2")
    createDestinationDefinition(destDefId1, "test-destination-definition-1")
    createDestinationDefinition(destDefId2, "test-destination-definition-2")

    // Create sources and destinations with different definition IDs
    val sources: MutableList<SourceConnection> = ArrayList<SourceConnection>()
    val destinations: MutableList<DestinationConnection> = ArrayList<DestinationConnection>()

    sources.add(setupHelper.source!!.withName("Z"))
    sources.add(createAdditionalSource(setupHelper, "z", sourceDefId1))
    sources.add(createAdditionalSource(setupHelper, "Sample Data (Faker)", sourceDefId2))

    destinations.add(setupHelper.destination!!.withName("dest-alpha"))
    destinations.add(createAdditionalDestination(setupHelper, "dest-beta", destDefId1))
    destinations.add(createAdditionalDestination(setupHelper, "dest-gamma", destDefId2))

    val connectionIds: MutableList<UUID?> = ArrayList<UUID?>()

    // Create connections with various configurations
    var connectionCounter = 0
    for (sourceConnection in sources) {
      for (destinationConnection in destinations) {
        val source = sourceConnection
        val destination = destinationConnection

        // Create connection with varying properties
        val sync =
          createStandardSync(source!!, destination!!, mutableListOf<ConfiguredAirbyteStream?>())
            .withName("conn-" + ('a'.code + connectionCounter).toChar() + "-test-" + connectionCounter)
            .withStatus(if (connectionCounter % 3 == 0) StandardSync.Status.INACTIVE else StandardSync.Status.ACTIVE)

        // Add tags and jobs to some connections
        if (connectionCounter % 2 == 0 && !tags!!.isEmpty()) {
          sync.setTags(mutableListOf<Tag?>(tags!![connectionCounter % tags!!.size]))
        }

        connectionServiceJooqImpl.writeStandardSync(sync)
        connectionIds.add(sync.connectionId)
        createJobForConnection(sync.connectionId, connectionCounter)
        connectionCounter++
      }
    }

    // Add a few more connections without jobs to test filtering
    val source = sources.get(0)
    val destination = destinations.get(0)
    for (i in 0..2) {
      val syncWithoutJobs =
        createStandardSync(source, destination, mutableListOf<ConfiguredAirbyteStream?>())
          .withName("conn-no-job-" + i)
          .withStatus(StandardSync.Status.ACTIVE)
      connectionServiceJooqImpl.writeStandardSync(syncWithoutJobs)
      connectionIds.add(syncWithoutJobs.connectionId)
    }

    val sourceDefinitionIds =
      sources
        .stream()
        .map<UUID?> { obj: SourceConnection? -> obj!!.getSourceDefinitionId() }
        .distinct()
        .collect(Collectors.toList())
    val destinationDefinitionIds =
      destinations
        .stream()
        .map<UUID?> { obj: DestinationConnection? -> obj!!.getDestinationDefinitionId() }
        .distinct()
        .collect(Collectors.toList())

    return ComprehensiveTestData(
      workspaceId,
      connectionIds,
      tagIds,
      sourceDefinitionIds,
      destinationDefinitionIds,
      connectionIds.size,
    )
  }

  private fun createSourceDefinition(
    definitionId: UUID?,
    name: String?,
  ) {
    // Check if actor_definition already exists
    val definitionExists: Boolean =
      database!!.query<Boolean?>(
        ContextQueryFunction { ctx: org.jooq.DSLContext? ->
          ctx!!.fetchExists(
            ctx!!
              .selectFrom<ActorDefinitionRecord?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
              .where(
                io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID
                  .eq(definitionId),
              ),
          )
        },
      )!!

    if (!definitionExists) {
      // Create the actor_definition entry
      database!!.query<Int?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .insertInto<ActorDefinitionRecord?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
            .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID, definitionId)
            .set<String?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.NAME, name)
            .set<ActorType?>(
              io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ACTOR_TYPE,
              ActorType.source,
            ).set<Boolean?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.TOMBSTONE, false)
            .execute()
        },
      )

      // Create the actor_definition_version entry
      val versionId = UUID.randomUUID()
      database!!.query<Int?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .insertInto<ActorDefinitionVersionRecord?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION)
            .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.ID, versionId)
            .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID, definitionId)
            .set<String?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, "test/" + name)
            .set<String?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, "latest")
            .set<JSONB?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.SPEC, JSONB.valueOf("{}"))
            .set<SupportLevel?>(
              io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL,
              SupportLevel.community,
            ).set<Long?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL, 100L)
            .execute()
        },
      )

      // Update the actor_definition to point to this version as default
      database!!.query<Int?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .update<ActorDefinitionRecord?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
            .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID, versionId)
            .where(
              io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID
                .eq(definitionId),
            ).execute()
        },
      )
    }
  }

  private fun createDestinationDefinition(
    definitionId: UUID?,
    name: String?,
  ) {
    // Check if actor_definition already exists
    val definitionExists: Boolean =
      database!!.query<Boolean?>(
        ContextQueryFunction { ctx: org.jooq.DSLContext? ->
          ctx!!.fetchExists(
            ctx!!
              .selectFrom<ActorDefinitionRecord?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
              .where(
                io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID
                  .eq(definitionId),
              ),
          )
        },
      )!!

    if (!definitionExists) {
      // Create the actor_definition entry
      database!!.query<Int?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .insertInto<ActorDefinitionRecord?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
            .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID, definitionId)
            .set<String?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.NAME, name)
            .set<ActorType?>(
              io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ACTOR_TYPE,
              ActorType.destination,
            ).set<Boolean?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.TOMBSTONE, false)
            .execute()
        },
      )

      // Create the actor_definition_version entry
      val versionId = UUID.randomUUID()
      database!!.query<Int?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .insertInto<ActorDefinitionVersionRecord?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION)
            .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.ID, versionId)
            .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID, definitionId)
            .set<String?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, "test/" + name)
            .set<String?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, "latest")
            .set<JSONB?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.SPEC, JSONB.valueOf("{}"))
            .set<SupportLevel?>(
              io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL,
              SupportLevel.community,
            ).set<Long?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL, 100L)
            .execute()
        },
      )

      // Update the actor_definition to point to this version as default
      database!!.query<Int?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .update<ActorDefinitionRecord?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
            .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID, versionId)
            .where(
              io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID
                .eq(definitionId),
            ).execute()
        },
      )
    }
  }

  @Throws(IOException::class)
  private fun createAdditionalSource(
    setupHelper: JooqTestDbSetupHelper,
    name: String?,
    sourceDefinitionId: UUID?,
  ): SourceConnection {
    val source =
      SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(setupHelper.workspace!!.getWorkspaceId())
        .withSourceDefinitionId(sourceDefinitionId)
        .withName(name)
        .withTombstone(false)

    database!!.query<Int?>(
      ContextQueryFunction { ctx: DSLContext? ->
        ctx!!
          .insertInto<ActorRecord?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR)
          .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.ID, source.getSourceId())
          .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.WORKSPACE_ID, source.getWorkspaceId())
          .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.ACTOR_DEFINITION_ID, source.getSourceDefinitionId())
          .set<String?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.NAME, source.getName())
          .set<JSONB?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.CONFIGURATION, JSONB.valueOf("{}"))
          .set<ActorType?>(
            io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.ACTOR_TYPE,
            ActorType.source,
          ).set<Boolean?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.TOMBSTONE, false)
          .execute()
      },
    )

    return source
  }

  @Throws(IOException::class)
  private fun createAdditionalDestination(
    setupHelper: JooqTestDbSetupHelper,
    name: String?,
    destinationDefinitionId: UUID?,
  ): DestinationConnection {
    val destination =
      DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(setupHelper.workspace!!.getWorkspaceId())
        .withDestinationDefinitionId(destinationDefinitionId)
        .withName(name)
        .withTombstone(false)

    database!!.query<Int?>(
      ContextQueryFunction { ctx: DSLContext? ->
        ctx!!
          .insertInto<ActorRecord?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR)
          .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.ID, destination!!.destinationId)
          .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.WORKSPACE_ID, destination.getWorkspaceId())
          .set<UUID?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.ACTOR_DEFINITION_ID, destination.getDestinationDefinitionId())
          .set<String?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.NAME, destination.getName())
          .set<JSONB?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.CONFIGURATION, JSONB.valueOf("{}"))
          .set<ActorType?>(
            io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.ACTOR_TYPE,
            ActorType.destination,
          ).set<Boolean?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.TOMBSTONE, false)
          .execute()
      },
    )

    return destination
  }

  private fun createJobForConnection(
    connectionId: UUID,
    jobVariant: Int,
  ) {
    // Create jobs with different statuses
    val baseTime = 1700000000000L // Fixed base timestamp
    val jobId = baseTime + (jobVariant * 1000L) + connectionId.hashCode()
    val createdAt = baseTime - (jobVariant * 3600000L) // Hours in milliseconds
    val updatedAt = createdAt + (jobVariant * 60000L) // Minutes in milliseconds

    // Determine job status based on variant to create diverse test scenarios
    val jobStatus: String
    val attemptStatus: String
    when (jobVariant % 4) {
      1, 3 -> {
        jobStatus = "failed"
        attemptStatus = "failed"
      }

      2 -> {
        jobStatus = "running"
        attemptStatus = "running"
      }

      else -> {
        jobStatus = "succeeded"
        attemptStatus = "succeeded"
      }
    }

    val createdAtTs = OffsetDateTime.ofInstant(Instant.ofEpochMilli(createdAt), ZoneOffset.UTC)
    val updatedAtTs = OffsetDateTime.ofInstant(Instant.ofEpochMilli(updatedAt), ZoneOffset.UTC)

    jobDatabase!!.query<Int?>(
      ContextQueryFunction { ctx: DSLContext? ->
        ctx!!.execute(
          "INSERT INTO jobs (id, config_type, scope, status, created_at, updated_at) " +
            "VALUES (?, 'sync', ?, ?::job_status, ?::timestamptz, ?::timestamptz)",
          jobId,
          connectionId.toString(),
          jobStatus,
          createdAtTs,
          updatedAtTs,
        )
      },
    )
    jobDatabase!!.query<Int?>(
      ContextQueryFunction { ctx: DSLContext? ->
        ctx!!.execute(
          "INSERT INTO attempts (id, job_id, status, created_at, updated_at) " +
            "VALUES (?, ?, ?::attempt_status, ?::timestamptz, ?::timestamptz)",
          jobId,
          jobId,
          attemptStatus,
          createdAtTs,
          updatedAtTs,
        )
      },
    )
  }

  private fun verifyResultsSorted(
    results: MutableList<ConnectionWithJobInfo>,
    sortKey: SortKey,
    ascending: Boolean,
    testDescription: String?,
  ) {
    for (i in 0..<results.size - 1) {
      val current = results.get(i)
      val next = results.get(i + 1)

      val comparison = compareResults(current, next, sortKey)

      if (ascending) {
        Assertions.assertTrue(
          comparison <= 0,
          testDescription + " - Results should be sorted ascending but found: " +
            getSortValue(current, sortKey) + " > " + getSortValue(next, sortKey),
        )
      } else {
        Assertions.assertTrue(
          comparison >= 0,
          testDescription + " - Results should be sorted descending but found: " +
            getSortValue(current, sortKey) + " < " + getSortValue(next, sortKey),
        )
      }
    }
  }

  private fun compareResults(
    a: ConnectionWithJobInfo,
    b: ConnectionWithJobInfo,
    sortKey: SortKey,
  ): Int =
    when (sortKey) {
      SortKey.CONNECTION_NAME -> a.connection().getName().compareTo(b.connection().getName())
      SortKey.SOURCE_NAME -> a.sourceName().compareTo(b.sourceName())
      SortKey.DESTINATION_NAME -> a.destinationName().compareTo(b.destinationName())
      SortKey.LAST_SYNC -> {
        if (a.latestJobCreatedAt().isEmpty && b.latestJobCreatedAt().isEmpty) {
          0
        } else if (a.latestJobCreatedAt().isEmpty) {
          -1
        } else if (b.latestJobCreatedAt().isEmpty) {
          1
        } else {
          a.latestJobCreatedAt().get().compareTo(b.latestJobCreatedAt().get())
        }
      }

      else -> 0
    }

  private fun getSortValue(
    result: ConnectionWithJobInfo,
    sortKey: SortKey,
  ): String? =
    when (sortKey) {
      SortKey.CONNECTION_NAME -> result.connection().getName()
      SortKey.SOURCE_NAME -> result.sourceName()
      SortKey.DESTINATION_NAME -> result.destinationName()
      SortKey.LAST_SYNC -> if (result.latestJobCreatedAt().isPresent) result.latestJobCreatedAt().get().toString() else "null"
      else -> null
    }

  private fun verifyResultsMatchFilters(
    results: List<ConnectionWithJobInfo>,
    filters: Filters?,
    testDescription: String?,
  ) {
    if (filters == null) return

    for (result in results) {
      // Verify search term filter
      if (filters.searchTerm != null && !filters.searchTerm.isEmpty()) {
        val searchTerm = filters.searchTerm.lowercase(Locale.getDefault())
        val matches =
          result
            .connection()
            .getName()
            .lowercase(Locale.getDefault())
            .contains(searchTerm) ||
            result.sourceName().lowercase(Locale.getDefault()).contains(searchTerm) ||
            result.destinationName().lowercase(Locale.getDefault()).contains(searchTerm)
        Assertions.assertTrue(
          matches,
          testDescription + " - Result should match search term '" +
            filters.searchTerm + "' but got connection: " + result.connection().getName() +
            ", source: " + result.sourceName() + ", destination: " + result.destinationName(),
        )
      }

      // Verify source definition ID filter
      if (filters.sourceDefinitionIds != null && !filters.sourceDefinitionIds.isEmpty()) {
        val sourceDefinitionId = getSourceDefinitionId(result.connection().getSourceId())
        val matchesSourceDef = filters.sourceDefinitionIds.contains(sourceDefinitionId)
        Assertions.assertTrue(
          matchesSourceDef,
          testDescription + " - Result should match source definition filter. Expected one of: " +
            filters.sourceDefinitionIds + " but got: " + sourceDefinitionId + " for connection: " + result.connection().getName(),
        )
      }

      // Verify destination definition ID filter
      if (filters.destinationDefinitionIds != null && !filters.destinationDefinitionIds.isEmpty()) {
        val destinationDefinitionId = getDestinationDefinitionId(result.connection().getDestinationId())
        val matchesDestDef = filters.destinationDefinitionIds.contains(destinationDefinitionId)
        Assertions.assertTrue(
          matchesDestDef,
          testDescription + " - Result should match destination definition filter. Expected one of: " +
            filters.destinationDefinitionIds + " but got: " + destinationDefinitionId + " for connection: " +
            result
              .connection()
              .getName(),
        )
      }

      // Verify status filter (job status)
      if (filters.statuses != null && !filters.statuses.isEmpty()) {
        if (result.latestJobStatus().isPresent) {
          val resultStatus = mapJobStatusToConnectionJobStatus(result.latestJobStatus().get())

          val matchesStatusFilter: Boolean
          if (filters.statuses.contains(ConnectionJobStatus.HEALTHY)) {
            // HEALTHY filter should include both HEALTHY and RUNNING (but not FAILED)
            matchesStatusFilter = resultStatus == ConnectionJobStatus.HEALTHY || resultStatus == ConnectionJobStatus.RUNNING
          } else {
            // For other filters (FAILED, RUNNING), require exact match
            matchesStatusFilter = filters.statuses.contains(resultStatus)
          }
          Assertions.assertTrue(
            matchesStatusFilter,
            testDescription + " - Status filter mismatch. " +
              "Filter: " + filters.statuses + ", Got: " + resultStatus + " for connection: " + result.connection().getName() +
              ". Note: HEALTHY filter includes both HEALTHY and RUNNING statuses.",
          )
        } else {
          // Connections without jobs are included in HEALTHY filter but should be excluded from FAILED and
          // RUNNING filters
          if (!filters.statuses.contains(ConnectionJobStatus.HEALTHY)) {
            Assertions.fail<Any?>(
              testDescription + " - Connection without job status should not appear in " +
                filters.statuses + " filter results: " + result.connection().getName(),
            )
          }
        }
      }

      // Verify state filter (connection active/inactive status)
      if (filters.states != null && !filters.states.isEmpty()) {
        val resultState = if (result.connection().getStatus() == StandardSync.Status.ACTIVE) ActorStatus.ACTIVE else ActorStatus.INACTIVE
        val matchesState = filters.states.contains(resultState)
        Assertions.assertTrue(
          matchesState,
          testDescription + " - Result should match state filter. Expected one of: " +
            filters.states + " but got: " + resultState + " for connection: " + result.connection().getName(),
        )
      }

      // Verify tag ID filter
      if (filters.tagIds != null && !filters.tagIds.isEmpty()) {
        val matchesTag: Boolean
        if (result.connection().tags != null && !result.connection().tags.isEmpty()) {
          val resultTagIds =
            result
              .connection()
              .tags
              .stream()
              .map<UUID?> { obj: Tag? -> obj!!.getTagId() }
              .toList()
          matchesTag = filters.tagIds.stream().anyMatch { o: UUID? -> resultTagIds.contains(o) }
        } else {
          matchesTag = false // Connection has no tags, so can't match tag filter
        }
        Assertions.assertTrue(
          matchesTag,
          testDescription + " - Result should match tag filter. Expected one of: " +
            filters.tagIds + " but connection has tags: " +
            (
              if (result.connection().tags != null) {
                result
                  .connection()
                  .tags
                  .stream()
                  .map<UUID?> { obj: Tag? -> obj!!.getTagId() }
                  .toList()
              } else {
                "none"
              }
            ) +
            " for connection: " + result.connection().getName(),
        )
      }
    }
  }

  private fun mapJobStatusToConnectionJobStatus(jobStatus: JobStatus): ConnectionJobStatus =
    when (jobStatus) {
      JobStatus.succeeded -> ConnectionJobStatus.HEALTHY
      JobStatus.failed, JobStatus.cancelled, JobStatus.incomplete -> ConnectionJobStatus.FAILED
      JobStatus.running -> ConnectionJobStatus.RUNNING
      JobStatus.pending -> ConnectionJobStatus.RUNNING
    }

  private fun getSourceDefinitionId(sourceId: UUID?): UUID? {
    try {
      return database!!.query<UUID?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .select<Any?>(DSL.field("actor_definition_id"))
            .from(DSL.table("actor"))
            .where(DSL.field("id").eq(sourceId))
            .and(DSL.field("actor_type").cast<String?>(String::class.java).eq("source"))
            .fetchOneInto<UUID?>(UUID::class.java)
        },
      )
    } catch (e: Exception) {
      throw RuntimeException("Failed to get source definition ID for source: " + sourceId, e)
    }
  }

  private fun getDestinationDefinitionId(destinationId: UUID?): UUID? {
    try {
      return database!!.query<UUID?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .select<Any?>(DSL.field("actor_definition_id"))
            .from(DSL.table("actor"))
            .where(DSL.field("id").eq(destinationId))
            .and(DSL.field("actor_type").cast<String?>(String::class.java).eq("destination"))
            .fetchOneInto<UUID?>(UUID::class.java)
        },
      )
    } catch (e: Exception) {
      throw RuntimeException("Failed to get destination definition ID for destination: " + destinationId, e)
    }
  }

  companion object {
    private val catalogHelpers = CatalogHelpers(FieldGenerator())

    private var container: PostgreSQLContainer<*>? = null

    @BeforeAll
    @JvmStatic
    fun setup() {
      container =
        PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
          .withDatabaseName("airbyte")
          .withUsername("docker")
          .withPassword("docker")
      container!!.start()
    }

    @JvmStatic
    private fun actorSyncsStreamTestProvider(): Stream<Arguments?> {
      // Mock "connections" - just a list of streams that the connection syncs
      val connectionSyncingStreamA = mutableListOf<String?>("stream_a")
      val connectionSyncingStreamB = mutableListOf<String?>("stream_b")
      val connectionSyncingBothStreamsAAndB = mutableListOf<String?>("stream_a", "stream_b")

      // Lists of mock "connections" for a given actor
      val connectionsListForActorWithNoConnections = mutableListOf<MutableList<String?>?>()
      val connectionsListForActorWithOneConnectionSyncingStreamA = mutableListOf<MutableList<String?>?>(connectionSyncingStreamA)
      val connectionsListForActorWithOneConnectionSyncingStreamB = mutableListOf<MutableList<String?>?>(connectionSyncingStreamB)
      val connectionsListForActorWithOneConnectionSyncingSyncingAAndBInOneConnection =
        mutableListOf<MutableList<String?>?>(connectionSyncingBothStreamsAAndB)
      val connectionsListForActorWithOneConnectionSyncingSyncingAAndBInSeparateConnections =
        mutableListOf<MutableList<String?>?>(connectionSyncingStreamA, connectionSyncingStreamB)

      return Stream.of<Arguments?>( // Single affected stream
        Arguments.of(connectionsListForActorWithNoConnections, mutableListOf<String?>("stream_a"), false),
        Arguments.of(connectionsListForActorWithOneConnectionSyncingStreamA, mutableListOf<String?>("stream_a"), true),
        Arguments.of(connectionsListForActorWithOneConnectionSyncingStreamB, mutableListOf<String?>("stream_a"), false),
        Arguments.of(connectionsListForActorWithOneConnectionSyncingSyncingAAndBInOneConnection, mutableListOf<String?>("stream_a"), true),
        Arguments.of(
          connectionsListForActorWithOneConnectionSyncingSyncingAAndBInSeparateConnections,
          mutableListOf<String?>("stream_a"),
          true,
        ), // Multiple affected streams
        Arguments.of(connectionsListForActorWithNoConnections, mutableListOf<String?>("stream_a", "stream_b"), false),
        Arguments.of(connectionsListForActorWithOneConnectionSyncingStreamA, mutableListOf<String?>("stream_a", "stream_b"), true),
        Arguments.of(connectionsListForActorWithOneConnectionSyncingStreamB, mutableListOf<String?>("stream_a", "stream_b"), true),
        Arguments.of(
          connectionsListForActorWithOneConnectionSyncingSyncingAAndBInOneConnection,
          mutableListOf<String?>("stream_a", "stream_b"),
          true,
        ),
        Arguments.of(
          connectionsListForActorWithOneConnectionSyncingSyncingAAndBInSeparateConnections,
          mutableListOf<String?>("stream_a", "stream_b"),
          true,
        ),
      )
    }

    @JvmStatic
    private fun orderByTestProvider(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(
          SortKey.CONNECTION_NAME,
          true,
          DSL
            .lower(io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION.NAME)
            .cast<String?>(
              String::class.java,
            ).asc(),
          io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION.ID
            .asc(),
        ),
        Arguments.of(
          SortKey.CONNECTION_NAME,
          false,
          DSL
            .lower(io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION.NAME)
            .cast<String?>(
              String::class.java,
            ).desc(),
          io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION.ID
            .desc(),
        ),
        Arguments.of(
          SortKey.SOURCE_NAME,
          true,
          DSL.lower(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.NAME).cast<String?>(String::class.java).asc(),
          io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION.ID
            .asc(),
        ),
        Arguments.of(
          SortKey.SOURCE_NAME,
          false,
          DSL.lower(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.NAME).cast<String?>(String::class.java).desc(),
          io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION.ID
            .desc(),
        ),
        Arguments.of(
          SortKey.DESTINATION_NAME,
          true,
          DSL
            .lower(
              io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR
                .`as`("dest_actor")
                .NAME,
            ).cast<String?>(
              String::class.java,
            ).asc(),
          io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION.ID
            .asc(),
        ),
        Arguments.of(
          SortKey.DESTINATION_NAME,
          false,
          DSL
            .lower(
              io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR
                .`as`("dest_actor")
                .NAME,
            ).cast<String?>(
              String::class.java,
            ).desc(),
          io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION.ID
            .desc(),
        ),
        Arguments.of(
          SortKey.LAST_SYNC,
          true,
          DSL.field(DSL.name("latest_jobs", "created_at")).asc().nullsFirst(),
          io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION.ID
            .asc(),
        ),
        Arguments.of(
          SortKey.LAST_SYNC,
          false,
          DSL.field(DSL.name("latest_jobs", "created_at")).desc().nullsLast(),
          io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION.ID
            .desc(),
        ),
      )

    @JvmStatic
    private fun sortKeyTestProvider(): Stream<Arguments?> {
      val testConnectionId = UUID.randomUUID()
      return Stream.of<Arguments?>(
        Arguments.of(SortKey.CONNECTION_NAME, "connA", null, null, null, testConnectionId),
        Arguments.of(SortKey.SOURCE_NAME, null, "sourceA", null, null, testConnectionId),
        Arguments.of(SortKey.DESTINATION_NAME, null, null, "destA", null, testConnectionId),
        Arguments.of(SortKey.LAST_SYNC, null, null, null, 1234567890L, testConnectionId),
      )
    }

    @JvmStatic
    private fun stateFiltersTestProvider(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(mutableListOf<ActorStatus?>(ActorStatus.ACTIVE), mutableListOf<String?>("status", "active")),
        Arguments.of(mutableListOf<ActorStatus?>(ActorStatus.INACTIVE), mutableListOf<String?>("status", "inactive")),
        Arguments.of(
          mutableListOf<ActorStatus?>(ActorStatus.ACTIVE, ActorStatus.INACTIVE),
          mutableListOf<String?>("status", "active", "inactive"),
        ),
        Arguments.of(mutableListOf<Any?>(), mutableListOf<Any?>()),
      )

    @JvmStatic
    private fun statusFiltersTestProvider(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(mutableListOf<ConnectionJobStatus?>(ConnectionJobStatus.HEALTHY), mutableListOf<String?>("status")),
        Arguments.of(mutableListOf<ConnectionJobStatus?>(ConnectionJobStatus.FAILED), mutableListOf<String?>("status", "failed")),
        Arguments.of(mutableListOf<ConnectionJobStatus?>(ConnectionJobStatus.RUNNING), mutableListOf<String?>("status", "running")),
        Arguments.of(
          mutableListOf<ConnectionJobStatus?>(ConnectionJobStatus.HEALTHY, ConnectionJobStatus.FAILED),
          mutableListOf<String?>("status"),
        ),
        Arguments.of(
          mutableListOf<ConnectionJobStatus?>(ConnectionJobStatus.FAILED, ConnectionJobStatus.RUNNING),
          mutableListOf<String?>("status", "failed", "running"),
        ),
        Arguments.of(
          mutableListOf<ConnectionJobStatus?>(ConnectionJobStatus.HEALTHY, ConnectionJobStatus.RUNNING),
          mutableListOf<String?>("status", "running"),
        ),
        Arguments.of(
          mutableListOf<ConnectionJobStatus?>(ConnectionJobStatus.HEALTHY, ConnectionJobStatus.FAILED, ConnectionJobStatus.RUNNING),
          mutableListOf<String?>("status"),
        ),
        Arguments.of(mutableListOf<Any?>(), mutableListOf<Any?>()),
      )

    @JvmStatic
    private fun connectionFilterConditionsTestProvider(): Stream<Arguments?> {
      val workspaceId = UUID.randomUUID()
      val sourceId = UUID.randomUUID()
      val destinationId = UUID.randomUUID()
      val sourceDefId = UUID.randomUUID()
      val destDefId = UUID.randomUUID()
      val tagId = UUID.randomUUID()

      return Stream.of<Arguments?>( // Basic query filters - no cursor
        Arguments.of(
          StandardSyncQuery(workspaceId, null, null, false),
          null,
          mutableListOf<String?>("workspace_id"),
          "Basic query with workspace filter only",
        ),
        Arguments.of(
          StandardSyncQuery(workspaceId, mutableListOf<UUID>(sourceId), mutableListOf<UUID>(destinationId), false),
          null,
          mutableListOf<String?>("workspace_id", "source_id", "destination_id"),
          "Basic query with source and destination filters",
        ),
        Arguments.of(
          StandardSyncQuery(workspaceId, null, null, true),
          null,
          mutableListOf<String?>("workspace_id"),
          "Basic query with includeDeleted=true (no deprecated filter)",
        ), // Cursor with no filters
        Arguments.of(
          StandardSyncQuery(workspaceId, null, null, false),
          Cursor(SortKey.CONNECTION_NAME, null, null, null, null, null, null, null, true, null),
          mutableListOf<String?>("workspace_id"),
          "Cursor with no filters",
        ), // Individual cursor filters
        Arguments.of(
          StandardSyncQuery(workspaceId, null, null, false),
          Cursor(
            SortKey.CONNECTION_NAME,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            Filters("search", null, null, null, null, null),
          ),
          mutableListOf<String?>("workspace_id", "name"),
          "Search term filter",
        ),
        Arguments.of(
          StandardSyncQuery(workspaceId, null, null, false),
          Cursor(
            SortKey.CONNECTION_NAME,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            Filters(null, mutableListOf<UUID>(sourceDefId), null, null, null, null),
          ),
          mutableListOf<String?>("workspace_id", "actor_definition_id"),
          "Source definition filter",
        ),
        Arguments.of(
          StandardSyncQuery(workspaceId, null, null, false),
          Cursor(
            SortKey.CONNECTION_NAME,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            Filters(null, null, mutableListOf<UUID>(destDefId), null, null, null),
          ),
          mutableListOf<String?>("workspace_id", "actor_definition_id"),
          "Destination definition filter",
        ),
        Arguments.of(
          StandardSyncQuery(workspaceId, null, null, false),
          Cursor(
            SortKey.CONNECTION_NAME,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            Filters(null, null, null, listOf(ConnectionJobStatus.HEALTHY), null, null),
          ),
          mutableListOf<String?>("workspace_id", "status"),
          "Status filter",
        ),
        Arguments.of(
          StandardSyncQuery(workspaceId, null, null, false),
          Cursor(
            SortKey.CONNECTION_NAME,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            Filters(null, null, null, null, listOf(ActorStatus.ACTIVE), null),
          ),
          mutableListOf<String?>("workspace_id", "status"),
          "State filter",
        ),
        Arguments.of(
          StandardSyncQuery(workspaceId, null, null, false),
          Cursor(
            SortKey.CONNECTION_NAME,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            Filters(null, null, null, null, null, mutableListOf<UUID>(tagId)),
          ),
          mutableListOf<String?>("workspace_id", "tag_id"),
          "Tag filter",
        ), // Combined filters
        Arguments.of(
          StandardSyncQuery(workspaceId, mutableListOf<UUID>(sourceId), mutableListOf<UUID>(destinationId), false),
          Cursor(
            SortKey.CONNECTION_NAME,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            Filters(
              "search",
              mutableListOf<UUID>(sourceDefId),
              mutableListOf<UUID>(destDefId),
              listOf(ConnectionJobStatus.HEALTHY),
              listOf(ActorStatus.ACTIVE),
              mutableListOf<UUID>(tagId),
            ),
          ),
          mutableListOf<String?>("workspace_id", "source_id", "destination_id", "name", "actor_definition_id", "status", "tag_id"),
          "All filters combined",
        ), // Empty filter lists (should not add conditions)
        Arguments.of(
          StandardSyncQuery(workspaceId, null, null, false),
          Cursor(
            SortKey.CONNECTION_NAME,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            Filters(
              null,
              mutableListOf<UUID>(),
              mutableListOf<UUID>(),
              listOf<ConnectionJobStatus>(),
              listOf<ActorStatus>(),
              mutableListOf<UUID>(),
            ),
          ),
          mutableListOf<String?>("workspace_id"),
          "Empty filter lists",
        ),
      )
    }

    @JvmStatic
    private fun buildCursorConditionLastSyncDescTestProvider(): Stream<Arguments?> {
      val connectionId = UUID.randomUUID()
      val anotherConnectionId = UUID.randomUUID()
      val lastSyncEpoch = 1704000000L // 2024-01-01 00:00:00 UTC
      val anotherLastSyncEpoch = 1704003600L // 2024-01-01 01:00:00 UTC

      return Stream.of<Arguments?>( // Null cursor - should return no condition
        Arguments.of(
          null,
          true,
          "null cursor returns no condition",
        ), // Cursor with all null values - should return no condition (first page)
        Arguments.of(
          fromValues(
            SortKey.LAST_SYNC,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            20,
            true,
            null,
          ).cursor,
          true,
          "cursor with all null values returns no condition",
        ), // Cursor with null connection ID - should return no condition
        Arguments.of(
          Cursor(
            SortKey.LAST_SYNC,
            "connection1",
            "source1",
            null,
            "destination1",
            null,
            lastSyncEpoch,
            null as UUID?,
            false,
            null as Filters?,
          ),
          true,
          "cursor with null connection ID returns no condition",
        ), // Cursor with connection ID but null lastSync - should return condition with null check
        Arguments.of(
          fromValues(
            SortKey.LAST_SYNC,
            "connection1",
            "source1",
            null,
            "destination1",
            null,
            null as Long?,
            connectionId,
            20,
            true,
            null as Filters?,
          ).cursor,
          false,
          "cursor with null lastSync returns condition with null check",
        ), // Cursor with connection ID and lastSync - should return condition with time comparison
        Arguments.of(
          fromValues(
            SortKey.LAST_SYNC,
            "connection1",
            "source1",
            null,
            "destination1",
            null,
            lastSyncEpoch,
            connectionId,
            20,
            true,
            null as Filters?,
          ).cursor,
          false,
          "cursor with lastSync returns condition with time comparison",
        ), // Cursor with different connection ID and lastSync - should return condition with time comparison
        Arguments.of(
          fromValues(
            SortKey.LAST_SYNC,
            "connection2",
            "source2",
            null,
            "destination2",
            null,
            anotherLastSyncEpoch,
            anotherConnectionId,
            20,
            true,
            null as Filters?,
          ).cursor,
          false,
          "cursor with different connection ID and lastSync returns condition with time comparison",
        ), // Cursor with zero lastSync - should return condition with time comparison
        Arguments.of(
          fromValues(
            SortKey.LAST_SYNC,
            "connection1",
            "source1",
            null,
            "destination1",
            null,
            0L,
            connectionId,
            20,
            true,
            null as Filters?,
          ).cursor,
          false,
          "cursor with zero lastSync returns condition with time comparison",
        ),
      )
    }

    @JvmStatic
    private fun paginationTestProvider(): Stream<Arguments?> {
      // Use the deterministic definition IDs that match createComprehensiveTestData
      val sourceDefId1 = UUID.fromString("11111111-1111-1111-1111-111111111111")
      val sourceDefId2 = UUID.fromString("22222222-2222-2222-2222-222222222222")
      val destDefId1 = UUID.fromString("33333333-3333-3333-3333-333333333333")
      val destDefId2 = UUID.fromString("44444444-4444-4444-4444-444444444444")

      return Stream.of<Arguments?>( // Test all sort keys
        Arguments.of(SortKey.CONNECTION_NAME, true, null, "Sort by connection name ascending"),
        Arguments.of(SortKey.CONNECTION_NAME, false, null, "Sort by connection name descending"),
        Arguments.of(SortKey.SOURCE_NAME, true, null, "Sort by source name ascending"),
        Arguments.of(SortKey.SOURCE_NAME, false, null, "Sort by source name descending"),
        Arguments.of(SortKey.DESTINATION_NAME, true, null, "Sort by destination name ascending"),
        Arguments.of(SortKey.DESTINATION_NAME, false, null, "Sort by destination name descending"),
        Arguments.of(SortKey.LAST_SYNC, true, null, "Sort by last sync ascending"),
        Arguments.of(SortKey.LAST_SYNC, false, null, "Sort by last sync descending"), // Test various filters
        Arguments.of(
          SortKey.CONNECTION_NAME,
          true,
          Filters("conn", null, null, null, null, null),
          "Search filter",
        ),
        Arguments.of(
          SortKey.CONNECTION_NAME,
          true,
          Filters(null, null, null, listOf(ConnectionJobStatus.HEALTHY), null, null),
          "Status filter - HEALTHY",
        ),
        Arguments.of(
          SortKey.CONNECTION_NAME,
          true,
          Filters(null, null, null, listOf(ConnectionJobStatus.FAILED), null, null),
          "Status filter - FAILED",
        ),
        Arguments.of(
          SortKey.CONNECTION_NAME,
          true,
          Filters(null, null, null, listOf(ConnectionJobStatus.RUNNING), null, null),
          "Status filter - RUNNING",
        ),
        Arguments.of(
          SortKey.CONNECTION_NAME,
          true,
          Filters(null, null, null, null, listOf(ActorStatus.ACTIVE), null),
          "State filter - ACTIVE",
        ),
        Arguments.of(
          SortKey.CONNECTION_NAME,
          true,
          Filters(null, null, null, null, listOf(ActorStatus.INACTIVE), null),
          "State filter - INACTIVE",
        ), // Test combined filters
        Arguments.of(
          SortKey.CONNECTION_NAME,
          true,
          Filters(
            "conn",
            null,
            null,
            listOf(ConnectionJobStatus.HEALTHY),
            listOf(ActorStatus.ACTIVE),
            null,
          ),
          "Combined filters",
        ), // Test different sort keys with filters
        Arguments.of(
          SortKey.SOURCE_NAME,
          true,
          Filters(null, null, null, listOf(ConnectionJobStatus.HEALTHY), null, null),
          "Source sort with status filter",
        ),
        Arguments.of(
          SortKey.DESTINATION_NAME,
          false,
          Filters(null, null, null, null, listOf(ActorStatus.ACTIVE), null),
          "Destination sort with state filter",
        ),
        Arguments.of(
          SortKey.LAST_SYNC,
          true,
          Filters("test", null, null, null, null, null),
          "Last sync sort with search filter",
        ), // Source and destination definition ID filters - now using actual deterministic IDs
        Arguments.of(
          SortKey.CONNECTION_NAME,
          true,
          Filters(null, mutableListOf<UUID>(sourceDefId1), null, null, null, null),
          "Source definition filter - sourceDefId1",
        ),
        Arguments.of(
          SortKey.CONNECTION_NAME,
          true,
          Filters(null, mutableListOf<UUID>(sourceDefId2), null, null, null, null),
          "Source definition filter - sourceDefId2",
        ),
        Arguments.of(
          SortKey.CONNECTION_NAME,
          true,
          Filters(null, null, mutableListOf<UUID>(destDefId1), null, null, null),
          "Destination definition filter - destDefId1",
        ),
        Arguments.of(
          SortKey.CONNECTION_NAME,
          true,
          Filters(null, null, mutableListOf<UUID>(destDefId2), null, null, null),
          "Destination definition filter - destDefId2",
        ),
        Arguments.of(
          SortKey.SOURCE_NAME,
          false,
          Filters(
            null,
            mutableListOf<UUID>(sourceDefId1),
            mutableListOf<UUID>(destDefId1),
            null,
            null,
            null,
          ),
          "Combined definition filters",
        ),
      )
    }
  }
}
