/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_39_17_001__AddStreamDescriptorsToStateTableTest : AbstractConfigsDatabaseTest() {
  private lateinit var connection1: UUID
  private lateinit var connection2: UUID

  @Test
  fun testSimpleMigration() {
    val context = getDslContext()

    // Adding a couple of states
    context
      .insertInto(DSL.table(STATE_TABLE))
      .columns(
        DSL.field("id"),
        DSL.field("connection_id"),
      ).values(UUID.randomUUID(), connection1)
      .values(UUID.randomUUID(), connection2)
      .execute()

    // Preconditions check: we should have one row in state
    Assertions.assertEquals(2, context.select().from(STATE_TABLE).execute())

    // Applying the migration
    devConfigsDbMigrator!!.migrate()

    val newState = UUID.randomUUID()
    context
      .insertInto(DSL.table(STATE_TABLE))
      .columns(
        DSL.field("id"),
        DSL.field("connection_id"),
        DSL.field("stream_name"),
      ).values(newState, connection1, "new_stream")
      .execute()

    log.info { context.selectFrom("connection").fetch().toString() }
    log.info { context.selectFrom(STATE_TABLE).fetch().toString() }

    // Our two initial rows and the new row should be LEGACY
    Assertions.assertEquals(
      3,
      context
        .select()
        .from(STATE_TABLE)
        .where(DSL.field("type").equal(V0_39_17_001__AddStreamDescriptorsToStateTable.StateType.LEGACY))
        .execute(),
    )

    // There should be no STREAM or GLOBAL
    Assertions.assertEquals(
      0,
      context
        .select()
        .from(STATE_TABLE)
        .where(
          DSL.field("type").`in`(
            V0_39_17_001__AddStreamDescriptorsToStateTable.StateType.GLOBAL,
            V0_39_17_001__AddStreamDescriptorsToStateTable.StateType.STREAM,
          ),
        ).execute(),
    )
  }

  @Test
  fun testUniquenessConstraint() {
    devConfigsDbMigrator!!.migrate()

    val context = getDslContext()
    context
      .insertInto(DSL.table(STATE_TABLE))
      .columns(
        DSL.field("id"),
        DSL.field("connection_id"),
        DSL.field("type"),
        DSL.field("stream_name"),
        DSL.field("namespace"),
      ).values(
        UUID.randomUUID(),
        connection1,
        V0_39_17_001__AddStreamDescriptorsToStateTable.StateType.GLOBAL,
        "stream1",
        "ns2",
      ).execute()

    context
      .insertInto(DSL.table(STATE_TABLE))
      .columns(
        DSL.field("id"),
        DSL.field("connection_id"),
        DSL.field("type"),
        DSL.field("stream_name"),
        DSL.field("namespace"),
      ).values(
        UUID.randomUUID(),
        connection1,
        V0_39_17_001__AddStreamDescriptorsToStateTable.StateType.GLOBAL,
        "stream1",
        "ns1",
      ).execute()

    context
      .insertInto(DSL.table(STATE_TABLE))
      .columns(
        DSL.field("id"),
        DSL.field("connection_id"),
        DSL.field("type"),
        DSL.field("stream_name"),
        DSL.field("namespace"),
      ).values(
        UUID.randomUUID(),
        connection1,
        V0_39_17_001__AddStreamDescriptorsToStateTable.StateType.GLOBAL,
        "stream2",
        "ns2",
      ).execute()

    Assertions.assertThrows(
      DataAccessException::class.java,
    ) {
      context
        .insertInto(
          DSL.table(
            STATE_TABLE,
          ),
        ).columns(
          DSL.field("id"),
          DSL.field("connection_id"),
          DSL.field("type"),
          DSL.field("stream_name"),
          DSL.field("namespace"),
        ).values(
          UUID.randomUUID(),
          connection1,
          V0_39_17_001__AddStreamDescriptorsToStateTable.StateType.GLOBAL,
          "stream1",
          "ns2",
        ).execute()
    }
  }

  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_39_17_001__AddStreamDescriptorsToStateTableTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_39_1_001__CreateStreamReset()
    devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator!!.createBaseline()
    injectMockData()
  }

  @AfterEach
  fun afterEach() {
    // Making sure we reset between tests
    dslContext.dropSchemaIfExists("public").cascade().execute()
    dslContext.createSchema("public").execute()
    dslContext.setSchema("public").execute()
  }

  private fun injectMockData() {
    val context = getDslContext()

    val workspaceId = UUID.randomUUID()
    val actorId = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    connection1 = UUID.randomUUID()
    connection2 = UUID.randomUUID()

    context
      .insertInto(DSL.table("workspace"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("slug"),
        DSL.field("initial_setup_complete"),
      ).values(
        workspaceId,
        "base workspace",
        "base_workspace",
        true,
      ).execute()
    context
      .insertInto(DSL.table("actor_definition"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("docker_repository"),
        DSL.field("docker_image_tag"),
        DSL.field("actor_type"),
        DSL.field("spec"),
      ).values(
        actorDefinitionId,
        "Jenkins",
        "farosai/airbyte-jenkins-source",
        "0.1.23",
        V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
        JSONB.valueOf("{}"),
      ).execute()
    context
      .insertInto(DSL.table("actor"))
      .columns(
        DSL.field("id"),
        DSL.field("workspace_id"),
        DSL.field("actor_definition_id"),
        DSL.field("name"),
        DSL.field("configuration"),
        DSL.field("actor_type"),
      ).values(
        actorId,
        workspaceId,
        actorDefinitionId,
        "ActorName",
        JSONB.valueOf("{}"),
        V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
      ).execute()

    insertConnection(context, connection1, actorId)
    insertConnection(context, connection2, actorId)
  }

  private fun insertConnection(
    context: DSLContext,
    connectionId: UUID,
    actorId: UUID,
  ) {
    context
      .insertInto(DSL.table("connection"))
      .columns(
        DSL.field("id"),
        DSL.field("namespace_definition"),
        DSL.field("source_id"),
        DSL.field("destination_id"),
        DSL.field("name"),
        DSL.field("catalog"),
        DSL.field("manual"),
      ).values(
        connectionId,
        V0_32_8_001__AirbyteConfigDatabaseDenormalization.NamespaceDefinitionType.source,
        actorId,
        actorId,
        "Connection$connectionId",
        JSONB.valueOf("{}"),
        true,
      ).execute()
  }

  private lateinit var devConfigsDbMigrator: DevDatabaseMigrator

  companion object {
    private const val STATE_TABLE = "State"
  }
}
