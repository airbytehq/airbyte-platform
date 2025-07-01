/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.exception.IntegrityConstraintViolationException
import org.jooq.impl.DSL
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V1_1_0_011__CreateConnectionTagTableTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V1_1_0_011__CreateConnectionTagTableTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V1_1_0_010__CreateTagTable()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
    val ctx = dslContext!!
    V1_1_0_011__CreateConnectionTagTable.migrate(ctx)
  }

  @AfterEach
  fun teardown() {
    // Fully tear down db after each test
    val dslContext = dslContext!!
    dslContext.dropSchemaIfExists("public").cascade().execute()
    dslContext.createSchema("public").execute()
    dslContext.setSchema("public").execute()
  }

  @Test
  fun testInsertConnectionTag() {
    val ctx = dslContext!!
    val tagId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    // Inserting a connection_tag relationship should succeed
    Assertions.assertDoesNotThrow {
      ctx
        .insertInto(DSL.table(CONNECTION_TAG_TABLE))
        .columns(
          DSL.field(ID),
          DSL.field(TAG_ID),
          DSL.field(CONNECTION_ID),
        ).values(
          UUID.randomUUID(),
          tagId,
          connectionId,
        ).execute()
    }
  }

  @Test
  fun testUniqueTagConnectionConstraint() {
    val ctx = dslContext!!
    val tagId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    // Adding the first connection_tag relationship should succeed
    Assertions.assertDoesNotThrow {
      ctx
        .insertInto(DSL.table(CONNECTION_TAG_TABLE))
        .columns(
          DSL.field(ID),
          DSL.field(TAG_ID),
          DSL.field(CONNECTION_ID),
        ).values(
          UUID.randomUUID(),
          tagId,
          connectionId,
        ).execute()
    }

    // But adding a duplicate connection_tag relationship should fail
    val e: Exception =
      Assertions.assertThrows(
        IntegrityConstraintViolationException::class.java,
      ) {
        ctx
          .insertInto(DSL.table(CONNECTION_TAG_TABLE))
          .columns(
            DSL.field(ID),
            DSL.field(TAG_ID),
            DSL.field(CONNECTION_ID),
          ).values(
            UUID.randomUUID(),
            tagId,
            connectionId,
          ).execute()
      }
    Assertions
      .assertTrue(e.message!!.contains("ERROR: duplicate key value violates unique constraint \"connection_tag_tag_id_connection_id_key\""))
  }

  companion object {
    private const val CONNECTION_TAG_TABLE = "connection_tag"
    private const val ID = "id"
    private const val TAG_ID = "tag_id"
    private const val CONNECTION_ID = "connection_id"
  }
}
