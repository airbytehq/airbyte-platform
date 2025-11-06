/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
class V1_8_1_003__AddJobIdToConnectionTimelineEventTest : AbstractConfigsDatabaseTest() {
  companion object {
    private const val CONNECTION_TIMELINE_EVENT_TABLE = "connection_timeline_event"
    private const val JOB_ID_COLUMN = "job_id"
  }

  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V1_8_1_003__AddJobIdToConnectionTimelineEventTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V1_8_1_002__AddNameToOrchestrationTask()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun `test migration adds job_id column with null values for existing rows`() {
    val ctx = dslContext!!
    val now = OffsetDateTime.now()

    // Temporarily disable foreign key constraints for testing (PostgreSQL)
    ctx.execute("SET session_replication_role = replica")

    // Insert a row BEFORE migration (without job_id column)
    val existingEventId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()
    ctx
      .insertInto(DSL.table(CONNECTION_TIMELINE_EVENT_TABLE))
      .columns(
        DSL.field("id"),
        DSL.field("connection_id"),
        DSL.field("event_type"),
        DSL.field("created_at"),
      ).values(
        existingEventId,
        connectionId,
        "existing_event",
        now,
      ).execute()

    // Run the migration to add job_id column
    V1_8_1_003__AddJobIdToConnectionTimelineEvent.addJobIdColumn(ctx)

    // Verify existing row has null value for job_id
    val existingRow =
      ctx
        .selectFrom(DSL.table(CONNECTION_TIMELINE_EVENT_TABLE))
        .where(DSL.field("id").eq(existingEventId))
        .fetchOne()

    Assertions.assertNotNull(existingRow)
    Assertions.assertNull(existingRow?.get(JOB_ID_COLUMN), "Existing row should have null job_id")

    // Insert a new row WITH job_id value after migration
    val newEventId = UUID.randomUUID()
    val jobId = 12345L
    ctx
      .insertInto(DSL.table(CONNECTION_TIMELINE_EVENT_TABLE))
      .columns(
        DSL.field("id"),
        DSL.field("connection_id"),
        DSL.field("event_type"),
        DSL.field("created_at"),
        DSL.field(JOB_ID_COLUMN, SQLDataType.BIGINT),
      ).values(
        newEventId,
        connectionId,
        "new_event",
        now,
        jobId,
      ).execute()

    // Verify new row has the job_id value set
    val newRow =
      ctx
        .selectFrom(DSL.table(CONNECTION_TIMELINE_EVENT_TABLE))
        .where(DSL.field("id").eq(newEventId))
        .fetchOne()

    Assertions.assertNotNull(newRow)
    Assertions.assertEquals(jobId, newRow?.get(JOB_ID_COLUMN), "New row should have job_id set")

    // Re-enable foreign key constraints
    ctx.execute("SET session_replication_role = origin")
  }
}
