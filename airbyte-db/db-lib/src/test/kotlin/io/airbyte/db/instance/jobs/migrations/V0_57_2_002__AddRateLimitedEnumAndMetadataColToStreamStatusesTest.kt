/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.development.DevDatabaseMigrator
import io.airbyte.db.instance.jobs.AbstractJobsDatabaseTest
import io.airbyte.db.instance.jobs.JobsDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.stream.Collectors

@Suppress("ktlint:standard:class-naming")
internal class V0_57_2_002__AddRateLimitedEnumAndMetadataColToStreamStatusesTest : AbstractJobsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_57_2_001__AddRefreshJobType",
        JobsDatabaseMigrator.DB_IDENTIFIER,
        JobsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val jobsDatabaseMigrator = JobsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_57_2_001__AddRefreshJobType()
    val devConfigsDbMigrator = DevDatabaseMigrator(jobsDatabaseMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun test() {
    val context = getDslContext()
    Assertions.assertFalse(metadataColumnExists(context))
    Assertions.assertFalse(rateLimitedEnumExists(context))
    V0_57_2_002__AddRateLimitedEnumAndMetadataColToStreamStatuses.migrate(context)
    Assertions.assertTrue(metadataColumnExists(context))
    Assertions.assertTrue(rateLimitedEnumExists(context))
  }

  companion object {
    private fun metadataColumnExists(ctx: DSLContext): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.columns")
          .where(
            DSL
              .field("table_name")
              .eq("stream_statuses")
              .and(DSL.field("column_name").eq("metadata")),
          ),
      )

    private fun rateLimitedEnumExists(ctx: DSLContext): Boolean =
      ctx
        .resultQuery("SELECT enumlabel FROM pg_enum WHERE enumtypid = 'job_stream_status_run_state'::regtype")
        .fetch()
        .stream()
        .map { c: Record -> c.getValue("enumlabel") }
        .collect(Collectors.toSet())
        .contains("rate_limited")
  }
}
