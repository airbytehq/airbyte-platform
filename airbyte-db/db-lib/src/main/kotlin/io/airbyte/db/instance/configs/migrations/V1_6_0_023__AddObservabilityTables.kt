/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.DSL.constraint
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_023__AddObservabilityTables : BaseJavaMigration() {
  companion object {
    private const val OBSERVABILITY_JOBS_STATS_TABLE = "observability_jobs_stats"
    private const val ORGANIZATION_ID = "organization_id"
    private const val WORKSPACE_ID = "workspace_id"
    private const val CONNECTION_ID = "connection_id"
    private const val SOURCE_ID = "source_id"
    private const val SOURCE_DEFINITION_ID = "source_definition_id"
    private const val SOURCE_IMAGE_TAG = "source_image_tag"
    private const val DESTINATION_ID = "destination_id"
    private const val DESTINATION_DEFINITION_ID = "destination_definition_id"
    private const val DESTINATION_IMAGE_TAG = "destination_image_tag"
    private const val JOB_ID = "job_id"
    private const val CREATED_AT = "created_at"
    private const val JOB_TYPE = "job_type"
    private const val STATUS = "status"
    private const val ATTEMPT_COUNT = "attempt_count"
    private const val DURATION_SECONDS = "duration_seconds"

    private const val OBSERVABILITY_STREAM_STATS_TABLE = "observability_stream_stats"
    private const val STREAM_NAME = "stream_name"
    private const val STREAM_NAMESPACE = "stream_namespace"
    private const val BYTES_LOADED = "bytes_loaded"
    private const val RECORDS_LOADED = "records_loaded"
    private const val RECORDS_REJECTED = "records_rejected"
    private const val WAS_BACKFILLED = "was_backfilled"
    private const val WAS_RESUMED = "was_resumed"
  }

  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx: DSLContext = DSL.using(context.connection)

    createObsJobsStats(ctx)
    createObsStreamStats(ctx)
  }

  fun createObsJobsStats(ctx: DSLContext) {
    ctx
      .createTableIfNotExists(OBSERVABILITY_JOBS_STATS_TABLE)
      .column(JOB_ID, SQLDataType.BIGINT.notNull())
      .column(CONNECTION_ID, SQLDataType.UUID.notNull())
      .column(WORKSPACE_ID, SQLDataType.UUID.notNull())
      .column(ORGANIZATION_ID, SQLDataType.UUID.notNull())
      .column(SOURCE_ID, SQLDataType.UUID.notNull())
      .column(SOURCE_DEFINITION_ID, SQLDataType.UUID.notNull())
      .column(SOURCE_IMAGE_TAG, SQLDataType.VARCHAR)
      .column(DESTINATION_ID, SQLDataType.UUID.notNull())
      .column(DESTINATION_DEFINITION_ID, SQLDataType.UUID.notNull())
      .column(DESTINATION_IMAGE_TAG, SQLDataType.VARCHAR)
      .column(CREATED_AT, SQLDataType.TIMESTAMPWITHTIMEZONE.notNull())
      .column(JOB_TYPE, SQLDataType.VARCHAR)
      .column(STATUS, SQLDataType.VARCHAR)
      .column(ATTEMPT_COUNT, SQLDataType.INTEGER)
      .column(DURATION_SECONDS, SQLDataType.BIGINT)
      .primaryKey(JOB_ID)
      .execute()

    ctx
      .createIndexIfNotExists("${OBSERVABILITY_JOBS_STATS_TABLE}__${JOB_ID}__idx")
      .on(DSL.table(OBSERVABILITY_JOBS_STATS_TABLE), DSL.field(JOB_ID))
      .execute()
    ctx
      .createIndexIfNotExists("${OBSERVABILITY_JOBS_STATS_TABLE}__${CONNECTION_ID}_${CREATED_AT}__idx")
      .on(DSL.table(OBSERVABILITY_JOBS_STATS_TABLE), DSL.field(CONNECTION_ID), DSL.field(CREATED_AT))
      .execute()
  }

  fun createObsStreamStats(ctx: DSLContext) {
    ctx
      .createTableIfNotExists(OBSERVABILITY_STREAM_STATS_TABLE)
      .column(JOB_ID, SQLDataType.BIGINT.notNull())
      .column(STREAM_NAMESPACE, SQLDataType.VARCHAR)
      .column(STREAM_NAME, SQLDataType.VARCHAR.notNull())
      .column(BYTES_LOADED, SQLDataType.BIGINT.notNull())
      .column(RECORDS_LOADED, SQLDataType.BIGINT.notNull())
      .column(RECORDS_REJECTED, SQLDataType.BIGINT.notNull())
      .column(WAS_BACKFILLED, SQLDataType.BOOLEAN.notNull())
      .column(WAS_RESUMED, SQLDataType.BOOLEAN.notNull())
      .execute()

    ctx
      .alterTable(OBSERVABILITY_STREAM_STATS_TABLE)
      .add(
        constraint("${OBSERVABILITY_STREAM_STATS_TABLE}__${JOB_ID}__${STREAM_NAMESPACE}__${STREAM_NAME}__uq")
          .unique(DSL.field(JOB_ID), DSL.field(STREAM_NAMESPACE), DSL.field(STREAM_NAME)),
      ).execute()

    ctx
      .createUniqueIndexIfNotExists("${OBSERVABILITY_STREAM_STATS_TABLE}__${JOB_ID}__${STREAM_NAME}__uq")
      .on(DSL.table(OBSERVABILITY_STREAM_STATS_TABLE), DSL.field(JOB_ID), DSL.field(STREAM_NAME))
      .where(DSL.field(STREAM_NAMESPACE).isNull)
      .execute()

    ctx
      .createIndexIfNotExists("${OBSERVABILITY_STREAM_STATS_TABLE}__${JOB_ID}__idx")
      .on(DSL.table(OBSERVABILITY_STREAM_STATS_TABLE), DSL.field(JOB_ID))
      .execute()
  }
}
