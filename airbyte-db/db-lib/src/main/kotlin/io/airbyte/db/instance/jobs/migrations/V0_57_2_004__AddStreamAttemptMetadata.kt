/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_57_2_004__AddStreamAttemptMetadata : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    val id = DSL.field("id", SQLDataType.UUID.notNull())
    val attemptId = DSL.field("attempt_id", SQLDataType.INTEGER.nullable(false))
    val streamNamespace = DSL.field("stream_namespace", SQLDataType.VARCHAR)
    val streamName = DSL.field("stream_name", SQLDataType.VARCHAR.notNull())
    val wasBackfilled = DSL.field("was_backfilled", SQLDataType.BOOLEAN.nullable(false).defaultValue(false))
    val wasResumed = DSL.field("was_resumed", SQLDataType.BOOLEAN.nullable(false).defaultValue(false))

    ctx
      .createTableIfNotExists(STREAM_ATTEMPT_METADATA_TABLE_NAME)
      .columns(id, attemptId, streamNamespace, streamName, wasBackfilled, wasResumed)
      .constraints(
        DSL.primaryKey(id),
        DSL.foreignKey(attemptId).references("attempts", "id").onDeleteCascade(),
      ).execute()

    // We expect attemptId based look ups
    ctx
      .createIndexIfNotExists("stream_attempt_metadata__attempt_id_idx")
      .on(STREAM_ATTEMPT_METADATA_TABLE_NAME, attemptId.name)
      .execute()

    // Uniqueness constraint on name, namespace per attempt to avoid duplicates
    ctx
      .createUniqueIndexIfNotExists("stream_attempt_metadata__attempt_id_name_namespace_idx")
      .on(STREAM_ATTEMPT_METADATA_TABLE_NAME, attemptId.name, streamNamespace.name, streamName.name)
      .where(streamNamespace.isNotNull())
      .execute()

    // Workaround for namespace being null and pg dropping null values from indexes
    ctx
      .createUniqueIndexIfNotExists("stream_attempt_metadata__attempt_id_name_idx")
      .on(STREAM_ATTEMPT_METADATA_TABLE_NAME, attemptId.name, streamName.name)
      .where(streamNamespace.isNull())
      .execute()
  }
}

private const val STREAM_ATTEMPT_METADATA_TABLE_NAME = "stream_attempt_metadata"
