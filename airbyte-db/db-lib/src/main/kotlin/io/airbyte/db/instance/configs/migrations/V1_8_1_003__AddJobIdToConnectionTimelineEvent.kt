/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_8_1_003__AddJobIdToConnectionTimelineEvent : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx: DSLContext = DSL.using(context.connection)

    // Add nullable job_id column as int8/long
    addJobIdColumn(ctx)

    // Note: Concurrent index creation will be done in a separate migration
    // since it cannot run inside a transaction while column addition requires one
  }

  companion object {
    private const val TABLE_NAME: String = "connection_timeline_event"
    private val jobIdField = DSL.field("job_id", SQLDataType.BIGINT.nullable(true))

    @JvmStatic
    fun addJobIdColumn(ctx: DSLContext) {
      ctx
        .alterTable(TABLE_NAME)
        .addColumnIfNotExists(jobIdField)
        .execute()
    }
  }
}
