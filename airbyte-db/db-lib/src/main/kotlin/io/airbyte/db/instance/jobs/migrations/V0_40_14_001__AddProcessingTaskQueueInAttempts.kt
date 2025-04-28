/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Add task queue to attempt.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_14_001__AddProcessingTaskQueueInAttempts : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx = DSL.using(context.connection)
    addProtocolVersionColumn(ctx)
  }
}

private fun addProtocolVersionColumn(ctx: DSLContext) {
  ctx
    .alterTable("attempts")
    .addColumnIfNotExists(
      DSL.field("processing_task_queue", SQLDataType.VARCHAR(255).nullable(true)),
    ).execute()
}
