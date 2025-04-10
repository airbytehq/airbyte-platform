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

private val log = KotlinLogging.logger { }

/**
 * Add attempt sync config migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_28_001__AddAttemptSyncConfig : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addAttemptSyncConfigToAttempts(ctx)
  }
}

private fun addAttemptSyncConfigToAttempts(ctx: DSLContext) {
  ctx
    .alterTable("attempts")
    .addColumnIfNotExists(
      DSL.field("attempt_sync_config", SQLDataType.JSONB.nullable(true)),
    ).execute()
}
