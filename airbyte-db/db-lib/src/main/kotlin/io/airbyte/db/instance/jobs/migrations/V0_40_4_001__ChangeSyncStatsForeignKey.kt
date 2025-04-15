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
 * Add sync stats migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_4_001__ChangeSyncStatsForeignKey : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    changeForeignKeyType(ctx)
  }
}

private fun changeForeignKeyType(ctx: DSLContext) {
  ctx
    .alterTable("sync_stats")
    .alter("attempt_id")
    .set(SQLDataType.BIGINT.nullable(false))
    .execute()
}
