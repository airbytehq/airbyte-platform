/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Add `connect` to the `workload_type` enum.
 *
 * Postgres cannot remove a label from an enum, so this is not reversible in place. It is safe to
 * leave applied on rollback — no rows are written here, and nothing creates connect workloads
 * until the connect command ships.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_048__AddConnectWorkloadType : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    runMigration(ctx)
  }

  companion object {
    fun runMigration(ctx: DSLContext) {
      ctx.execute("ALTER TYPE workload_type ADD VALUE IF NOT EXISTS 'connect'")
    }
  }
}
