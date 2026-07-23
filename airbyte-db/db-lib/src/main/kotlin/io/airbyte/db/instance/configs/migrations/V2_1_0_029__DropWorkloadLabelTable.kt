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
 * Drops the legacy workload_label table. Labels are now stored as a JSONB column
 * on the workload table (added in V2_1_0_006). All read and write paths were
 * migrated off this table in a prior release.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_029__DropWorkloadLabelTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx: DSLContext = DSL.using(context.connection)
    ctx.execute("DROP INDEX IF EXISTS workload_label_workload_id_idx")
    ctx.execute("DROP TABLE IF EXISTS workload_label")
  }
}
