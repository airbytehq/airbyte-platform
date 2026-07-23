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
 * Drops the unused orchestration tables and their enum types (created in V2_0_0_002/003/005/006/007).
 *
 * The orchestration feature (API, data layer, entitlement, and feature flag) was removed before
 * release and nothing ever wrote to these tables, so they are empty in every environment.
 *
 * Not reversible: the tables and enum types are dropped outright. There is no data to preserve and
 * the feature is gone, so there is nothing to roll back to.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_033__DropOrchestrationTables : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx: DSLContext = DSL.using(context.connection)

    // Drop in foreign-key dependency order: children before the tables they reference.
    ctx.execute("DROP TABLE IF EXISTS orchestration_task_run")
    ctx.execute("DROP TABLE IF EXISTS orchestration_task")
    ctx.execute("DROP TABLE IF EXISTS orchestration_run")
    ctx.execute("DROP TABLE IF EXISTS orchestration")

    // Enum types can only be dropped once their owning columns are gone.
    ctx.execute("DROP TYPE IF EXISTS orchestration_task_run_status")
    ctx.execute("DROP TYPE IF EXISTS orchestration_task_type")
    ctx.execute("DROP TYPE IF EXISTS orchestration_run_status")
  }
}
