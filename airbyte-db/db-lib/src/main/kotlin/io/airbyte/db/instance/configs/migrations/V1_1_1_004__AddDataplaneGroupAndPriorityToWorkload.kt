/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
class V1_1_1_004__AddDataplaneGroupAndPriorityToWorkload : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addDataplaneGroup(ctx)
    addPriority(ctx)
  }

  companion object {
    fun addDataplaneGroup(ctx: DSLContext) {
      ctx
        .alterTable("workload")
        .addColumnIfNotExists("dataplane_group", SQLDataType.VARCHAR(256).nullable(true))
        .execute()
    }

    fun addPriority(ctx: DSLContext) {
      ctx
        .alterTable("workload")
        .addColumnIfNotExists("priority", SQLDataType.INTEGER.nullable(true))
        .execute()
    }
  }
}
