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
class V0_50_33_007__AddGeographyColumnToWorkload : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    addGeographyColumnToWorkload(ctx)
  }

  companion object {
    private fun addGeographyColumnToWorkload(ctx: DSLContext) {
      ctx
        .alterTable("workload")
        .addColumnIfNotExists(
          DSL.field(
            "geography",
            SQLDataType.VARCHAR.nullable(false).defaultValue("AUTO"),
          ),
        ).execute()
    }
  }
}
