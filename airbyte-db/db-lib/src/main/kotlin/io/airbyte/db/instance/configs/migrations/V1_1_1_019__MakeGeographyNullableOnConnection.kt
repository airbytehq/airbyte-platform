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
class V1_1_1_019__MakeGeographyNullableOnConnection : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    doMigration(ctx)
  }

  companion object {
    private val CONNECTION = DSL.table("connection")
    private val CONNECTION_GEOGRAPHY = DSL.field("geography", SQLDataType.VARCHAR.nullable(true))

    fun doMigration(ctx: DSLContext) {
      log.info { "Making 'geography' column in 'connection' table nullable" }
      ctx
        .alterTable(CONNECTION)
        .alterColumn(CONNECTION_GEOGRAPHY)
        .dropNotNull()
        .execute()
    }
  }
}
