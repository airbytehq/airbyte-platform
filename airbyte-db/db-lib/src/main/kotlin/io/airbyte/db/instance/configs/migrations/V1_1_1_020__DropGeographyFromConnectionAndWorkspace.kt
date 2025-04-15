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
class V1_1_1_020__DropGeographyFromConnectionAndWorkspace : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    doMigration(ctx)
  }

  companion object {
    private val CONNECTION = DSL.table("connection")
    private val WORKSPACE = DSL.table("workspace")

    private val CONNECTION_GEOGRAPHY = DSL.field("geography", SQLDataType.VARCHAR)
    private val WORKSPACE_GEOGRAPHY_DO_NOT_USE = DSL.field(DSL.name("geography_DO_NOT_USE"), SQLDataType.VARCHAR)

    fun doMigration(ctx: DSLContext) {
      log.info { "Dropping 'geography' column from 'connection' table" }
      ctx
        .alterTable(CONNECTION)
        .dropColumn(CONNECTION_GEOGRAPHY)
        .execute()

      log.info { "Dropping 'geography_DO_NOT_USE' column from 'workspace' table" }
      ctx
        .alterTable(WORKSPACE)
        .dropColumn(WORKSPACE_GEOGRAPHY_DO_NOT_USE)
        .execute()
    }
  }
}
