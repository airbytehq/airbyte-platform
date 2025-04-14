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
class V1_1_1_000__AddComponentsToBuilderProjects : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    runMigration(ctx)
  }

  companion object {
    @JvmStatic
    fun runMigration(ctx: DSLContext) {
      ctx
        .alterTable("connector_builder_project")
        .addColumn("components_file_content", SQLDataType.CLOB.nullable(true))
        .execute()
    }
  }
}
