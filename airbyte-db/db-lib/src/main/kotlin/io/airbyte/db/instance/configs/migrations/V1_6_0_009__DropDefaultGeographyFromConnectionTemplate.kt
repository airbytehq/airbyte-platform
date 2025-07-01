/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_009__DropDefaultGeographyFromConnectionTemplate : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    dropDefaultGeographyFromConnectionTemplate(ctx)
  }

  companion object {
    private const val CONNECTION_TEMPLATE_TABLE_NAME: String = "connection_template"
    private const val DEFAULT_GEOGRAPHY_COLUMN_NAME: String = "default_geography"

    fun dropDefaultGeographyFromConnectionTemplate(ctx: DSLContext) {
      ctx
        .alterTable(CONNECTION_TEMPLATE_TABLE_NAME)
        .drop(DEFAULT_GEOGRAPHY_COLUMN_NAME)
        .execute()
    }
  }
}
