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
class V1_1_0_006__MakeResourceColumnsNullableScopedConfiguration : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    runMigration(ctx)
  }

  companion object {
    private const val SCOPED_CONFIGURATION = "scoped_configuration"
    private const val RESOURCE_TYPE = "resource_type"
    private const val RESOURCE_ID = "resource_id"

    fun runMigration(ctx: DSLContext) {
      ctx
        .alterTable(SCOPED_CONFIGURATION)
        .alter(DSL.field(RESOURCE_ID))
        .dropNotNull()
        .execute()

      ctx
        .alterTable(SCOPED_CONFIGURATION)
        .alter(DSL.field(RESOURCE_TYPE))
        .dropNotNull()
        .execute()
    }
  }
}
