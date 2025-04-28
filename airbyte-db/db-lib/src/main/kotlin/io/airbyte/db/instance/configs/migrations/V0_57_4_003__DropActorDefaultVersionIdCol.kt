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

/**
 * Migration to drop the column `default_version_id` from the `actor` table. This column is no
 * longer needed as breaking changes are now handled via scoped_configuration entries.
 */
@Suppress("ktlint:standard:class-naming")
class V0_57_4_003__DropActorDefaultVersionIdCol : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    dropActorDefaultVersionIdCol(ctx)
  }

  companion object {
    private fun dropActorDefaultVersionIdCol(ctx: DSLContext) {
      ctx
        .alterTable(DSL.table("actor"))
        .dropColumnIfExists("default_version_id")
        .execute()
    }
  }
}
