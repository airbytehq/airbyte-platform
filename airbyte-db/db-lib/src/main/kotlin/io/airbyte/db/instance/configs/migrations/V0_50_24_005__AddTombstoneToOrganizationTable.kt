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
class V0_50_24_005__AddTombstoneToOrganizationTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addTombstoneColumn(ctx)

    log.info { "Migration finished!" }
  }

  companion object {
    private const val ORGANIZATION_TABLE = "organization"
    private const val TOMBSTONE_COLUMN = "tombstone"

    fun addTombstoneColumn(ctx: DSLContext) {
      ctx
        .alterTable(ORGANIZATION_TABLE)
        .addColumnIfNotExists(
          DSL.field(
            TOMBSTONE_COLUMN,
            SQLDataType.BOOLEAN.nullable(false).defaultValue(false),
          ),
        ).execute()
    }
  }
}
