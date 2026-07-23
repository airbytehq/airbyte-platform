/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
class V2_1_0_013__AddTombstoneToOrganizationDomainVerificationTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    addTombstoneColumn(ctx)

    log.info { "Migration completed: ${javaClass.simpleName}" }
  }

  companion object {
    private const val TABLE_NAME = "organization_domain_verification"
    private const val TOMBSTONE_COLUMN = "tombstone"

    fun addTombstoneColumn(ctx: DSLContext) {
      ctx
        .alterTable(TABLE_NAME)
        .addColumnIfNotExists(
          DSL.field(
            TOMBSTONE_COLUMN,
            SQLDataType.BOOLEAN.nullable(false).defaultValue(false),
          ),
        ).execute()
    }
  }
}
