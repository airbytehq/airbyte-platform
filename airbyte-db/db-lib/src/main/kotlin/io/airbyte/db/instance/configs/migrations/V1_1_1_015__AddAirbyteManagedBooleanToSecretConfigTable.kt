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

/**
 * Adds airbyte_managed boolean column to SecretConfig table.
 */
@Suppress("ktlint:standard:class-naming")
class V1_1_1_015__AddAirbyteManagedBooleanToSecretConfigTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addAirbyteManagedColumn(ctx)
  }

  companion object {
    // Note that the SecretConfig table is currently empty, so we can add a non-nullable column
    // without providing a default value. We want writers to explicitly set this column's value
    // for all rows, so a default value would be inappropriate.
    fun addAirbyteManagedColumn(ctx: DSLContext) {
      ctx
        .alterTable("secret_config")
        .addColumn("airbyte_managed", SQLDataType.BOOLEAN.nullable(false))
        .execute()
    }
  }
}
