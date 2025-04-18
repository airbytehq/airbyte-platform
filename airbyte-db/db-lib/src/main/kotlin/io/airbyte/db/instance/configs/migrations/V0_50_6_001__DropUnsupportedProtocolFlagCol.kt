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
 * Drop unsupported_protocol_version flag from connection migration. This field has not been used
 * and is safe to remove.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_6_001__DropUnsupportedProtocolFlagCol : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx = DSL.using(context.connection)
    dropUnsupportedProtocolFlagCol(ctx)
  }

  private fun dropUnsupportedProtocolFlagCol(ctx: DSLContext) {
    ctx
      .alterTable("connection")
      .dropColumn(DSL.field("unsupported_protocol_version"))
      .execute()
  }
}
