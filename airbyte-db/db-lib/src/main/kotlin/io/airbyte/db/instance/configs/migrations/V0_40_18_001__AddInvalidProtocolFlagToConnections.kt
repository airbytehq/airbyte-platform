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
 * Add protocol flags to connection migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_18_001__AddInvalidProtocolFlagToConnections : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addInvalidProtocolFlagToConnections(ctx)
  }

  private fun addInvalidProtocolFlagToConnections(ctx: DSLContext) {
    ctx
      .alterTable("connection")
      .addColumn(
        DSL.field("unsupported_protocol_version", SQLDataType.BOOLEAN.nullable(false).defaultValue(false)),
      ).execute()
  }
}
