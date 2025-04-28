/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Add index to connections tatus migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_18_003__AddIndexToConnectionStatus : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    ctx.createIndexIfNotExists("connection_status_idx").on(CONNECTION_TABLE, "status").execute()
  }

  companion object {
    private const val CONNECTION_TABLE = "connection"
  }
}
