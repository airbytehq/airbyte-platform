/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_50_33_006__AddInputPayloadAndLogPathColumnsToWorkload : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    val payloadColumn = DSL.field(PAYLOAD_COLUMN_NAME, SQLDataType.CLOB.nullable(false))
    ctx
      .alterTable(TABLE)
      .addColumnIfNotExists(payloadColumn)
      .execute()

    val logPathColumn = DSL.field(LOG_PATH_COLUMN_NAME, SQLDataType.CLOB.nullable(false))
    ctx
      .alterTable(TABLE)
      .addColumnIfNotExists(logPathColumn)
      .execute()
  }

  companion object {
    private const val TABLE = "workload"
    private const val PAYLOAD_COLUMN_NAME = "input_payload"
    private const val LOG_PATH_COLUMN_NAME = "log_path"
  }
}
