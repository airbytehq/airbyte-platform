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
class V1_6_0_010__Add_Connector_IPC_Options_Column : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx: DSLContext = DSL.using(context.connection)

    addIPCOptionsColumn(ctx)
  }

  companion object {
    @JvmStatic
    fun addIPCOptionsColumn(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition_version")
        .addColumn("connector_ipc_options", SQLDataType.JSONB.nullable(true))
        .execute()
    }
  }
}
