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
 * Add webhook operations columns migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_12_001__AddWebhookOperationColumns : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addWebhookOperationConfigColumn(ctx)
    addWebhookOperationType(ctx)
    addWebhookConfigColumnsToWorkspaceTable(ctx)
  }

  private fun addWebhookConfigColumnsToWorkspaceTable(ctx: DSLContext) {
    ctx
      .alterTable("workspace")
      .addColumnIfNotExists(DSL.field("webhook_operation_configs", SQLDataType.JSONB.nullable(true)))
      .execute()
  }

  private fun addWebhookOperationType(ctx: DSLContext) {
    ctx.alterType("operator_type").addValue("webhook").execute()
  }

  private fun addWebhookOperationConfigColumn(ctx: DSLContext) {
    ctx
      .alterTable("operation")
      .addColumnIfNotExists(DSL.field("operator_webhook", SQLDataType.JSONB.nullable(true)))
      .execute()
  }
}
