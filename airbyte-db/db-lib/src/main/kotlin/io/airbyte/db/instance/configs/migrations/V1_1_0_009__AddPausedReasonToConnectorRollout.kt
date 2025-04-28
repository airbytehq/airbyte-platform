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
class V1_1_0_009__AddPausedReasonToConnectorRollout : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    addPausedReasonToConnectorRollout(ctx)
  }

  private fun addPausedReasonToConnectorRollout(ctx: DSLContext) {
    ctx
      .alterTable(CONNECTOR_ROLLOUT)
      .addColumnIfNotExists(
        DSL.field("paused_reason", SQLDataType.VARCHAR(1024).nullable(true)),
      ).execute()
  }

  companion object {
    private const val CONNECTOR_ROLLOUT = "connector_rollout"
  }
}
