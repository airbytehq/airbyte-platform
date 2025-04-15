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

@Suppress("ktlint:standard:class-naming")
class V1_1_0_005__UpdateConnectorRolloutStateEnum : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    runMigration(ctx)
  }

  companion object {
    private const val CONNECTOR_ROLLOUT_TABLE = "connector_rollout"
    private const val STATE_COLUMN = "state"
    private const val CONNECTOR_ROLLOUT_STATE_TYPE_ENUM = "connector_rollout_state_type"
    private const val CANCELED_ROLLED_BACK = "canceled_rolled_back"
    private const val CANCELED = "canceled"

    @JvmStatic
    fun runMigration(ctx: DSLContext) {
      ctx
        .alterType(CONNECTOR_ROLLOUT_STATE_TYPE_ENUM)
        .renameValue(CANCELED_ROLLED_BACK)
        .to(CANCELED)
        .execute()
      log.info { "Updated from '$CANCELED_ROLLED_BACK' to '$CANCELED' in table '$CONNECTOR_ROLLOUT_TABLE' column '$STATE_COLUMN'" }

      ctx
        .update(DSL.table(CONNECTOR_ROLLOUT_TABLE))
        .set(DSL.field(STATE_COLUMN), CANCELED)
        .where(DSL.field(STATE_COLUMN).eq(CANCELED_ROLLED_BACK))
        .execute()
    }
  }
}
