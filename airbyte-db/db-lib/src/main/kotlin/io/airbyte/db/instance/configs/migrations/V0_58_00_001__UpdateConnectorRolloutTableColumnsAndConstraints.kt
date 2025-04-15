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
class V0_58_00_001__UpdateConnectorRolloutTableColumnsAndConstraints : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addWorkflowRunIdColumn(ctx)
    makeExpiresAtOptional(ctx)
    makeMaxStepWaitTimeMinsOptional(ctx)
    makeFinalTargetRolloutPctOptional(ctx)
    makeInitialRolloutPctOptional(ctx)
    makeRolloutStrategyOptional(ctx)
    addWorkflowStartedStateToEnum(ctx)
    addReleaseCandidateToEnum(ctx)
    dropOldActorDefinitionStatePartialUniqueIndex(ctx)
    createNewActorDefinitionStatePartialUniqueIndex(ctx)
  }

  companion object {
    private const val TABLE = "connector_rollout"
    private const val CONFIG_ORIGIN_TYPE = "config_origin_type"
    private const val RELEASE_CANDIDATE = "release_candidate"
    private const val CONNECTOR_ROLLOUT_STATE_TYPE = "connector_rollout_state_type"
    private const val WORKFLOW_STARTED = "workflow_started"

    fun addWorkflowRunIdColumn(ctx: DSLContext) {
      ctx
        .alterTable(TABLE)
        .addColumnIfNotExists(DSL.field("workflow_run_id", SQLDataType.VARCHAR(64).nullable(true)))
        .execute()
    }

    fun makeExpiresAtOptional(ctx: DSLContext) {
      ctx
        .alterTable(TABLE)
        .alterColumn(DSL.field("expires_at", SQLDataType.TIMESTAMP))
        .dropNotNull()
        .execute()
    }

    fun makeMaxStepWaitTimeMinsOptional(ctx: DSLContext) {
      ctx
        .alterTable(TABLE)
        .alterColumn(DSL.field("max_step_wait_time_mins", SQLDataType.INTEGER))
        .dropNotNull()
        .execute()
    }

    fun makeFinalTargetRolloutPctOptional(ctx: DSLContext) {
      ctx
        .alterTable(TABLE)
        .alterColumn(DSL.field("final_target_rollout_pct", SQLDataType.INTEGER))
        .dropNotNull()
        .execute()
    }

    fun makeInitialRolloutPctOptional(ctx: DSLContext) {
      ctx
        .alterTable(TABLE)
        .alterColumn(DSL.field("initial_rollout_pct", SQLDataType.INTEGER))
        .dropNotNull()
        .execute()
    }

    fun makeRolloutStrategyOptional(ctx: DSLContext) {
      ctx
        .alterTable(TABLE)
        .alterColumn(DSL.field("rollout_strategy", SQLDataType.VARCHAR(256)))
        .dropNotNull()
        .execute()
    }

    fun addWorkflowStartedStateToEnum(ctx: DSLContext) {
      ctx.alterType(CONNECTOR_ROLLOUT_STATE_TYPE).addValue(WORKFLOW_STARTED).execute()
    }

    fun addReleaseCandidateToEnum(ctx: DSLContext) {
      ctx.alterType(CONFIG_ORIGIN_TYPE).addValue(RELEASE_CANDIDATE).execute()
    }

    fun dropOldActorDefinitionStatePartialUniqueIndex(ctx: DSLContext) {
      ctx.dropIndexIfExists("actor_definition_id_state_unique_idx").execute()
    }

    fun createNewActorDefinitionStatePartialUniqueIndex(ctx: DSLContext) {
      // Replacing the old index with a new one that includes `errored` as an active state
      ctx
        .createUniqueIndex("actor_definition_id_state_unique_idx")
        .on(DSL.table(TABLE), DSL.field("actor_definition_id"))
        .where(
          DSL
            .field("state")
            .`in`("errored", "finalizing", "in_progress", "initialized", "paused", "workflow_started"),
        ).execute()
    }
  }
}
