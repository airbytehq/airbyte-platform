/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_58_00_001__UpdateConnectorRolloutTableColumnsAndConstraints extends BaseJavaMigration {

  private static final String TABLE = "connector_rollout";
  private static final Logger LOGGER = LoggerFactory.getLogger(V0_58_00_001__UpdateConnectorRolloutTableColumnsAndConstraints.class);
  private static final String CONFIG_ORIGIN_TYPE = "config_origin_type";
  private static final String RELEASE_CANDIDATE = "release_candidate";
  private static final String CONNECTOR_ROLLOUT_STATE_TYPE = "connector_rollout_state_type";
  private static final String WORKFLOW_STARTED = "workflow_started";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addWorkflowRunIdColumn(ctx);
    makeExpiresAtOptional(ctx);
    makeMaxStepWaitTimeMinsOptional(ctx);
    makeFinalTargetRolloutPctOptional(ctx);
    makeInitialRolloutPctOptional(ctx);
    makeRolloutStrategyOptional(ctx);
    addWorkflowStartedStateToEnum(ctx);
    addReleaseCandidateToEnum(ctx);
    dropOldActorDefinitionStatePartialUniqueIndex(ctx);
    createNewActorDefinitionStatePartialUniqueIndex(ctx);
  }

  static void addWorkflowRunIdColumn(final DSLContext ctx) {
    ctx.alterTable(TABLE)
        .addColumnIfNotExists(DSL.field("workflow_run_id", SQLDataType.VARCHAR(64).nullable(true)))
        .execute();
  }

  static void makeExpiresAtOptional(final DSLContext ctx) {
    ctx.alterTable(TABLE)
        .alterColumn(DSL.field("expires_at", SQLDataType.TIMESTAMP))
        .dropNotNull()
        .execute();
  }

  static void makeMaxStepWaitTimeMinsOptional(final DSLContext ctx) {
    ctx.alterTable(TABLE)
        .alterColumn(DSL.field("max_step_wait_time_mins", SQLDataType.INTEGER))
        .dropNotNull()
        .execute();
  }

  static void makeFinalTargetRolloutPctOptional(final DSLContext ctx) {
    ctx.alterTable(TABLE)
        .alterColumn(DSL.field("final_target_rollout_pct", SQLDataType.INTEGER))
        .dropNotNull()
        .execute();
  }

  static void makeInitialRolloutPctOptional(final DSLContext ctx) {
    ctx.alterTable(TABLE)
        .alterColumn(DSL.field("initial_rollout_pct", SQLDataType.INTEGER))
        .dropNotNull()
        .execute();
  }

  static void makeRolloutStrategyOptional(final DSLContext ctx) {
    ctx.alterTable(TABLE)
        .alterColumn(DSL.field("rollout_strategy", SQLDataType.VARCHAR(256)))
        .dropNotNull()
        .execute();
  }

  static void addWorkflowStartedStateToEnum(final DSLContext ctx) {
    ctx.alterType(CONNECTOR_ROLLOUT_STATE_TYPE).addValue(WORKFLOW_STARTED).execute();
  }

  static void addReleaseCandidateToEnum(final DSLContext ctx) {
    ctx.alterType(CONFIG_ORIGIN_TYPE).addValue(RELEASE_CANDIDATE).execute();
  }

  static void dropOldActorDefinitionStatePartialUniqueIndex(final DSLContext ctx) {
    ctx.dropIndexIfExists("actor_definition_id_state_unique_idx").execute();
  }

  static void createNewActorDefinitionStatePartialUniqueIndex(final DSLContext ctx) {
    // Replacing the old index with a new one that includes `errored` as an active state
    ctx.createUniqueIndex("actor_definition_id_state_unique_idx")
        .on(DSL.table(TABLE), DSL.field("actor_definition_id"))
        .where(DSL.field("state").in("errored", "finalizing", "in_progress", "initialized", "paused", "workflow_started"))
        .execute();
  }

}
