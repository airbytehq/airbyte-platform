/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V1_1_0_005__UpdateConnectorRolloutStateEnum extends BaseJavaMigration {

  private static final String CONNECTOR_ROLLOUT_TABLE = "connector_rollout";
  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_0_005__UpdateConnectorRolloutStateEnum.class);
  private static final String STATE_COLUMN = "state";
  private static final String CONNECTOR_ROLLOUT_STATE_TYPE_ENUM = "connector_rollout_state_type";
  private static final String CANCELED_ROLLED_BACK = "canceled_rolled_back";
  private static final String CANCELED = "canceled";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    runMigration(ctx);
  }

  public static void runMigration(final DSLContext ctx) {
    ctx.alterType(CONNECTOR_ROLLOUT_STATE_TYPE_ENUM).renameValue(CANCELED_ROLLED_BACK).to(CANCELED).execute();
    LOGGER.info(
        "Updated from '{}' to '{}' in table '{}' column '{}'",
        CANCELED_ROLLED_BACK, CANCELED, CONNECTOR_ROLLOUT_TABLE, STATE_COLUMN);

    ctx.update(DSL.table(CONNECTOR_ROLLOUT_TABLE))
        .set(DSL.field(STATE_COLUMN), CANCELED)
        .where(DSL.field(STATE_COLUMN).eq(CANCELED_ROLLED_BACK))
        .execute();
  }

}
