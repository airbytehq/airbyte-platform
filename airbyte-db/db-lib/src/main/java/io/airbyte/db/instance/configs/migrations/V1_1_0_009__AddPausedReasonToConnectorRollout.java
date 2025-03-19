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

public class V1_1_0_009__AddPausedReasonToConnectorRollout extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_0_009__AddPausedReasonToConnectorRollout.class);
  private static final String CONNECTOR_ROLLOUT = "connector_rollout";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    addPausedReasomToConnectorRollout(ctx);
  }

  private void addPausedReasomToConnectorRollout(final DSLContext ctx) {
    ctx.alterTable(CONNECTOR_ROLLOUT)
        .addColumnIfNotExists(
            DSL.field("paused_reason", SQLDataType.VARCHAR(1024).nullable(true)))
        .execute();
  }

}
