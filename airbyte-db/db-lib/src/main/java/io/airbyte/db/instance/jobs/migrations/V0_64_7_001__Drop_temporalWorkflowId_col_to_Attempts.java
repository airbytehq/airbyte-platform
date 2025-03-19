/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Add temporal workflow id to attempt table.
 */
public class V0_64_7_001__Drop_temporalWorkflowId_col_to_Attempts extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_64_7_001__Drop_temporalWorkflowId_col_to_Attempts.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    ctx.alterTable("attempts")
        .dropColumn(DSL.field("temporal_workflow_id"))
        .execute();
  }

}
