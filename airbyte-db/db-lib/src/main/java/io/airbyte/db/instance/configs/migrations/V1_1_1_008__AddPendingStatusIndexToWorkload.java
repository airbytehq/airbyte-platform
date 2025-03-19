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

public class V1_1_1_008__AddPendingStatusIndexToWorkload extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_008__AddPendingStatusIndexToWorkload.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());

    ctx.query(
        "CREATE INDEX IF NOT EXISTS workload_pending_status_by_dataplane_group_idx ON workload(status, dataplane_group) where status = 'pending'")
        .execute();
  }

}
