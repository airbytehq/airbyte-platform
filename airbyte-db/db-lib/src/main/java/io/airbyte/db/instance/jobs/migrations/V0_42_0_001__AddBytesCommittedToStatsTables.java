/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Add Bytes Committed to SyncStats and StreamStats.
 */
public class V0_42_0_001__AddBytesCommittedToStatsTables extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_42_0_001__AddBytesCommittedToStatsTables.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    updateSyncStats(ctx);
    updateStreamStats(ctx);
  }

  private static void updateStreamStats(final DSLContext ctx) {
    final String streamStats = "stream_stats";
    ctx.alterTable(streamStats)
        .addColumnIfNotExists(DSL.field("bytes_committed", SQLDataType.BIGINT.nullable(true)))
        .execute();
    ctx.alterTable(streamStats)
        .addColumnIfNotExists(DSL.field("records_committed", SQLDataType.BIGINT.nullable(true)))
        .execute();
  }

  private static void updateSyncStats(final DSLContext ctx) {
    final String streamStats = "sync_stats";
    ctx.alterTable(streamStats)
        .addColumnIfNotExists(DSL.field("bytes_committed", SQLDataType.BIGINT.nullable(true)))
        .execute();
  }

}
