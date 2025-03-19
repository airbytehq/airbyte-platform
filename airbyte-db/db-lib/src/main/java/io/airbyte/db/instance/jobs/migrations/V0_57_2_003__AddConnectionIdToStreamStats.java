/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations;

/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

import com.google.common.annotations.VisibleForTesting;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_57_2_003__AddConnectionIdToStreamStats extends BaseJavaMigration {

  private static final Table<Record> STREAM_STATS = DSL.table("stream_stats");
  private static final Field<UUID> CONNECTION_ID = DSL.field("connection_id", SQLDataType.UUID);
  private static final Logger LOGGER = LoggerFactory.getLogger(
      V0_57_2_003__AddConnectionIdToStreamStats.class);

  @VisibleForTesting
  static void addConnectionIdColumn(final DSLContext ctx) {
    LOGGER.info("Adding connection_id column to stream_stats table");

    ctx.alterTable(STREAM_STATS)
        .addColumnIfNotExists(CONNECTION_ID)
        .execute();
  }

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());

    addConnectionIdColumn(ctx);
    LOGGER.info("Migration finished!");
  }

}
