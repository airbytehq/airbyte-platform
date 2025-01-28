/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds metadata column in jobs table.
 */
public class V1_1_0_000__AddMetadataInJobs extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_0_000__AddMetadataInJobs.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());

    LOGGER.info("Add metadata column");
    addMetadataInJobs(ctx);

    LOGGER.info("Completed migration: {}", this.getClass().getSimpleName());
  }

  private static void addMetadataInJobs(final DSLContext ctx) {
    final Table<?> jobsTable = DSL.table("jobs");
    final Field<JSONB> metadata = DSL.field("metadata", SQLDataType.JSONB.nullable(true));

    ctx.alterTable(jobsTable)
        .addColumnIfNotExists(metadata)
        .execute();

  }

}
