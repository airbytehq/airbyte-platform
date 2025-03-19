/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations;

import static org.jooq.impl.DSL.constraint;
import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;

import java.time.OffsetDateTime;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds the table to store retry state to be accessed by the connection manager workflow.
 */
public class V0_50_4_001__CreateRetryStatesTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_4_001__CreateRetryStatesTable.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    LOGGER.info("Creating table");
    createRetryStatesTable(ctx);

    LOGGER.info("Creating indices");
    createIndices(ctx);

    LOGGER.info("Completed migration: {}", this.getClass().getSimpleName());
  }

  private static void createRetryStatesTable(final DSLContext ctx) {
    // metadata
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.notNull());
    final Field<UUID> connectionId = DSL.field("connection_id", SQLDataType.UUID.notNull());
    final Field<Long> jobId = DSL.field("job_id", SQLDataType.BIGINT.notNull());

    // row timestamps
    final Field<OffsetDateTime> createdAt = DSL
        .field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt = DSL
        .field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(currentOffsetDateTime()));

    // values
    final Field<Integer> successiveCompleteFailures = DSL.field("successive_complete_failures", SQLDataType.INTEGER.notNull());
    final Field<Integer> totalCompleteFailures = DSL.field("total_complete_failures", SQLDataType.INTEGER.notNull());
    final Field<Integer> successivePartialFailures = DSL.field("successive_partial_failures", SQLDataType.INTEGER.notNull());
    final Field<Integer> totalPartialFailures = DSL.field("total_partial_failures", SQLDataType.INTEGER.notNull());

    ctx.createTableIfNotExists("retry_states")
        .columns(id, connectionId, jobId, createdAt, updatedAt, successiveCompleteFailures, totalCompleteFailures, successivePartialFailures,
            totalPartialFailures)
        .constraints(
            primaryKey(id),
            foreignKey(jobId).references("jobs", "id").onDeleteCascade(),
            constraint("uniq_job_id").unique("job_id"))
        .execute();
  }

  private static void createIndices(final DSLContext ctx) {
    ctx.createIndexIfNotExists("retry_state_connection_id_idx").on("retry_states", "connection_id").execute();
    ctx.createIndexIfNotExists("retry_state_job_id_idx").on("retry_states", "job_id").execute();
  }

}
