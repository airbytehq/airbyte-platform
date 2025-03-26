/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.constraint;
import static org.jooq.impl.DSL.currentOffsetDateTime;
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

public class V1_1_1_010__CreateWorkloadQueueTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_010__CreateWorkloadQueueTable.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    LOGGER.info("Creating table");
    createWorkloadQueueTable(ctx);

    LOGGER.info("Creating indices");
    createIndices(ctx);

    LOGGER.info("Completed migration: {}", this.getClass().getSimpleName());
  }

  private static void createWorkloadQueueTable(final DSLContext ctx) {
    // metadata
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.notNull());

    // operational data
    final Field<String> dataplaneGroup = DSL.field("dataplane_group", SQLDataType.VARCHAR(256).notNull());
    final Field<Integer> priority = DSL.field("priority", SQLDataType.INTEGER.notNull());
    final Field<String> workloadId = DSL.field("workload_id", SQLDataType.VARCHAR(256).notNull());
    final Field<OffsetDateTime> pollDeadline = DSL
        .field("poll_deadline", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> ackedAt = DSL
        .field("acked_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true));

    // row timestamps
    final Field<OffsetDateTime> createdAt = DSL
        .field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt = DSL
        .field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists("workload_queue")
        .columns(id, dataplaneGroup, priority, workloadId, pollDeadline, ackedAt, createdAt, updatedAt)
        .constraints(
            primaryKey(id),
            constraint("uniq_workload_id").unique("workload_id"))
        .execute();
  }

  private static void createIndices(final DSLContext ctx) {
    ctx.query(
        "CREATE INDEX IF NOT EXISTS dataplane_group_priority_poll_deadline_idx "
            + "ON workload_queue(dataplane_group, priority, poll_deadline) "
            + "WHERE acked_at IS NULL")
        .execute();

    ctx.createIndexIfNotExists("workload_id_idx").on("workload_queue", "workload_id").execute();
  }

}
