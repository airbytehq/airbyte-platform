/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.unique;

import java.time.OffsetDateTime;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.Catalog;
import org.jooq.DSLContext;
import org.jooq.EnumType;
import org.jooq.Field;
import org.jooq.Schema;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.SchemaImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_33_001__AddWorkloadTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_001__AddWorkloadTable.class);
  private static final String WORKLOAD_TABLE_NAME = "workload";
  private static final String WORKLOAD_ID_COLUMN_NAME = "id";
  private static final String LABEL_TABLE_NAME = "workload_label";
  private static final String LABEL_ID_COLUMN_NAME = "id";
  private static final String WORKLOAD_STATUS = "workload_status";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    createEnumStatusType(ctx);
    createWorkload(ctx);
    createLabel(ctx);
    createWorkloadLabelIndex(ctx);
    LOGGER.info("Migration finished!");
  }

  enum WorkloadStatus implements EnumType {

    PENDING("pending"),
    CLAIMED("claimed"),
    RUNNING("running"),
    SUCCESS("success"),

    FAILURE("failure"),
    CANCELLED("cancelled");

    private final String literal;

    WorkloadStatus(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema() == null ? null : getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"), null);
    }

    @Override
    public String getName() {
      return "workload_status";
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

  static void createWorkloadLabelIndex(final DSLContext ctx) {
    ctx.createIndexIfNotExists("workload_label_workload_id_idx")
        .on(LABEL_TABLE_NAME, "workload_id")
        .execute();
  }

  static void createWorkload(final DSLContext ctx) {
    final Field<String> id = DSL.field(WORKLOAD_ID_COLUMN_NAME, SQLDataType.VARCHAR(256).nullable(false));
    // null when pending
    final Field<String> dataplaneId = DSL.field("dataplane_id", SQLDataType.VARCHAR(256).nullable(true));
    final Field<WorkloadStatus> status = DSL.field("status", SQLDataType.VARCHAR.asEnumDataType(WorkloadStatus.class).nullable(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    // null while not heartbeating
    final Field<OffsetDateTime> lastHeartbeatAt =
        DSL.field("last_heartbeat_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true));

    ctx.createTableIfNotExists(WORKLOAD_TABLE_NAME)
        .columns(id,
            dataplaneId,
            status,
            createdAt,
            updatedAt,
            lastHeartbeatAt)
        .constraints(primaryKey(id))
        .execute();

    LOGGER.info("workload table created");
  }

  static void createLabel(final DSLContext ctx) {
    final Field<UUID> id = DSL.field(LABEL_ID_COLUMN_NAME, SQLDataType.UUID.nullable(false));
    final Field<String> workloadId = DSL.field("workload_id", SQLDataType.VARCHAR(256).nullable(false));
    final Field<String> key = DSL.field("key", SQLDataType.VARCHAR(256).nullable(false));
    final Field<String> value = DSL.field("value", SQLDataType.VARCHAR(256).nullable(false));

    ctx.createTableIfNotExists(LABEL_TABLE_NAME)
        .columns(
            id,
            workloadId,
            key,
            value)
        .constraints(
            primaryKey(id),
            foreignKey(workloadId).references(WORKLOAD_TABLE_NAME, WORKLOAD_ID_COLUMN_NAME),
            unique(workloadId, key))
        .execute();
    LOGGER.info("workload label table created");
  }

  private static void createEnumStatusType(final DSLContext ctx) {
    ctx.createType(WORKLOAD_STATUS).asEnum(WorkloadStatus.CLAIMED.literal,
        WorkloadStatus.RUNNING.literal,
        WorkloadStatus.PENDING.literal,
        WorkloadStatus.SUCCESS.literal,
        WorkloadStatus.FAILURE.literal,
        WorkloadStatus.CANCELLED.literal).execute();
  }

}
