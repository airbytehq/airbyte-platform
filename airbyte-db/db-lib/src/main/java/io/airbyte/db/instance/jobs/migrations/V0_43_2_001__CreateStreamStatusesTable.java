/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;

import java.time.OffsetDateTime;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jetbrains.annotations.NotNull;
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

/**
 * Adds stream_statuses table.
 */
public class V0_43_2_001__CreateStreamStatusesTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_43_2_001__CreateStreamStatusesTable.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    LOGGER.info("Creating enums");
    createStatusRunState(ctx);
    createStatusIncompleteRunCause(ctx);
    createStatusJobType(ctx);

    LOGGER.info("Creating table");
    createStreamStatusesTable(ctx);

    LOGGER.info("Creating indices");
    createIndices(ctx);

    LOGGER.info("Completed migration: {}", this.getClass().getSimpleName());
  }

  private static void createStatusRunState(final DSLContext ctx) {
    ctx.createType("job_stream_status_run_state")
        .asEnum("pending", "running", "complete", "incomplete")
        .execute();
  }

  private static void createStatusIncompleteRunCause(final DSLContext ctx) {
    ctx.createType("job_stream_status_incomplete_run_cause")
        .asEnum("failed", "canceled")
        .execute();
  }

  private static void createStatusJobType(final DSLContext ctx) {
    ctx.createType("job_stream_status_job_type")
        .asEnum("sync", "reset")
        .execute();
  }

  private static void createStreamStatusesTable(final DSLContext ctx) {
    // metadata
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.notNull());
    final Field<UUID> workspaceId = DSL.field("workspace_id", SQLDataType.UUID.notNull());
    final Field<UUID> connectionId = DSL.field("connection_id", SQLDataType.UUID.notNull());
    final Field<Long> jobId = DSL.field("job_id", SQLDataType.BIGINT.notNull());
    final Field<Integer> attemptNo = DSL.field("attempt_number", SQLDataType.INTEGER.notNull());
    final Field<String> streamNamespace = DSL.field("stream_namespace", SQLDataType.VARCHAR.notNull());
    final Field<String> streamName = DSL.field("stream_name", SQLDataType.VARCHAR.notNull());
    final Field<StatusJobType> jobType = DSL
        .field("job_type", SQLDataType.VARCHAR.asEnumDataType(StatusJobType.class).notNull());

    // row timestamps
    final Field<OffsetDateTime> createdAt = DSL
        .field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt = DSL
        .field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(currentOffsetDateTime()));

    // values
    final Field<RunState> state = DSL
        .field("run_state", SQLDataType.VARCHAR.asEnumDataType(RunState.class).notNull());
    final Field<IncompleteRunCause> incompleteCause = DSL
        .field("incomplete_run_cause", SQLDataType.VARCHAR.asEnumDataType(IncompleteRunCause.class).nullable(true));
    final Field<OffsetDateTime> transitionedAt = DSL
        .field("transitioned_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull());

    ctx.createTableIfNotExists("stream_statuses")
        .columns(id, workspaceId, connectionId, jobId, streamNamespace, streamName, createdAt, updatedAt, attemptNo, jobType, state, incompleteCause,
            transitionedAt)
        .constraints(
            primaryKey(id),
            foreignKey(jobId).references("jobs", "id").onDeleteCascade())
        .execute();
  }

  private static void createIndices(final DSLContext ctx) {
    ctx.createIndexIfNotExists("stream_status_connection_id_idx").on("stream_statuses", "connection_id").execute();
    ctx.createIndexIfNotExists("stream_status_job_id_idx").on("stream_statuses", "job_id").execute();
  }

  enum RunState implements EnumType {

    PENDING("pending"),
    RUNNING("running"),
    COMPLETE("complete"),
    INCOMPLETE("incomplete");

    private final String literal;

    RunState(@NotNull final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"), null);
    }

    @Override
    public String getName() {
      return "job_stream_status_run_state";
    }

    @Override
    @NotNull
    public String getLiteral() {
      return literal;
    }

  }

  enum IncompleteRunCause implements EnumType {

    FAILED("failed"),
    CANCELED("canceled");

    private final String literal;

    IncompleteRunCause(@NotNull final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"), null);
    }

    @Override
    public String getName() {
      return "job_stream_status_incomplete_run_cause";
    }

    @Override
    @NotNull
    public String getLiteral() {
      return literal;
    }

  }

  enum StatusJobType implements EnumType {

    SYNC("sync"),
    RESET("reset");

    private final String literal;

    StatusJobType(@NotNull final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"), null);
    }

    @Override
    public String getName() {
      return "job_stream_status_job_type";
    }

    @Override
    @NotNull
    public String getLiteral() {
      return literal;
    }

  }

}
