/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

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

public class V0_55_1_002__AddGenerationTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_55_1_002__AddGenerationTable.class);

  static final String STREAM_GENERATION_TABLE_NAME = "stream_generation";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    createGenerationTable(ctx);
  }

  static void createGenerationTable(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<UUID> connectionId = DSL.field("connection_id", SQLDataType.UUID.nullable(false));
    final Field<String> streamName = DSL.field("stream_name", SQLDataType.VARCHAR.nullable(false));
    final Field<String> streamNamespace = DSL.field("stream_namespace", SQLDataType.VARCHAR.nullable(true));
    final Field<Long> generationId = DSL.field("generation_id", SQLDataType.BIGINT.nullable(false));
    final Field<Long> startJobId = DSL.field("start_job_id", SQLDataType.BIGINT.nullable(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTable(STREAM_GENERATION_TABLE_NAME)
        .columns(id,
            connectionId,
            streamName,
            streamNamespace,
            generationId,
            startJobId,
            createdAt,
            updatedAt)
        .constraints(
            primaryKey(id),
            foreignKey(connectionId).references("connection", "id").onDeleteCascade())
        .execute();

    final String indexCreationQuery = String.format("CREATE INDEX ON %s USING btree (%s, %s, %s DESC)",
        STREAM_GENERATION_TABLE_NAME, connectionId.getName(), streamName.getName(), generationId.getName());
    final String indexCreationQuery2 = String.format("CREATE INDEX ON %s USING btree (%s, %s, %s, %s DESC)",
        STREAM_GENERATION_TABLE_NAME, connectionId.getName(), streamName.getName(), streamNamespace.getName(), generationId.getName());
    ctx.execute(indexCreationQuery);
    ctx.execute(indexCreationQuery2);
  }

}
