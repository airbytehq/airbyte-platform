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

public class V0_55_1_003__EditRefreshTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_55_1_003__EditRefreshTable.class);

  static final String STREAM_REFRESHES_TABLE = "stream_refreshes";

  private static final Field<UUID> connectionId = DSL.field("connection_id", SQLDataType.UUID.nullable(false));
  private static final Field<String> streamName = DSL.field("stream_name", SQLDataType.VARCHAR.nullable(false));
  private static final Field<String> streamNamespace = DSL.field("stream_namespace", SQLDataType.VARCHAR.nullable(true));

  private static final Field<OffsetDateTime> createdAtField =
      DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    editRefreshTable(ctx);
  }

  static void editRefreshTable(final DSLContext ctx) {
    ctx.truncate(STREAM_REFRESHES_TABLE).execute();
    ctx.dropTable(STREAM_REFRESHES_TABLE).execute();

    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    ctx.createTable(STREAM_REFRESHES_TABLE)
        .columns(id,
            connectionId,
            streamName,
            streamNamespace,
            createdAtField)
        .constraints(
            primaryKey(id),
            foreignKey(connectionId).references("connection", "id").onDeleteCascade())
        .execute();

    final String indexCreationQuery = String.format("CREATE INDEX ON %s USING btree (%s)",
        STREAM_REFRESHES_TABLE, connectionId.getName());
    final String indexCreationQuery2 = String.format("CREATE INDEX ON %s USING btree (%s, %s)",
        STREAM_REFRESHES_TABLE, connectionId.getName(), streamName.getName());
    final String indexCreationQuery3 = String.format("CREATE INDEX ON %s USING btree (%s, %s, %s)",
        STREAM_REFRESHES_TABLE, connectionId.getName(), streamName.getName(), streamNamespace.getName());
    ctx.execute(indexCreationQuery);
    ctx.execute(indexCreationQuery2);
    ctx.execute(indexCreationQuery3);
  }

}
