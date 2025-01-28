/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.table;

import java.time.OffsetDateTime;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_41_008__AddConnectionTimeline extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_41_008__AddConnectionTimeline.class);

  private static final Field<UUID> idField = DSL.field("id", SQLDataType.UUID.nullable(false));

  private static final Field<UUID> connectionIdField = DSL.field("connection_id", SQLDataType.UUID.nullable(false));

  private static final Field<UUID> userIdField = DSL.field("user_id", SQLDataType.UUID.nullable(true));

  private static final Field<String> eventTypeField = DSL.field("event_type", SQLDataType.VARCHAR.nullable(false));

  private static final Field<JSONB> summaryField = DSL.field("summary", SQLDataType.JSONB.nullable(true));

  private static final Field<OffsetDateTime> createdAtField = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false));
  public static final String TABLE_NAME = "connection_timeline_event";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    ctx.createTable(TABLE_NAME)
        .columns(idField,
            connectionIdField,
            userIdField,
            eventTypeField,
            summaryField,
            createdAtField)
        .constraints(primaryKey(idField),
            foreignKey(connectionIdField).references("connection", "id").onDeleteCascade(),
            foreignKey(userIdField).references("user", "id"))
        .execute();
    ctx.createIndexIfNotExists("idx_connection_timeline_connection_id")
        .on(table(TABLE_NAME), connectionIdField.asc(), createdAtField.desc(), eventTypeField.asc())
        .execute();
  }

}
