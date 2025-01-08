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
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V1_1_0_011__CreateConnectionTagTable extends BaseJavaMigration {

  private static final String CONNECTION_TAG_TABLE = "connection_tag";

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_0_011__CreateConnectionTagTable.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    migrate(ctx);
  }

  public static void migrate(final DSLContext ctx) {
    addPrimaryKeyToTagTable(ctx);
    createConnectionTagTable(ctx);
  }

  private static void addPrimaryKeyToTagTable(final DSLContext ctx) {
    ctx.alterTable("tag")
        .add(primaryKey("id"))
        .execute();
  }

  private static void createConnectionTagTable(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<UUID> tagId = DSL.field("tag_id", SQLDataType.UUID.nullable(false));
    final Field<UUID> connectionId = DSL.field("connection_id", SQLDataType.UUID.nullable(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTable(CONNECTION_TAG_TABLE)
        .columns(id, tagId,
            connectionId,
            createdAt,
            updatedAt)
        .constraints(
            primaryKey(id),
            foreignKey(tagId).references("tag", "id").onDeleteCascade(),
            foreignKey(connectionId).references("connection", "id").onDeleteCascade(),
            unique(tagId, connectionId))
        .execute();
  }

}
