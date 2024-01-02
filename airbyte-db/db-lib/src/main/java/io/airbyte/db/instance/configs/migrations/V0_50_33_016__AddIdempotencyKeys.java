/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.CONNECTION_TABLE;
import static io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_TABLE;
import static io.airbyte.db.instance.DatabaseConstants.USER_TABLE;
import static io.airbyte.db.instance.DatabaseConstants.WORKSPACE_TABLE;

import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_33_016__AddIdempotencyKeys extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_016__AddIdempotencyKeys.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addIdempotencyKey(ctx, USER_TABLE);
    addIdempotencyKey(ctx, ORGANIZATION_TABLE);
    addIdempotencyKey(ctx, WORKSPACE_TABLE);
    addIdempotencyKey(ctx, CONNECTION_TABLE);
    addActorIdempotencyKey(ctx, "actor_definition");
    addActorIdempotencyKey(ctx, "actor");
  }

  private static void addIdempotencyKey(final @NotNull DSLContext ctx, final @NotNull String table) {
    final String columnName = "idempotency_key";
    final String indexName = String.format("%s_idempotency_key", table);
    final Field<UUID> idempotencyKey = DSL.field(columnName, SQLDataType.UUID.nullable(true));
    ctx.alterTable(table).addColumnIfNotExists(idempotencyKey).execute();
    ctx.createUniqueIndex(indexName).on(table, columnName).execute();
  }

  private static void addActorIdempotencyKey(final @NotNull DSLContext ctx, final @NotNull String table) {
    final String columnName = "idempotency_key";
    final String indexName = String.format("%s_actor_type_idempotency_key", table);
    final Field<UUID> idempotencyKey = DSL.field(columnName, SQLDataType.UUID.nullable(true));
    ctx.alterTable(table).addColumnIfNotExists(idempotencyKey).execute();
    ctx.createUniqueIndex(indexName).on(table, columnName, "actor_type").execute();
  }

}
