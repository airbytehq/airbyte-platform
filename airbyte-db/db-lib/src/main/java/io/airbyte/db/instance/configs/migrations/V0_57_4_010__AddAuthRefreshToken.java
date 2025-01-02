/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.unique;

import java.time.OffsetDateTime;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migration to add auth_refresh_token table.
 */
public class V0_57_4_010__AddAuthRefreshToken extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_4_010__AddAuthRefreshToken.class);
  private static final String AUTH_REFRESH_TOKEN_TABLE = "auth_refresh_token";

  private static Field<String> sessionId = DSL.field("session_id", SQLDataType.VARCHAR.nullable(false));
  private static Field<String> value = DSL.field("value", SQLDataType.VARCHAR.nullable(false));
  private static Field<Boolean> revoked = DSL.field("revoked", SQLDataType.BOOLEAN.nullable(false).defaultValue(true));
  private static Field<OffsetDateTime> createdAt =
      DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
  private static Field<OffsetDateTime> updatedAt =
      DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    createAuthRefreshTokenTable(ctx);
  }

  static void createAuthRefreshTokenTable(final DSLContext ctx) {
    ctx.createTable(AUTH_REFRESH_TOKEN_TABLE)
        .columns(
            value,
            sessionId,
            revoked,
            createdAt,
            updatedAt)
        .constraints(primaryKey(value), unique(sessionId, value))
        .execute();
  }

}
