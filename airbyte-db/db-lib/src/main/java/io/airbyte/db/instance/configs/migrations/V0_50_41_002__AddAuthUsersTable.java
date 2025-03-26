/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.AUTH_USER_TABLE;
import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.unique;

import io.airbyte.commons.enums.Enums;
import io.airbyte.db.instance.configs.migrations.V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider;
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
 * Migration to add the AuthUser table to the OSS Config DB. This table will replace the columns
 * auth_user_id and auth_provider in the user table to allow a 1:n relationship between user and
 * auth_user
 */
public class V0_50_41_002__AddAuthUsersTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_41_002__AddAuthUsersTable.class);
  private static final String USER_TABLE = "\"user\"";

  private static final Field<UUID> idField = DSL.field("id", SQLDataType.UUID.nullable(false));
  private static final Field<UUID> userIdField = DSL.field("user_id", SQLDataType.UUID.nullable(false));
  private static final Field<String> authUserIdField = DSL.field("auth_user_id", SQLDataType.VARCHAR(256).nullable(false));
  private static final Field<AuthProvider> authProviderField =
      DSL.field("auth_provider", SQLDataType.VARCHAR.asEnumDataType(AuthProvider.class).nullable(false));
  private static final Field<OffsetDateTime> createdAtField =
      DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
  private static final Field<OffsetDateTime> updatedAtField =
      DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    createAuthUsersTable(ctx);
    populateAuthUserTable(ctx);
  }

  static void createAuthUsersTable(final DSLContext ctx) {

    ctx.createTable(AUTH_USER_TABLE)
        .columns(idField,
            userIdField,
            authUserIdField,
            authProviderField,
            createdAtField,
            updatedAtField)
        .constraints(
            primaryKey(idField),
            foreignKey(userIdField).references("user", "id").onDeleteCascade(),
            unique(authUserIdField, authProviderField))
        .execute();
  }

  static void populateAuthUserTable(final DSLContext ctx) {
    final var userRecords = ctx.select(
        DSL.field("id"),
        DSL.field("auth_user_id"),
        DSL.field("auth_provider"),
        DSL.field("created_at"),
        DSL.field("updated_at")).from(DSL.table(USER_TABLE)).fetch();

    userRecords.forEach(userRecord -> {
      final OffsetDateTime now = OffsetDateTime.now();

      ctx.insertInto(DSL.table(AUTH_USER_TABLE))
          .set(idField, UUID.randomUUID())
          .set(userIdField, userRecord.get(DSL.field("id", UUID.class)))
          .set(authUserIdField, userRecord.get(DSL.field("auth_user_id", String.class)))
          .set(authProviderField, Enums.toEnum(userRecord.get(DSL.field("auth_provider", String.class)), AuthProvider.class).orElseThrow())
          .set(createdAtField, now)
          .set(updatedAtField, now)
          .execute();
    });
  }

}
