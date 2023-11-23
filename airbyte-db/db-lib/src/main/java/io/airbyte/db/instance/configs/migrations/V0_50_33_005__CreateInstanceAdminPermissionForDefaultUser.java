/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.select;

import io.airbyte.db.instance.configs.migrations.V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V0_50_33_005__CreateInstanceAdminPermissionForDefaultUser extends BaseJavaMigration {

  private static final String PERMISSION_TABLE = "permission";
  private static final String USER_TABLE = "\"user\"";
  // The all-zero UUID is used to reliably identify the default user.
  private static final UUID DEFAULT_USER_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  private static final Field<UUID> ID_COLUMN = DSL.field("id", SQLDataType.UUID);
  private static final Field<UUID> USER_ID_COLUMN = DSL.field("user_id", SQLDataType.UUID);
  private static final Field<PermissionType> PERMISSION_TYPE_COLUMN =
      DSL.field("permission_type", SQLDataType.VARCHAR.asEnumDataType(PermissionType.class));

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_005__CreateInstanceAdminPermissionForDefaultUser.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    createInstanceAdminPermissionForDefaultUser(ctx);
  }

  static void createInstanceAdminPermissionForDefaultUser(final DSLContext ctx) {
    // return early if the default user is not present in the database. This
    // shouldn't happen in practice, but if somebody manually removed the default
    // user prior to this migration, we want this to be a no-op.
    if (!ctx.fetchExists(select()
        .from(DSL.table(USER_TABLE))
        .where(ID_COLUMN.eq(DEFAULT_USER_ID)))) {
      LOGGER.warn("Default user does not exist. Skipping this migration.");
      return;
    }

    // return early if the default user already has an instance_admin permission.
    // This shouldn't happen in practice, but if somebody manually inserted a
    // permission record prior to this migration, we want this to be a no-op.
    if (ctx.fetchExists(select()
        .from(DSL.table(PERMISSION_TABLE))
        .where(USER_ID_COLUMN.eq(DEFAULT_USER_ID))
        .and(PERMISSION_TYPE_COLUMN.eq(PermissionType.INSTANCE_ADMIN)))) {
      LOGGER.warn("Default user already has instance_admin permission. Skipping this migration.");
      return;
    }

    LOGGER.info("Inserting instance_admin permission record for default user.");
    ctx.insertInto(DSL.table(PERMISSION_TABLE),
        ID_COLUMN,
        USER_ID_COLUMN,
        PERMISSION_TYPE_COLUMN)
        .values(UUID.randomUUID(), DEFAULT_USER_ID, PermissionType.INSTANCE_ADMIN)
        .execute();
  }

}
