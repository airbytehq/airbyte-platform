/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import java.util.Arrays;
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

/**
 * Update enum type: AuthProvider (in User table) and PermissionType (in Permission table). Note: At
 * the time updating these enums, User table and Permission table are still empty in OSS, so there
 * is no real data migration needed.
 */
public class V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.class);
  private static final String USER_TABLE = "user";
  private static final String PERMISSION_TABLE = "permission";
  private static final String AUTH_PROVIDER_COLUMN_NAME = "auth_provider";
  private static final String PERMISSION_TYPE_COLUMN_NAME = "permission_type";

  private static final Field<V0_44_4_001__AddUserAndPermissionTables.AuthProvider> OLD_AUTH_PROVIDER_COLUMN =
      DSL.field(AUTH_PROVIDER_COLUMN_NAME, SQLDataType.VARCHAR.asEnumDataType(
          V0_44_4_001__AddUserAndPermissionTables.AuthProvider.class).nullable(false));
  private static final Field<AuthProvider> NEW_AUTH_PROVIDER_COLUMN =
      DSL.field(AUTH_PROVIDER_COLUMN_NAME, SQLDataType.VARCHAR.asEnumDataType(
          AuthProvider.class).nullable(false));

  private static final Field<V0_44_4_001__AddUserAndPermissionTables.PermissionType> OLD_PERMISSION_TYPE_COLUMN =
      DSL.field(PERMISSION_TYPE_COLUMN_NAME, SQLDataType.VARCHAR.asEnumDataType(
          V0_44_4_001__AddUserAndPermissionTables.PermissionType.class).nullable(false));
  private static final Field<PermissionType> NEW_PERMISSION_TYPE_COLUMN =
      DSL.field(PERMISSION_TYPE_COLUMN_NAME, SQLDataType.VARCHAR.asEnumDataType(
          PermissionType.class).nullable(false));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    updateAuthProviderEnumType(ctx);
    updatePermissionTypeEnumType(ctx);
    LOGGER.info("Migration finished!");
  }

  static void updateAuthProviderEnumType(final DSLContext ctx) {
    ctx.alterTable(USER_TABLE).dropColumn(OLD_AUTH_PROVIDER_COLUMN).execute();
    ctx.dropTypeIfExists(AuthProvider.NAME).execute();
    ctx.createType(AuthProvider.NAME)
        .asEnum(Arrays.stream(AuthProvider.values()).map(AuthProvider::getLiteral).toArray(String[]::new))
        .execute();
    ctx.alterTable(USER_TABLE).addColumn(NEW_AUTH_PROVIDER_COLUMN).execute();
    ctx.createIndexIfNotExists("user_auth_provider_auth_user_id_idx")
        .on(USER_TABLE, "auth_provider", "auth_user_id")
        .execute();
  }

  static void updatePermissionTypeEnumType(final DSLContext ctx) {
    ctx.alterTable(PERMISSION_TABLE).dropColumn(OLD_PERMISSION_TYPE_COLUMN).execute();
    ctx.dropTypeIfExists(PermissionType.NAME).execute();
    ctx.createType(PermissionType.NAME)
        .asEnum(Arrays.stream(PermissionType.values()).map(PermissionType::getLiteral).toArray(String[]::new))
        .execute();
    ctx.alterTable(PERMISSION_TABLE).addColumn(NEW_PERMISSION_TYPE_COLUMN).execute();
  }

  /**
   * User AuthProvider enums.
   */
  public enum AuthProvider implements EnumType {

    AIRBYTE("airbyte"),
    GOOGLE_IDENTITY_PLATFORM("google_identity_platform"),
    KEYCLOAK("keycloak");

    private final String literal;
    public static final String NAME = "auth_provider";

    AuthProvider(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"));
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

  /**
   * User Roles as PermissionType enums.
   */
  public enum PermissionType implements EnumType {

    INSTANCE_ADMIN("instance_admin"),
    ORGANIZATION_ADMIN("organization_admin"),
    ORGANIZATION_EDITOR("organization_editor"),
    ORGANIZATION_READER("organization_reader"),
    WORKSPACE_ADMIN("workspace_admin"),
    WORKSPACE_EDITOR("workspace_editor"),
    WORKSPACE_READER("workspace_reader");

    private final String literal;
    public static final String NAME = "permission_type";

    PermissionType(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"));
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
