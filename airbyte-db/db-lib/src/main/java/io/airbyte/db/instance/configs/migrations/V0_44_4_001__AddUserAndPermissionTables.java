/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.PERMISSION_TABLE;
import static io.airbyte.db.instance.DatabaseConstants.USER_TABLE;
import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.Catalog;
import org.jooq.DSLContext;
import org.jooq.EnumType;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Schema;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.SchemaImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Migration to add User and Permission tables to the OSS Config DB. These are essentially
 * duplicated from Cloud, in preparation for introducing user-based auth to OSS.
 */
public class V0_44_4_001__AddUserAndPermissionTables extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_44_4_001__AddUserAndPermissionTables.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());

    LOGGER.info("Creating user data types, table, and indexes...");
    createStatusEnumType(ctx);
    createAuthProviderEnumType(ctx);
    createUserTableAndIndexes(ctx);

    LOGGER.info("Creating permission data types, table, and indexes...");
    createPermissionTypeEnumType(ctx);
    createPermissionTableAndIndexes(ctx);

    LOGGER.info("Migration finished!");
  }

  private static void createStatusEnumType(final DSLContext ctx) {
    ctx.createType(Status.NAME)
        .asEnum(Arrays.stream(Status.values()).map(Status::getLiteral).toArray(String[]::new))
        .execute();
  }

  private static void createAuthProviderEnumType(final DSLContext ctx) {
    ctx.createType(AuthProvider.NAME)
        .asEnum(Arrays.stream(AuthProvider.values()).map(AuthProvider::getLiteral).toArray(String[]::new))
        .execute();
  }

  private static void createPermissionTypeEnumType(final DSLContext ctx) {
    ctx.createType(PermissionType.NAME)
        .asEnum(Arrays.stream(PermissionType.values()).map(PermissionType::getLiteral).toArray(String[]::new))
        .execute();
  }

  private static void createUserTableAndIndexes(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<String> name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false));
    final Field<String> authUserId = DSL.field("auth_user_id", SQLDataType.VARCHAR(256).nullable(false));
    final Field<AuthProvider> authProvider = DSL.field("auth_provider", SQLDataType.VARCHAR.asEnumDataType(AuthProvider.class).nullable(false));
    final Field<UUID> defaultWorkspaceId = DSL.field("default_workspace_id", SQLDataType.UUID.nullable(true));
    final Field<Status> status = DSL.field("status", SQLDataType.VARCHAR.asEnumDataType(Status.class).nullable(true));
    final Field<String> companyName = DSL.field("company_name", SQLDataType.VARCHAR(256).nullable(true));
    final Field<String> email = DSL.field("email", SQLDataType.VARCHAR(256).nullable(false)); // this was nullable in cloud, but should be required.
    final Field<Boolean> news = DSL.field("news", SQLDataType.BOOLEAN.nullable(true));
    final Field<JSONB> uiMetadata = DSL.field("ui_metadata", SQLDataType.JSONB.nullable(true));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists(USER_TABLE)
        .columns(id,
            name,
            authUserId,
            authProvider,
            defaultWorkspaceId,
            status,
            companyName,
            email,
            news,
            uiMetadata,
            createdAt,
            updatedAt)
        .constraints(
            primaryKey(id),
            foreignKey(defaultWorkspaceId).references("workspace", "id").onDeleteSetNull())
        .execute();

    ctx.createIndexIfNotExists("user_auth_provider_auth_user_id_idx")
        .on(USER_TABLE, "auth_provider", "auth_user_id")
        .execute();

    ctx.createIndexIfNotExists("user_email_idx")
        .on(USER_TABLE, "email")
        .execute();
  }

  private static void createPermissionTableAndIndexes(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<UUID> userId = DSL.field("user_id", SQLDataType.UUID.nullable(false));
    final Field<UUID> workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(true));
    final Field<PermissionType> permissionType =
        DSL.field("permission_type", SQLDataType.VARCHAR.asEnumDataType(PermissionType.class).nullable(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists(PERMISSION_TABLE)
        .columns(id,
            userId,
            workspaceId,
            permissionType,
            createdAt,
            updatedAt)
        .constraints(
            primaryKey(id),
            foreignKey(userId).references(USER_TABLE, "id").onDeleteCascade(),
            foreignKey(workspaceId).references("workspace", "id").onDeleteCascade())
        .execute();

    ctx.createIndexIfNotExists("permission_user_id_idx")
        .on(PERMISSION_TABLE, "user_id")
        .execute();

    ctx.createIndexIfNotExists("permission_workspace_id_idx")
        .on(PERMISSION_TABLE, "workspace_id")
        .execute();
  }

  /**
   * User Status enum copied from Cloud DB.
   */
  public enum Status implements EnumType {

    INVITED("invited"),
    REGISTERED("registered"),
    DISABLED("disabled");

    private final String literal;
    public static final String NAME = "status";

    Status(final String literal) {
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
   * User AuthProvider enum copied from Cloud DB.
   */
  public enum AuthProvider implements EnumType {

    GOOGLE_IDENTITY_PLATFORM("google_identity_platform");

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
   * User PermissionType enum copied from Cloud DB.
   */
  public enum PermissionType implements EnumType {

    INSTANCE_ADMIN("instance_admin"),
    WORKSPACE_OWNER("workspace_owner");

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
