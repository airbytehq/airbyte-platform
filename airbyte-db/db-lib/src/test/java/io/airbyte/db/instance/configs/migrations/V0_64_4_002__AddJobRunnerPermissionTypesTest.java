/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Catalog;
import org.jooq.DSLContext;
import org.jooq.EnumType;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.exception.IntegrityConstraintViolationException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.SchemaImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class V0_64_4_002__AddJobRunnerPermissionTypesTest extends AbstractConfigsDatabaseTest {

  private static final Table<Record> PERMISSION_TABLE = DSL.table("permission");
  private static final Table<Record> USER_TABLE = DSL.table("\"user\"");
  private static final Field<String> EMAIL = DSL.field("email", SQLDataType.VARCHAR);
  private static final Field<UUID> ID = DSL.field("id", SQLDataType.UUID);
  private static final Field<UUID> USER_ID = DSL.field("user_id", SQLDataType.UUID);
  private static final Field<PermissionType> PERMISSION_TYPE = DSL.field("permission_type", SQLDataType.VARCHAR.asEnumDataType(PermissionType.class));
  private static final Field<UUID> ORGANIZATION_ID = DSL.field("organization_id", SQLDataType.UUID);
  private static final Field<UUID> WORKSPACE_ID = DSL.field("workspace_id", SQLDataType.UUID);
  private static final UUID DEFAULT_ORGANIZATION_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_64_4_002__AddJobRunnerPermissionTypesTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_64_4_001__AddFinalizationInputToWorkload();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
    final DSLContext ctx = getDslContext();
    ctx.deleteFrom(PERMISSION_TABLE).execute();
    ctx.deleteFrom(USER_TABLE).execute();

  }

  @Test
  void testCreateOrgRunnerPermission() {
    final DSLContext ctx = getDslContext();
    V0_64_4_002__AddJobRunnerPermissionTypes.runMigration(ctx);
    final UUID userId = createUser(ctx, "test@test.com");
    createOrgPermission(ctx, userId, PermissionType.ORGANIZATION_RUNNER);
    Assertions.assertEquals(1, ctx.fetch("SELECT * FROM permission WHERE permission_type = 'organization_runner'").size());
  }

  @Test
  void testCreateWorkspaceRunnerPermission() {
    final DSLContext ctx = getDslContext();
    V0_64_4_002__AddJobRunnerPermissionTypes.runMigration(ctx);
    final UUID workspaceId = UUID.randomUUID();
    createWorkspace(ctx, workspaceId, DEFAULT_ORGANIZATION_ID);
    final UUID userId = createUser(ctx, "test@test.com");
    createWorkspacePermission(ctx, userId, workspaceId, PermissionType.WORKSPACE_RUNNER);
    Assertions.assertEquals(1, ctx.fetch("SELECT * FROM permission WHERE permission_type = 'workspace_runner'").size());
  }

  @Test
  void testCreateOldPermission() {
    final DSLContext ctx = getDslContext();
    V0_64_4_002__AddJobRunnerPermissionTypes.runMigration(ctx);
    final UUID workspaceId = UUID.randomUUID();
    createWorkspace(ctx, workspaceId, DEFAULT_ORGANIZATION_ID);
    final UUID userId = createUser(ctx, "test@test.com");
    createWorkspacePermission(ctx, userId, workspaceId, PermissionType.WORKSPACE_ADMIN);
    createOrgPermission(ctx, userId, PermissionType.ORGANIZATION_ADMIN);
    Assertions.assertEquals(1, ctx.fetch("SELECT * FROM permission WHERE permission_type = 'workspace_admin'").size());
    Assertions.assertEquals(1, ctx.fetch("SELECT * FROM permission WHERE permission_type = 'organization_admin'").size());
  }

  @Test
  void testCreateInvalidPermission() {
    final DSLContext ctx = getDslContext();
    V0_64_4_002__AddJobRunnerPermissionTypes.runMigration(ctx);
    final UUID workspaceId = UUID.randomUUID();
    createWorkspace(ctx, workspaceId, DEFAULT_ORGANIZATION_ID);
    final UUID userId = createUser(ctx, "test@test.com");
    Assertions.assertThrows(IntegrityConstraintViolationException.class,
        () -> createWorkspacePermission(ctx, userId, workspaceId, PermissionType.INSTANCE_ADMIN));
    Assertions.assertThrows(IntegrityConstraintViolationException.class, () -> createOrgPermission(ctx, userId, PermissionType.INSTANCE_ADMIN));
  }

  private static UUID createUser(final DSLContext ctx, final String email) {
    final UUID userId = UUID.randomUUID();
    ctx.insertInto(USER_TABLE, ID, EMAIL, DSL.field("name"))
        .values(userId, email, "Name")
        .execute();
    return userId;
  }

  private static void createOrgPermission(final DSLContext ctx, final UUID userId, final PermissionType permissionType) {
    ctx.insertInto(PERMISSION_TABLE)
        .set(ID, UUID.randomUUID())
        .set(USER_ID, userId)
        .set(PERMISSION_TYPE, permissionType)
        .set(ORGANIZATION_ID, DEFAULT_ORGANIZATION_ID)
        .execute();
  }

  private static void createWorkspacePermission(final DSLContext ctx,
                                                final UUID userId,
                                                final UUID workspaceId,
                                                final PermissionType permissionType) {
    ctx.insertInto(PERMISSION_TABLE)
        .set(ID, userId)
        .set(USER_ID, userId)
        .set(WORKSPACE_ID, workspaceId)
        .set(PERMISSION_TYPE, permissionType)
        .execute();
  }

  private static void createWorkspace(final DSLContext ctx, final UUID workspaceId, final UUID organizationId) {
    ctx.insertInto(DSL.table("workspace"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("slug"),
            DSL.field("initial_setup_complete"),
            DSL.field("organization_id"))
        .values(
            workspaceId,
            "workspace",
            "workspace",
            true,
            organizationId)
        .execute();
  }

  /**
   * User Roles as PermissionType enums.
   */
  enum PermissionType implements EnumType {

    INSTANCE_ADMIN("instance_admin"),
    ORGANIZATION_ADMIN("organization_admin"),
    ORGANIZATION_EDITOR("organization_editor"),
    ORGANIZATION_RUNNER("organization_runner"),
    ORGANIZATION_READER("organization_reader"),
    WORKSPACE_ADMIN("workspace_admin"),
    WORKSPACE_EDITOR("workspace_editor"),
    WORKSPACE_RUNNER("workspace_runner"),
    WORKSPACE_READER("workspace_reader");

    private final String literal;
    public static final String NAME = "permission_type";

    PermissionType(final String literal) {
      this.literal = literal;
    }

    @Override
    public @Nullable Catalog getCatalog() {
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
    public @NotNull String getLiteral() {
      return literal;
    }

  }

}
