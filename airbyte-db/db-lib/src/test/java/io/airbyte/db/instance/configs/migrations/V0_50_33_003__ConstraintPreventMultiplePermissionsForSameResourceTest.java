/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.migrations.V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class V0_50_33_003__ConstraintPreventMultiplePermissionsForSameResourceTest extends AbstractConfigsDatabaseTest {

  static final Table<?> PERMISSION_TABLE = DSL.table("permission");
  static final Field<UUID> USER_ID_FIELD = DSL.field("user_id", UUID.class);
  static final Field<UUID> WORKSPACE_ID_FIELD = DSL.field("workspace_id", UUID.class);
  static final Field<UUID> ORGANIZATION_ID_FIELD = DSL.field("organization_id", UUID.class);

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_33_003__ConstraintPreventMultiplePermissionsForSameResourceTest",
            ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_24_009__AddConstraintInPermissionTable();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @AfterEach
  void afterEach() {
    // Making sure we reset between tests
    dslContext.dropSchemaIfExists("public").cascade().execute();
    dslContext.createSchema("public").execute();
    dslContext.setSchema("public").execute();
  }

  private static void insertWorkspacePermission(final DSLContext ctx,
                                                final UUID userId,
                                                final UUID workspaceId,
                                                final PermissionType permissionType) {
    ctx.insertInto(PERMISSION_TABLE)
        .columns(
            DSL.field("id"),
            USER_ID_FIELD,
            WORKSPACE_ID_FIELD,
            DSL.field("permission_type"))
        .values(
            UUID.randomUUID(),
            userId,
            workspaceId,
            permissionType)
        .execute();
  }

  private static void insertOrganizationPermission(final DSLContext ctx,
                                                   final UUID userId,
                                                   final UUID organizationId,
                                                   final PermissionType permissionType) {
    ctx.insertInto(PERMISSION_TABLE)
        .columns(
            DSL.field("id"),
            USER_ID_FIELD,
            ORGANIZATION_ID_FIELD,
            DSL.field("permission_type"))
        .values(
            UUID.randomUUID(),
            userId,
            organizationId,
            permissionType)
        .execute();
  }

  private static void insertInstanceAdminPermission(final DSLContext ctx, final UUID userId) {
    ctx.insertInto(PERMISSION_TABLE)
        .columns(
            DSL.field("id"),
            USER_ID_FIELD,
            DSL.field("permission_type"))
        .values(
            UUID.randomUUID(),
            userId,
            PermissionType.INSTANCE_ADMIN)
        .execute();
  }

  private static void assertUserWorkspacePermissionCount(final DSLContext ctx, final UUID userId, final UUID workspaceId, final int expectedCount) {
    final int actualCount = ctx.select()
        .from(PERMISSION_TABLE)
        .where(USER_ID_FIELD.eq(userId))
        .and(WORKSPACE_ID_FIELD.eq(workspaceId))
        .fetch()
        .size();
    Assertions.assertEquals(expectedCount, actualCount);
  }

  private static void assertUserOrganizationPermissionCount(final DSLContext ctx,
                                                            final UUID userId,
                                                            final UUID organizationId,
                                                            final int expectedCount) {
    final int actualCount = ctx.select()
        .from(PERMISSION_TABLE)
        .where(USER_ID_FIELD.eq(userId))
        .and(ORGANIZATION_ID_FIELD.eq(organizationId))
        .fetch()
        .size();
    Assertions.assertEquals(expectedCount, actualCount);
  }

  private static void assertUserInstanceAdminPermissionCount(final DSLContext ctx, final UUID userId, final int expectedCount) {
    final int actualCount = ctx.select()
        .from(PERMISSION_TABLE)
        .where(USER_ID_FIELD.eq(userId))
        .and(DSL.field("permission_type").eq(PermissionType.INSTANCE_ADMIN))
        .fetch()
        .size();
    Assertions.assertEquals(expectedCount, actualCount);
  }

  @Test
  void testBeforeMigrate() {
    final DSLContext ctx = getDslContext();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    final UUID userId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final UUID organizationId = UUID.randomUUID();

    // insert a permission for a user and workspace
    insertWorkspacePermission(ctx, userId, workspaceId, PermissionType.WORKSPACE_ADMIN);
    // insert another permission for the same user and workspace, should be allowed because constraint
    // isn't applied yet
    insertWorkspacePermission(ctx, userId, workspaceId, PermissionType.WORKSPACE_READER);

    assertUserWorkspacePermissionCount(ctx, userId, workspaceId, 2);

    // insert a permission for a user and organization
    insertOrganizationPermission(ctx, userId, organizationId, PermissionType.ORGANIZATION_ADMIN);
    // insert another permission for the same user and organization, should be allowed because
    // constraint isn't applied yet
    insertOrganizationPermission(ctx, userId, organizationId, PermissionType.ORGANIZATION_READER);

    assertUserOrganizationPermissionCount(ctx, userId, organizationId, 2);
  }

  @Test
  void testAfterMigrate() {
    final DSLContext ctx = getDslContext();

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;");

    final UUID userId = UUID.randomUUID();
    final UUID workspaceId = UUID.randomUUID();
    final UUID organizationId = UUID.randomUUID();

    V0_50_33_003__ConstraintPreventMultiplePermissionsForSameResource.migrate(ctx);

    // insert a permission for a user and workspace
    insertWorkspacePermission(ctx, userId, workspaceId, PermissionType.WORKSPACE_ADMIN);
    assertUserWorkspacePermissionCount(ctx, userId, workspaceId, 1);

    // insert a permission for a user and organization
    insertOrganizationPermission(ctx, userId, organizationId, PermissionType.ORGANIZATION_ADMIN);
    assertUserOrganizationPermissionCount(ctx, userId, organizationId, 1);

    // insert another permission for the same user and workspace, should be prevented because constraint
    // is applied
    assertThrows(DataAccessException.class, () -> insertWorkspacePermission(ctx, userId, workspaceId, PermissionType.WORKSPACE_READER));

    // insert another permission for the same user and organization, should be prevented because
    // constraint is applied
    assertThrows(DataAccessException.class, () -> insertOrganizationPermission(ctx, userId, organizationId, PermissionType.ORGANIZATION_READER));

    // assert there are still just one permission for each user and workspace/organization
    assertUserWorkspacePermissionCount(ctx, userId, workspaceId, 1);
    assertUserOrganizationPermissionCount(ctx, userId, organizationId, 1);

    // make sure that an instance_admin permission can still be inserted for the same user
    insertInstanceAdminPermission(ctx, userId);
    assertUserInstanceAdminPermissionCount(ctx, userId, 1);
  }

}
