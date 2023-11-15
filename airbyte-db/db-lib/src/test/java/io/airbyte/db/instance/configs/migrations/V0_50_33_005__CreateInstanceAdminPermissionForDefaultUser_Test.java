/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V0_50_33_005__CreateInstanceAdminPermissionForDefaultUser_Test extends AbstractConfigsDatabaseTest {

  private static final UUID DEFAULT_USER_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  // The user table is quoted to avoid conflict with the reserved user keyword in Postgres.
  private static final String USER_TABLE = "\"user\"";
  private static final String PERMISSION_TABLE = "permission";
  private static final Field<UUID> ID_COLUMN = DSL.field("id", SQLDataType.UUID);
  private static final Field<UUID> USER_ID_COLUMN = DSL.field("user_id", SQLDataType.UUID);
  private static final Field<PermissionType> PERMISSION_TYPE_COLUMN =
      DSL.field("permission_type", SQLDataType.VARCHAR.asEnumDataType(PermissionType.class));

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_33_005__CreateInstanceAdminPermissionForDefaultUser", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_33_004__AddSecretPersistenceTypeColumnAndAlterConstraint();
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

  @Test
  void testMigrationDefaultState() {
    final DSLContext ctx = getDslContext();

    // a prior migration should have already inserted the default user
    var userRecord = ctx.selectFrom(DSL.table(USER_TABLE))
        .where(ID_COLUMN.eq(DEFAULT_USER_ID))
        .fetchOne();
    assertNotNull(userRecord);

    var instanceAdminPermission = ctx.selectFrom(DSL.table(PERMISSION_TABLE))
        .where(USER_ID_COLUMN.eq(DEFAULT_USER_ID))
        .and(PERMISSION_TYPE_COLUMN.eq(PermissionType.INSTANCE_ADMIN))
        .fetchOne();

    // instance_admin record should not yet exist for the default user
    assertNull(instanceAdminPermission);

    // run the migration
    V0_50_33_005__CreateInstanceAdminPermissionForDefaultUser.createInstanceAdminPermissionForDefaultUser(ctx);

    // verify that an instance_admin permission record was written to the database for the default user
    instanceAdminPermission = ctx.selectFrom(DSL.table(PERMISSION_TABLE))
        .where(USER_ID_COLUMN.eq(DEFAULT_USER_ID))
        .and(PERMISSION_TYPE_COLUMN.eq(PermissionType.INSTANCE_ADMIN))
        .fetchOne();

    assertNotNull(instanceAdminPermission);
  }

  @Test
  void testMigrationNoDefaultUser() {
    final DSLContext ctx = getDslContext();

    // a prior migration should have already inserted the default user
    var userRecord = ctx.selectFrom(DSL.table(USER_TABLE))
        .where(ID_COLUMN.eq(DEFAULT_USER_ID))
        .fetchOne();
    assertNotNull(userRecord);

    var instanceAdminPermission = ctx.selectFrom(DSL.table(PERMISSION_TABLE))
        .where(USER_ID_COLUMN.eq(DEFAULT_USER_ID))
        .and(PERMISSION_TYPE_COLUMN.eq(PermissionType.INSTANCE_ADMIN))
        .fetchOne();

    // instance_admin record should not yet exist for the default user
    assertNull(instanceAdminPermission);

    // remove the default user to simulate this non-conventional state
    ctx.deleteFrom(DSL.table(USER_TABLE))
        .where(ID_COLUMN.eq(DEFAULT_USER_ID))
        .execute();

    // record the count of permission records at this time
    final var permissionCountBeforeMigration = ctx.fetchCount(DSL.table(PERMISSION_TABLE));

    // run the migration
    V0_50_33_005__CreateInstanceAdminPermissionForDefaultUser.createInstanceAdminPermissionForDefaultUser(ctx);

    // verify that the permission record count is unchanged because this should be a no-op.
    final var permissionCountAfterMigration = ctx.fetchCount(DSL.table(PERMISSION_TABLE));

    assertEquals(permissionCountBeforeMigration, permissionCountAfterMigration);
  }

  @Test
  void testMigrationAlreadyInstanceAdmin() {
    final DSLContext ctx = getDslContext();

    // a prior migration should have already inserted the default user
    var userRecord = ctx.selectFrom(DSL.table(USER_TABLE))
        .where(ID_COLUMN.eq(DEFAULT_USER_ID))
        .fetchOne();
    assertNotNull(userRecord);

    var instanceAdminPermission = ctx.selectFrom(DSL.table(PERMISSION_TABLE))
        .where(USER_ID_COLUMN.eq(DEFAULT_USER_ID))
        .and(PERMISSION_TYPE_COLUMN.eq(PermissionType.INSTANCE_ADMIN))
        .fetchOne();

    // instance_admin record should not yet exist for the default user
    assertNull(instanceAdminPermission);

    // manually insert an instance_admin permission record for the default user
    ctx.insertInto(DSL.table(PERMISSION_TABLE),
        ID_COLUMN,
        USER_ID_COLUMN,
        PERMISSION_TYPE_COLUMN)
        .values(UUID.randomUUID(), DEFAULT_USER_ID, PermissionType.INSTANCE_ADMIN)
        .execute();

    // record the count of permission records at this time
    final var permissionCountBeforeMigration = ctx.fetchCount(DSL.table(PERMISSION_TABLE));

    // run the migration
    V0_50_33_005__CreateInstanceAdminPermissionForDefaultUser.createInstanceAdminPermissionForDefaultUser(ctx);

    // verify that the permission record count is unchanged because this should be a no-op.
    final var permissionCountAfterMigration = ctx.fetchCount(DSL.table(PERMISSION_TABLE));

    assertEquals(permissionCountBeforeMigration, permissionCountAfterMigration);
  }

}
