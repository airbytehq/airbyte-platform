/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_50_33_005__CreateInstanceAdminPermissionForDefaultUser.Companion.createInstanceAdminPermissionForDefaultUser
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_33_005__CreateInstanceAdminPermissionForDefaultUser_Test : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_33_005__CreateInstanceAdminPermissionForDefaultUser",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_50_33_004__AddSecretPersistenceTypeColumnAndAlterConstraint()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @AfterEach
  fun afterEach() {
    // Making sure we reset between tests
    dslContext.dropSchemaIfExists("public").cascade().execute()
    dslContext.createSchema("public").execute()
    dslContext.setSchema("public").execute()
  }

  @Test
  fun testMigrationDefaultState() {
    val ctx = getDslContext()

    // a prior migration should have already inserted the default user
    val userRecord =
      ctx
        .selectFrom(DSL.table(USER_TABLE))
        .where(ID_COLUMN.eq(DEFAULT_USER_ID))
        .fetchOne()
    Assertions.assertNotNull(userRecord)

    var instanceAdminPermission =
      ctx
        .selectFrom(DSL.table(PERMISSION_TABLE))
        .where(USER_ID_COLUMN.eq(DEFAULT_USER_ID))
        .and(PERMISSION_TYPE_COLUMN.eq(V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.INSTANCE_ADMIN))
        .fetchOne()

    // instance_admin record should not yet exist for the default user
    Assertions.assertNull(instanceAdminPermission)

    // run the migration
    createInstanceAdminPermissionForDefaultUser(ctx)

    // verify that an instance_admin permission record was written to the database for the default user
    instanceAdminPermission =
      ctx
        .selectFrom(DSL.table(PERMISSION_TABLE))
        .where(USER_ID_COLUMN.eq(DEFAULT_USER_ID))
        .and(PERMISSION_TYPE_COLUMN.eq(V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.INSTANCE_ADMIN))
        .fetchOne()

    Assertions.assertNotNull(instanceAdminPermission)
  }

  @Test
  fun testMigrationNoDefaultUser() {
    val ctx = getDslContext()

    // a prior migration should have already inserted the default user
    val userRecord =
      ctx
        .selectFrom(DSL.table(USER_TABLE))
        .where(ID_COLUMN.eq(DEFAULT_USER_ID))
        .fetchOne()
    Assertions.assertNotNull(userRecord)

    val instanceAdminPermission =
      ctx
        .selectFrom(DSL.table(PERMISSION_TABLE))
        .where(USER_ID_COLUMN.eq(DEFAULT_USER_ID))
        .and(PERMISSION_TYPE_COLUMN.eq(V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.INSTANCE_ADMIN))
        .fetchOne()

    // instance_admin record should not yet exist for the default user
    Assertions.assertNull(instanceAdminPermission)

    // remove the default user to simulate this non-conventional state
    ctx
      .deleteFrom(DSL.table(USER_TABLE))
      .where(ID_COLUMN.eq(DEFAULT_USER_ID))
      .execute()

    // record the count of permission records at this time
    val permissionCountBeforeMigration = ctx.fetchCount(DSL.table(PERMISSION_TABLE))

    // run the migration
    createInstanceAdminPermissionForDefaultUser(ctx)

    // verify that the permission record count is unchanged because this should be a no-op.
    val permissionCountAfterMigration = ctx.fetchCount(DSL.table(PERMISSION_TABLE))

    Assertions.assertEquals(permissionCountBeforeMigration, permissionCountAfterMigration)
  }

  @Test
  fun testMigrationAlreadyInstanceAdmin() {
    val ctx = getDslContext()

    // a prior migration should have already inserted the default user
    val userRecord =
      ctx
        .selectFrom(DSL.table(USER_TABLE))
        .where(ID_COLUMN.eq(DEFAULT_USER_ID))
        .fetchOne()
    Assertions.assertNotNull(userRecord)

    val instanceAdminPermission =
      ctx
        .selectFrom(DSL.table(PERMISSION_TABLE))
        .where(USER_ID_COLUMN.eq(DEFAULT_USER_ID))
        .and(PERMISSION_TYPE_COLUMN.eq(V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.INSTANCE_ADMIN))
        .fetchOne()

    // instance_admin record should not yet exist for the default user
    Assertions.assertNull(instanceAdminPermission)

    // manually insert an instance_admin permission record for the default user
    ctx
      .insertInto(
        DSL.table(PERMISSION_TABLE),
        ID_COLUMN,
        USER_ID_COLUMN,
        PERMISSION_TYPE_COLUMN,
      ).values(
        UUID.randomUUID(),
        DEFAULT_USER_ID,
        V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.INSTANCE_ADMIN,
      ).execute()

    // record the count of permission records at this time
    val permissionCountBeforeMigration = ctx.fetchCount(DSL.table(PERMISSION_TABLE))

    // run the migration
    createInstanceAdminPermissionForDefaultUser(ctx)

    // verify that the permission record count is unchanged because this should be a no-op.
    val permissionCountAfterMigration = ctx.fetchCount(DSL.table(PERMISSION_TABLE))

    Assertions.assertEquals(permissionCountBeforeMigration, permissionCountAfterMigration)
  }

  companion object {
    private val DEFAULT_USER_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")

    // The user table is quoted to avoid conflict with the reserved user keyword in Postgres.
    private const val USER_TABLE = "\"user\""
    private const val PERMISSION_TABLE = "permission"
    private val ID_COLUMN = DSL.field("id", SQLDataType.UUID)
    private val USER_ID_COLUMN = DSL.field("user_id", SQLDataType.UUID)
    private val PERMISSION_TYPE_COLUMN =
      DSL.field(
        "permission_type",
        SQLDataType.VARCHAR.asEnumDataType(
          V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType::class.java,
        ),
      )
  }
}
