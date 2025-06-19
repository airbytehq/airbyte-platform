/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.Table
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_33_003__ConstraintPreventMultiplePermissionsForSameResourceTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_33_003__ConstraintPreventMultiplePermissionsForSameResourceTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_50_24_009__AddConstraintInPermissionTable()
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
  fun testBeforeMigrate() {
    val ctx = getDslContext()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    val userId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()

    // insert a permission for a user and workspace
    insertWorkspacePermission(
      ctx,
      userId,
      workspaceId,
      V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.WORKSPACE_ADMIN,
    )
    // insert another permission for the same user and workspace, should be allowed because constraint
    // isn't applied yet
    insertWorkspacePermission(
      ctx,
      userId,
      workspaceId,
      V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.WORKSPACE_READER,
    )

    assertUserWorkspacePermissionCount(ctx, userId, workspaceId, 2)

    // insert a permission for a user and organization
    insertOrganizationPermission(
      ctx,
      userId,
      organizationId,
      V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.ORGANIZATION_ADMIN,
    )
    // insert another permission for the same user and organization, should be allowed because
    // constraint isn't applied yet
    insertOrganizationPermission(
      ctx,
      userId,
      organizationId,
      V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.ORGANIZATION_READER,
    )

    assertUserOrganizationPermissionCount(ctx, userId, organizationId, 2)
  }

  @Test
  fun testAfterMigrate() {
    val ctx = getDslContext()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    val userId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()

    V0_50_33_003__ConstraintPreventMultiplePermissionsForSameResource.migrate(ctx)

    // insert a permission for a user and workspace
    insertWorkspacePermission(
      ctx,
      userId,
      workspaceId,
      V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.WORKSPACE_ADMIN,
    )
    assertUserWorkspacePermissionCount(ctx, userId, workspaceId, 1)

    // insert a permission for a user and organization
    insertOrganizationPermission(
      ctx,
      userId,
      organizationId,
      V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.ORGANIZATION_ADMIN,
    )
    assertUserOrganizationPermissionCount(ctx, userId, organizationId, 1)

    // insert another permission for the same user and workspace, should be prevented because constraint
    // is applied
    Assertions.assertThrows(
      DataAccessException::class.java,
    ) {
      insertWorkspacePermission(
        ctx,
        userId,
        workspaceId,
        V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.WORKSPACE_READER,
      )
    }

    // insert another permission for the same user and organization, should be prevented because
    // constraint is applied
    Assertions.assertThrows(
      DataAccessException::class.java,
    ) {
      insertOrganizationPermission(
        ctx,
        userId,
        organizationId,
        V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.ORGANIZATION_READER,
      )
    }

    // assert there are still just one permission for each user and workspace/organization
    assertUserWorkspacePermissionCount(ctx, userId, workspaceId, 1)
    assertUserOrganizationPermissionCount(ctx, userId, organizationId, 1)

    // make sure that an instance_admin permission can still be inserted for the same user
    insertInstanceAdminPermission(ctx, userId)
    assertUserInstanceAdminPermissionCount(ctx, userId, 1)
  }

  companion object {
    val PERMISSION_TABLE: Table<*> = DSL.table("permission")
    val USER_ID_FIELD: Field<UUID> = DSL.field("user_id", UUID::class.java)
    val WORKSPACE_ID_FIELD: Field<UUID> =
      DSL.field(
        "workspace_id",
        UUID::class.java,
      )
    val ORGANIZATION_ID_FIELD: Field<UUID> =
      DSL.field(
        "organization_id",
        UUID::class.java,
      )

    private fun insertWorkspacePermission(
      ctx: DSLContext,
      userId: UUID,
      workspaceId: UUID,
      permissionType: V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType,
    ) {
      ctx
        .insertInto(PERMISSION_TABLE)
        .columns(
          DSL.field("id"),
          USER_ID_FIELD,
          WORKSPACE_ID_FIELD,
          DSL.field("permission_type"),
        ).values(
          UUID.randomUUID(),
          userId,
          workspaceId,
          permissionType,
        ).execute()
    }

    private fun insertOrganizationPermission(
      ctx: DSLContext,
      userId: UUID,
      organizationId: UUID,
      permissionType: V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType,
    ) {
      ctx
        .insertInto(PERMISSION_TABLE)
        .columns(
          DSL.field("id"),
          USER_ID_FIELD,
          ORGANIZATION_ID_FIELD,
          DSL.field("permission_type"),
        ).values(
          UUID.randomUUID(),
          userId,
          organizationId,
          permissionType,
        ).execute()
    }

    private fun insertInstanceAdminPermission(
      ctx: DSLContext,
      userId: UUID,
    ) {
      ctx
        .insertInto(PERMISSION_TABLE)
        .columns(
          DSL.field("id"),
          USER_ID_FIELD,
          DSL.field("permission_type"),
        ).values(
          UUID.randomUUID(),
          userId,
          V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.INSTANCE_ADMIN,
        ).execute()
    }

    private fun assertUserWorkspacePermissionCount(
      ctx: DSLContext,
      userId: UUID,
      workspaceId: UUID,
      expectedCount: Int,
    ) {
      val actualCount =
        ctx
          .select()
          .from(PERMISSION_TABLE)
          .where(USER_ID_FIELD.eq(userId))
          .and(WORKSPACE_ID_FIELD.eq(workspaceId))
          .fetch()
          .size
      Assertions.assertEquals(expectedCount, actualCount)
    }

    private fun assertUserOrganizationPermissionCount(
      ctx: DSLContext,
      userId: UUID,
      organizationId: UUID,
      expectedCount: Int,
    ) {
      val actualCount =
        ctx
          .select()
          .from(PERMISSION_TABLE)
          .where(USER_ID_FIELD.eq(userId))
          .and(ORGANIZATION_ID_FIELD.eq(organizationId))
          .fetch()
          .size
      Assertions.assertEquals(expectedCount, actualCount)
    }

    private fun assertUserInstanceAdminPermissionCount(
      ctx: DSLContext,
      userId: UUID,
      expectedCount: Int,
    ) {
      val actualCount =
        ctx
          .select()
          .from(PERMISSION_TABLE)
          .where(USER_ID_FIELD.eq(userId))
          .and(
            DSL
              .field("permission_type")
              .eq(V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.PermissionType.INSTANCE_ADMIN),
          ).fetch()
          .size
      Assertions.assertEquals(expectedCount, actualCount)
    }
  }
}
