/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.exception.IntegrityConstraintViolationException
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_64_4_002__AddJobRunnerPermissionTypesTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_64_4_002__AddJobRunnerPermissionTypesTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_64_4_001__AddFinalizationInputToWorkload()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
    val ctx = getDslContext()
    ctx.deleteFrom(PERMISSION_TABLE).execute()
    ctx.deleteFrom(USER_TABLE).execute()
  }

  @Test
  fun testCreateOrgRunnerPermission() {
    val ctx = getDslContext()
    V0_64_4_002__AddJobRunnerPermissionTypes.runMigration(ctx)
    val userId = createUser(ctx, "test@test.com")
    createOrgPermission(ctx, userId, PermissionType.ORGANIZATION_RUNNER)
    Assertions.assertEquals(
      1,
      ctx.fetch("SELECT * FROM permission WHERE permission_type = 'organization_runner'").size,
    )
  }

  @Test
  fun testCreateWorkspaceRunnerPermission() {
    val ctx = getDslContext()
    V0_64_4_002__AddJobRunnerPermissionTypes.runMigration(ctx)
    val workspaceId = UUID.randomUUID()
    createWorkspace(ctx, workspaceId, DEFAULT_ORGANIZATION_ID)
    val userId = createUser(ctx, "test@test.com")
    createWorkspacePermission(ctx, userId, workspaceId, PermissionType.WORKSPACE_RUNNER)
    Assertions.assertEquals(
      1,
      ctx.fetch("SELECT * FROM permission WHERE permission_type = 'workspace_runner'").size,
    )
  }

  @Test
  fun testCreateOldPermission() {
    val ctx = getDslContext()
    V0_64_4_002__AddJobRunnerPermissionTypes.runMigration(ctx)
    val workspaceId = UUID.randomUUID()
    createWorkspace(ctx, workspaceId, DEFAULT_ORGANIZATION_ID)
    val userId = createUser(ctx, "test@test.com")
    createWorkspacePermission(ctx, userId, workspaceId, PermissionType.WORKSPACE_ADMIN)
    createOrgPermission(ctx, userId, PermissionType.ORGANIZATION_ADMIN)
    Assertions.assertEquals(1, ctx.fetch("SELECT * FROM permission WHERE permission_type = 'workspace_admin'").size)
    Assertions.assertEquals(
      1,
      ctx.fetch("SELECT * FROM permission WHERE permission_type = 'organization_admin'").size,
    )
  }

  @Test
  fun testCreateInvalidPermission() {
    val ctx = getDslContext()
    V0_64_4_002__AddJobRunnerPermissionTypes.runMigration(ctx)
    val workspaceId = UUID.randomUUID()
    createWorkspace(ctx, workspaceId, DEFAULT_ORGANIZATION_ID)
    val userId = createUser(ctx, "test@test.com")
    Assertions.assertThrows(
      IntegrityConstraintViolationException::class.java,
    ) {
      createWorkspacePermission(
        ctx,
        userId,
        workspaceId,
        PermissionType.INSTANCE_ADMIN,
      )
    }
    Assertions.assertThrows(
      IntegrityConstraintViolationException::class.java,
    ) {
      createOrgPermission(
        ctx,
        userId,
        PermissionType.INSTANCE_ADMIN,
      )
    }
  }

  /**
   * User Roles as PermissionType enums.
   */
  internal enum class PermissionType(
    private val literal: String,
  ) : EnumType {
    INSTANCE_ADMIN("instance_admin"),
    ORGANIZATION_ADMIN("organization_admin"),
    ORGANIZATION_EDITOR("organization_editor"),
    ORGANIZATION_RUNNER("organization_runner"),
    ORGANIZATION_READER("organization_reader"),
    WORKSPACE_ADMIN("workspace_admin"),
    WORKSPACE_EDITOR("workspace_editor"),
    WORKSPACE_RUNNER("workspace_runner"),
    WORKSPACE_READER("workspace_reader"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "permission_type"
    }
  }

  companion object {
    private val PERMISSION_TABLE = DSL.table("permission")
    private val USER_TABLE = DSL.table("\"user\"")
    private val EMAIL = DSL.field("email", SQLDataType.VARCHAR)
    private val ID = DSL.field("id", SQLDataType.UUID)
    private val USER_ID = DSL.field("user_id", SQLDataType.UUID)
    private val PERMISSION_TYPE =
      DSL.field(
        "permission_type",
        SQLDataType.VARCHAR.asEnumDataType(
          PermissionType::class.java,
        ),
      )
    private val ORGANIZATION_ID = DSL.field("organization_id", SQLDataType.UUID)
    private val WORKSPACE_ID = DSL.field("workspace_id", SQLDataType.UUID)
    private val DEFAULT_ORGANIZATION_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")

    private fun createUser(
      ctx: DSLContext,
      email: String,
    ): UUID {
      val userId = UUID.randomUUID()
      ctx
        .insertInto(USER_TABLE, ID, EMAIL, DSL.field("name"))
        .values(userId, email, "Name")
        .execute()
      return userId
    }

    private fun createOrgPermission(
      ctx: DSLContext,
      userId: UUID,
      permissionType: PermissionType,
    ) {
      ctx
        .insertInto(PERMISSION_TABLE)
        .set(ID, UUID.randomUUID())
        .set(USER_ID, userId)
        .set(PERMISSION_TYPE, permissionType)
        .set(ORGANIZATION_ID, DEFAULT_ORGANIZATION_ID)
        .execute()
    }

    private fun createWorkspacePermission(
      ctx: DSLContext,
      userId: UUID,
      workspaceId: UUID,
      permissionType: PermissionType,
    ) {
      ctx
        .insertInto(PERMISSION_TABLE)
        .set(ID, userId)
        .set(USER_ID, userId)
        .set(WORKSPACE_ID, workspaceId)
        .set(PERMISSION_TYPE, permissionType)
        .execute()
    }

    private fun createWorkspace(
      ctx: DSLContext,
      workspaceId: UUID,
      organizationId: UUID,
    ) {
      ctx
        .insertInto(DSL.table("workspace"))
        .columns(
          DSL.field("id"),
          DSL.field("name"),
          DSL.field("slug"),
          DSL.field("initial_setup_complete"),
          DSL.field("organization_id"),
        ).values(
          workspaceId,
          "workspace",
          "workspace",
          true,
          organizationId,
        ).execute()
    }
  }
}
