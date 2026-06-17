/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.exception.IntegrityConstraintViolationException
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_030__AllowOrganizationMemberInvitationsTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V2_1_0_029__DropWorkloadLabelTableTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V2_1_0_029__DropWorkloadLabelTable()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
    val ctx = dslContext!!
    ctx.deleteFrom(DSL.table(USER_INVITATION_TABLE)).execute()
  }

  @Test
  fun testOrganizationMemberInvitationRejectedBeforeMigration() {
    val ctx = dslContext!!
    Assertions.assertThrows(IntegrityConstraintViolationException::class.java) {
      insertInvitation(ctx, "organization_member", "organization")
    }
  }

  @Test
  fun testOrganizationMemberInvitationAllowedAfterMigration() {
    val ctx = dslContext!!
    V2_1_0_030__AllowOrganizationMemberInvitations.runMigration(ctx)

    insertInvitation(ctx, "organization_member", "organization")
    insertInvitation(ctx, "organization_admin", "organization")
    insertInvitation(ctx, "workspace_admin", "workspace")

    val result = ctx.selectFrom(DSL.table(USER_INVITATION_TABLE)).fetch()
    Assertions.assertEquals(3, result.size)
  }

  @Test
  fun testInvalidScopePermissionStillRejectedAfterMigration() {
    val ctx = dslContext!!
    V2_1_0_030__AllowOrganizationMemberInvitations.runMigration(ctx)

    // organization_member is an organization role and must not be valid for a workspace-scoped invite.
    Assertions.assertThrows(IntegrityConstraintViolationException::class.java) {
      insertInvitation(ctx, "organization_member", "workspace")
    }
    Assertions.assertThrows(IntegrityConstraintViolationException::class.java) {
      insertInvitation(ctx, "workspace_admin", "organization")
    }
  }

  private fun insertInvitation(
    ctx: DSLContext,
    permissionType: String,
    scopeType: String,
  ) {
    ctx
      .execute(
        """
        INSERT INTO $USER_INVITATION_TABLE
          (id, inviter_user_id, invite_code, invited_email, status, permission_type, scope_type, scope_id, expires_at)
        VALUES
          (?, ?, ?, ?, 'pending'::invitation_status, ?::permission_type, ?::scope_type, ?, now())
        """.trimIndent(),
        UUID.randomUUID(),
        DEFAULT_USER_ID,
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString() + "@test.com",
        permissionType,
        scopeType,
        UUID.randomUUID(),
      )
  }

  companion object {
    private const val USER_INVITATION_TABLE = "user_invitation"
    private val DEFAULT_USER_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
  }
}
