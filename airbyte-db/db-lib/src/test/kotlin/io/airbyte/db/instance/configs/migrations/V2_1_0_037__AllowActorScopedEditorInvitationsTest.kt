/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.exception.IntegrityConstraintViolationException
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_037__AllowActorScopedEditorInvitationsTest : AbstractConfigsDatabaseTest() {
  private lateinit var ctx: DSLContext

  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V2_1_0_037__AllowActorScopedEditorInvitationsTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V2_1_0_036__AddActorScopedWorkspacePermissionTypes()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    ctx = dslContext!!
    ctx.deleteFrom(DSL.table(USER_INVITATION_TABLE)).execute()
    // The test container is shared across the methods of a test class and a widened constraint is
    // not rolled back, so restore the constraint this migration replaces.
    V2_1_0_030__AllowOrganizationMemberInvitations.runMigration(ctx)
  }

  @Test
  fun testActorScopedEditorInvitationRejectedBeforeMigration() {
    assertThrows(IntegrityConstraintViolationException::class.java) {
      insertInvitation(ctx, "workspace_source_editor", "workspace")
    }
    assertThrows(IntegrityConstraintViolationException::class.java) {
      insertInvitation(ctx, "workspace_destination_editor", "workspace")
    }
  }

  @Test
  fun testActorScopedEditorInvitationAllowedAfterMigration() {
    V2_1_0_037__AllowActorScopedEditorInvitations.runMigration(ctx)

    insertInvitation(ctx, "workspace_source_editor", "workspace")
    insertInvitation(ctx, "workspace_destination_editor", "workspace")
    insertInvitation(ctx, "workspace_editor", "workspace")
    insertInvitation(ctx, "organization_member", "organization")

    val result = ctx.selectFrom(DSL.table(USER_INVITATION_TABLE)).fetch()
    assertEquals(4, result.size)
  }

  @Test
  fun testInvalidScopePermissionStillRejectedAfterMigration() {
    V2_1_0_037__AllowActorScopedEditorInvitations.runMigration(ctx)

    // The actor-scoped editor roles are workspace-only and must not be valid for an
    // organization-scoped invite.
    assertThrows(IntegrityConstraintViolationException::class.java) {
      insertInvitation(ctx, "workspace_source_editor", "organization")
    }
    assertThrows(IntegrityConstraintViolationException::class.java) {
      insertInvitation(ctx, "workspace_destination_editor", "organization")
    }
    assertThrows(IntegrityConstraintViolationException::class.java) {
      insertInvitation(ctx, "organization_member", "workspace")
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
