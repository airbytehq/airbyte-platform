/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.USER_INVITATION_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Relax the user_invitation scope/permission constraint added in
 * [V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch] to also permit the
 * organization_member role. This lets organizations invite new users as plain members.
 *
 * This only widens the set of permitted permission types, so no existing rows can become invalid;
 * the constraint is dropped and re-added with organization_member included.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_030__AllowOrganizationMemberInvitations : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    runMigration(ctx)
  }

  companion object {
    private const val CONSTRAINT_NAME = "user_invitation_scope_permission_mismatch"

    fun runMigration(ctx: DSLContext) {
      ctx.execute("ALTER TABLE $USER_INVITATION_TABLE DROP CONSTRAINT IF EXISTS $CONSTRAINT_NAME")
      ctx.execute(
        """
        ALTER TABLE $USER_INVITATION_TABLE ADD CONSTRAINT $CONSTRAINT_NAME CHECK (
          (
            scope_type = 'workspace'
            AND permission_type IN ('workspace_admin', 'workspace_editor', 'workspace_runner', 'workspace_reader')
          )
          OR (
            scope_type = 'organization'
            AND permission_type IN (
              'organization_admin', 'organization_editor', 'organization_runner', 'organization_reader', 'organization_member'
            )
          )
        )
        """.trimIndent(),
      )
    }
  }
}
