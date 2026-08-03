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
 * Relax the user_invitation scope/permission constraint last widened in
 * [V2_1_0_030__AllowOrganizationMemberInvitations] to also permit the actor-scoped workspace editor
 * roles added in [V2_1_0_036__AddActorScopedWorkspacePermissionTypes]. This lets workspaces invite
 * new users directly as source or destination editors.
 *
 * This only widens the set of permitted permission types, so no existing rows can become invalid;
 * the constraint is dropped and re-added with the two new workspace roles included. Reversing it
 * means re-adding the previous constraint, which is safe as long as no invitation rows carry the new
 * roles.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_037__AllowActorScopedEditorInvitations : BaseJavaMigration() {
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
            AND permission_type IN (
              'workspace_admin', 'workspace_editor', 'workspace_source_editor', 'workspace_destination_editor',
              'workspace_runner', 'workspace_reader'
            )
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
