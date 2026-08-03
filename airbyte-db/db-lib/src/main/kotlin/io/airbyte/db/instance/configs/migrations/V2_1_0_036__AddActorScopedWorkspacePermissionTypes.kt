/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Add the actor-scoped workspace editor roles `workspace_source_editor` and
 * `workspace_destination_editor` to the `permission_type` enum. Each is a workspace editor minus the
 * opposite actor type.
 *
 * Widening the `user_invitation` scope/permission constraint to accept the new roles is deliberately
 * left to [V2_1_0_037__AllowActorScopedEditorInvitations]: Postgres refuses to use an enum label in
 * the same transaction that added it, and Flyway runs each migration in its own transaction.
 *
 * Postgres cannot remove a label from an enum, so this is not reversible in place. It is safe to
 * leave applied on rollback — no rows are written here, and nothing reads the new labels until the
 * role model ships.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_036__AddActorScopedWorkspacePermissionTypes : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    runMigration(ctx)
  }

  companion object {
    fun runMigration(ctx: DSLContext) {
      ctx.execute("ALTER TYPE permission_type ADD VALUE IF NOT EXISTS 'workspace_source_editor'")
      ctx.execute("ALTER TYPE permission_type ADD VALUE IF NOT EXISTS 'workspace_destination_editor'")
    }
  }
}
