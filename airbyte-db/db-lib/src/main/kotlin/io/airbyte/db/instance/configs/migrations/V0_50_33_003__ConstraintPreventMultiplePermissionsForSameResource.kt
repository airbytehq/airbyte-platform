/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.Table
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * This migration is to prevent multiple permissions for the same user and workspace/organization.
 * For example, a particular user should not be able to have multiple Workspace-level permissions
 * for the same workspace.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_33_003__ConstraintPreventMultiplePermissionsForSameResource : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    migrate(ctx)
    log.info { "Migration finished!" }
  }

  companion object {
    private val PERMISSION_TABLE: Table<*> = DSL.table("permission")
    private const val ORGANIZATION_ID = "organization_id"
    private const val WORKSPACE_ID = "workspace_id"
    private const val USER_ID = "user_id"
    private const val UNIQUE_CONSTRAINT_NAME_WORKSPACE = "permission_unique_user_workspace"
    private const val UNIQUE_CONSTRAINT_NAME_ORG = "permission_unique_user_organization"

    @JvmStatic
    @VisibleForTesting
    fun migrate(ctx: DSLContext) {
      addUniqueConstraints(ctx)
    }

    private fun addUniqueConstraints(ctx: DSLContext) {
      // Unique constraint for workspace_id and user_id
      ctx
        .alterTable(PERMISSION_TABLE)
        .add(
          DSL
            .constraint(UNIQUE_CONSTRAINT_NAME_WORKSPACE)
            .unique(DSL.field(USER_ID), DSL.field(WORKSPACE_ID)),
        ).execute()

      // Unique constraint for organization_id and user_id
      ctx
        .alterTable(PERMISSION_TABLE)
        .add(
          DSL
            .constraint(UNIQUE_CONSTRAINT_NAME_ORG)
            .unique(DSL.field(USER_ID), DSL.field(ORGANIZATION_ID)),
        ).execute()
    }
  }
}
