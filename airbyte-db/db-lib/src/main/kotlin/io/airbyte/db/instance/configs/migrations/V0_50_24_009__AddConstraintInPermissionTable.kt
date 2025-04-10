/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.Table
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_50_24_009__AddConstraintInPermissionTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addPermissionConstraint(ctx)
  }

  companion object {
    private val PERMISSION_TABLE: Table<*> = DSL.table("permission")
    private const val ORGANIZATION_ID = "organization_id"
    private const val WORKSPACE_ID = "workspace_id"
    private const val PERMISSION_TYPE = "permission_type"
    private const val PERMISSION_CONSTRAINT_NAME = "permission_check_organization_id_and_workspace_id"

    private fun addPermissionConstraint(ctx: DSLContext) {
      ctx
        .alterTable(PERMISSION_TABLE)
        .add(
          DSL.constraint(PERMISSION_CONSTRAINT_NAME).check(
            DSL
              .field(PERMISSION_TYPE)
              .eq("instance_admin")
              .and(DSL.field(WORKSPACE_ID).isNull())
              .and(DSL.field(ORGANIZATION_ID).isNull())
              .or(
                DSL
                  .field(PERMISSION_TYPE)
                  .`in`("organization_admin", "organization_editor", "organization_reader", "organization_member")
                  .and(DSL.field(WORKSPACE_ID).isNull())
                  .and(DSL.field(ORGANIZATION_ID).isNotNull()),
              ).or(
                DSL
                  .field(PERMISSION_TYPE)
                  .`in`("workspace_admin", "workspace_editor", "workspace_reader")
                  .and(DSL.field(WORKSPACE_ID).isNotNull())
                  .and(DSL.field(ORGANIZATION_ID).isNull()),
              ),
          ),
        ).execute()
    }
  }
}
