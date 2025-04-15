/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Configuration
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_64_4_002__AddJobRunnerPermissionTypes : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    runMigration(ctx)
  }

  companion object {
    private const val PERMISSION_TYPE = "permission_type"
    private const val ORGANIZATION_ID = "organization_id"
    private const val WORKSPACE_ID = "workspace_id"
    private val PERMISSION_TABLE = DSL.table("permission")
    private const val PERMISSION_CONSTRAINT_NAME = "permission_check_organization_id_and_workspace_id"

    @JvmStatic
    fun runMigration(ctx: DSLContext) {
      alterEnum(ctx)
      editPermissionConstraint(ctx)
    }

    private fun alterEnum(ctx: DSLContext) {
      ctx.transaction { configuration: Configuration? ->
        // SO we can do testing more easily
        ctx.execute("ALTER TYPE permission_type ADD VALUE IF NOT EXISTS 'organization_runner'")
        ctx.execute("ALTER TYPE permission_type ADD VALUE IF NOT EXISTS 'workspace_runner'")
      }
    }

    private fun editPermissionConstraint(ctx: DSLContext) {
      ctx.transaction { configuration: Configuration? ->
        ctx
          .alterTable(PERMISSION_TABLE)
          .dropConstraintIfExists(PERMISSION_CONSTRAINT_NAME)
          .execute()
        ctx
          .alterTable(PERMISSION_TABLE)
          .add(
            DSL
              .constraint(PERMISSION_CONSTRAINT_NAME)
              .check(
                DSL
                  .field(PERMISSION_TYPE)
                  .eq("instance_admin")
                  .and(DSL.field(WORKSPACE_ID).isNull())
                  .and(DSL.field(ORGANIZATION_ID).isNull())
                  .or(
                    DSL
                      .field(PERMISSION_TYPE)
                      .`in`(
                        "organization_admin",
                        "organization_editor",
                        "organization_reader",
                        "organization_member",
                        "organization_runner",
                      ).and(DSL.field(WORKSPACE_ID).isNull())
                      .and(DSL.field(ORGANIZATION_ID).isNotNull()),
                  ).or(
                    DSL
                      .field(PERMISSION_TYPE)
                      .`in`(
                        "workspace_admin",
                        "workspace_editor",
                        "workspace_reader",
                        "workspace_runner",
                      ).and(DSL.field(WORKSPACE_ID).isNotNull())
                      .and(DSL.field(ORGANIZATION_ID).isNull()),
                  ),
              ),
          ).execute()
      }
    }
  }
}
