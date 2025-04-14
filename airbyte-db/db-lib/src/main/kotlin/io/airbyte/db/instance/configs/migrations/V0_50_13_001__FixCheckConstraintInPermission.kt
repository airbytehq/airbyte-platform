/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * This new migration is to fix a check constraint in a previous migration:
 * V0_50_11_002__AddOrganizationIdColumnToPermission.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_13_001__FixCheckConstraintInPermission : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    log.info { "Fix check constraint in Permission table..." }
    fixCheckConstraint(ctx)
    log.info { "Migration finished!" }
  }

  companion object {
    private const val PERMISSION_TABLE = "permission"
    private const val ORGANIZATION_ID_COLUMN = "organization_id"
    private const val WORKSPACE_ID_COLUMN = "workspace_id"
    private const val CHECK_CONSTRAINT_NAME = "permission_check_access_type"

    private fun fixCheckConstraint(ctx: DSLContext) {
      ctx
        .alterTable(PERMISSION_TABLE)
        .dropConstraintIfExists(CHECK_CONSTRAINT_NAME)
        .execute()
      ctx
        .alterTable(PERMISSION_TABLE)
        .add(
          DSL.constraint(CHECK_CONSTRAINT_NAME).check(
            DSL.field(WORKSPACE_ID_COLUMN).isNull().or(DSL.field(ORGANIZATION_ID_COLUMN).isNull()),
          ),
        ).execute()
    }
  }
}
