/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Add resource_type column to Permission table migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_11_002__AddOrganizationIdColumnToPermission : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    log.info { "Add column organization_id to Permission table..." }
    addOrganizationIdColumnToPermission(ctx)
    log.info { "Migration finished!" }
  }

  companion object {
    private const val PERMISSION_TABLE = "permission"
    private const val ORGANIZATION_TABLE = "organization"
    private const val ORGANIZATION_ID_COLUMN = "organization_id"
    private const val WORKSPACE_ID_COLUMN = "organization_id"
    private const val ORGANIZATION_ID_FOREIGN_KEY = "permission_organization_id_fkey"
    private const val ORGANIZATION_ID_INDEX = "permission_organization_id_idx"

    private fun addOrganizationIdColumnToPermission(ctx: DSLContext) {
      val organizationId = DSL.field(ORGANIZATION_ID_COLUMN, SQLDataType.UUID.nullable(true))
      // 1. Add new column.
      ctx
        .alterTable(PERMISSION_TABLE)
        .addColumnIfNotExists(organizationId)
        .execute()
      // 2.1 Add foreign key constraint.
      ctx
        .alterTable(PERMISSION_TABLE)
        .add(
          DSL
            .constraint(ORGANIZATION_ID_FOREIGN_KEY)
            .foreignKey(organizationId)
            .references(ORGANIZATION_TABLE, "id")
            .onDeleteCascade(),
        ).execute()
      // 2.2 Add constraint check on access type: workspace OR organization.
      ctx
        .alterTable(PERMISSION_TABLE)
        .add(
          DSL.constraint("permission_check_access_type").check(
            DSL.field(WORKSPACE_ID_COLUMN).isNull().or(DSL.field(ORGANIZATION_ID_COLUMN).isNull()),
          ),
        ).execute()
      // 3. Add new index.
      ctx
        .createIndexIfNotExists(ORGANIZATION_ID_INDEX)
        .on(PERMISSION_TABLE, ORGANIZATION_ID_COLUMN)
        .execute()
    }
  }
}
