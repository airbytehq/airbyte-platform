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
 * Inserts an organization_id column to the workspace table. The organization_id is a foreign key to
 * the id of the organization table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_5_002__AddOrganizationColumnToWorkspaceTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addOrganizationColumnToWorkspace(ctx)
  }

  companion object {
    @JvmStatic
    fun addOrganizationColumnToWorkspace(ctx: DSLContext) {
      val organizationId = DSL.field("organization_id", SQLDataType.UUID.nullable(true))

      ctx.alterTable("workspace").addColumnIfNotExists(organizationId).execute()
      ctx.alterTable("workspace").add(DSL.foreignKey(organizationId).references("organization")).execute()

      log.info { "organization_id column added to workspace table" }
    }
  }
}
