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
import java.util.UUID

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_55_1_004__EnforceOrgsEverywhere : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    putAllWorkspacesWithoutOrgIntoDefaultOrg(ctx)
    setOrganizationIdNotNull(ctx)
  }

  companion object {
    private const val WORKSPACE_TABLE = "workspace"
    private val DEFAULT_ORGANIZATION_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
    private val ORGANIZATION_ID_COLUMN = DSL.field("organization_id", SQLDataType.UUID)
    private val UPDATED_AT_COLUMN = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE)

    @JvmStatic
    fun putAllWorkspacesWithoutOrgIntoDefaultOrg(ctx: DSLContext) {
      ctx
        .update(DSL.table(WORKSPACE_TABLE))
        .set(ORGANIZATION_ID_COLUMN, DEFAULT_ORGANIZATION_ID)
        .set(UPDATED_AT_COLUMN, DSL.currentOffsetDateTime())
        .where(ORGANIZATION_ID_COLUMN.isNull())
        .execute()
    }

    @JvmStatic
    fun setOrganizationIdNotNull(ctx: DSLContext) {
      ctx
        .alterTable(WORKSPACE_TABLE)
        .alterColumn(ORGANIZATION_ID_COLUMN)
        .setNotNull()
        .execute()
    }
  }
}
