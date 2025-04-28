/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.USER_INVITATION_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Replace the UserInvitation workspace_id and organization_id columns with a scope_type and
 * scope_id column. Note that this table is not yet written to, so this migration does not need to
 * handle any data migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_33_017__ReplaceUserInvitationWorkspaceAndOrganizationWithScope : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    dropWorkspaceIdColumn(ctx)
    dropOrganizationIdColumn(ctx)
    addScopeIdAndScopeTypeColumns(ctx)
    addScopeTypeAndScopeIdIndexes(ctx)
  }

  companion object {
    private const val SCOPE_TYPE = "scope_type"
    private const val SCOPE_ID = "scope_id"
    private val SCOPE_TYPE_COLUMN =
      DSL.field(
        SCOPE_TYPE,
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTable.ScopeTypeEnum::class.java,
          ).nullable(false),
      )
    private val SCOPE_ID_COLUMN = DSL.field(SCOPE_ID, SQLDataType.UUID.nullable(false))

    fun dropWorkspaceIdColumn(ctx: DSLContext) {
      ctx
        .alterTable(USER_INVITATION_TABLE)
        .dropColumn("workspace_id")
        .execute()
    }

    fun dropOrganizationIdColumn(ctx: DSLContext) {
      ctx
        .alterTable(USER_INVITATION_TABLE)
        .dropColumn("organization_id")
        .execute()
    }

    fun addScopeIdAndScopeTypeColumns(ctx: DSLContext) {
      ctx.alterTable(USER_INVITATION_TABLE).addColumnIfNotExists(SCOPE_ID_COLUMN).execute()
      ctx.alterTable(USER_INVITATION_TABLE).addColumnIfNotExists(SCOPE_TYPE_COLUMN).execute()
    }

    fun addScopeTypeAndScopeIdIndexes(ctx: DSLContext) {
      ctx
        .createIndex("user_invitation_scope_type_and_scope_id_index")
        .on(USER_INVITATION_TABLE, SCOPE_TYPE, SCOPE_ID)
        .execute()
      ctx
        .createIndex("user_invitation_scope_id_index")
        .on(USER_INVITATION_TABLE, SCOPE_ID)
        .execute()
    }
  }
}
