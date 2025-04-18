/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.config.ScopeType
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Record2
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * This migration adds scope_id and scope_type columns to the actor_definition_workspace_grant
 * table. The scope_type is an enum of either organization or workspace. The scope_id refers to the
 * id of the scope e.g. workspace_id or organization_id.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx = DSL.using(context.connection)
    addScopeTypeEnum(ctx)
    addScopeColumn(ctx)
    alterTable(ctx)
    migrateExistingRows(ctx)
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  internal enum class ScopeTypeEnum(
    private val literal: String,
  ) : EnumType {
    workspace(ScopeType.WORKSPACE.value()),
    organization(ScopeType.ORGANIZATION.value()),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = SCOPE_TYPE

    override fun getLiteral(): String = literal
  }

  companion object {
    private const val ACTOR_DEFINITION_WORKSPACE_GRANT = "actor_definition_workspace_grant"
    private const val SCOPE_TYPE = "scope_type"
    private val NEW_SCOPE_TYPE_COLUMN =
      DSL.field(
        SCOPE_TYPE,
        SQLDataType.VARCHAR
          .asEnumDataType(
            ScopeTypeEnum::class.java,
          ).nullable(false)
          .defaultValue(ScopeTypeEnum.workspace),
      )

    private val WORKSPACE_ID_COLUMN = DSL.field("workspace_id", SQLDataType.UUID)
    private val ACTOR_DEFINITION_ID_COLUMN = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
    private val SCOPE_ID_COLUMN = DSL.field("scope_id", SQLDataType.UUID.nullable(true))

    fun addScopeTypeEnum(ctx: DSLContext) {
      ctx.dropTypeIfExists(SCOPE_TYPE).execute()
      ctx.createType(SCOPE_TYPE).asEnum(ScopeType.WORKSPACE.value(), ScopeType.ORGANIZATION.value()).execute()
    }

    fun addScopeColumn(ctx: DSLContext) {
      ctx.alterTable(ACTOR_DEFINITION_WORKSPACE_GRANT).addColumnIfNotExists(SCOPE_ID_COLUMN).execute()
      ctx.alterTable(ACTOR_DEFINITION_WORKSPACE_GRANT).addColumnIfNotExists(NEW_SCOPE_TYPE_COLUMN).execute()

      log.info { "scope_id and scope_type columns added to actor_definition_workspace_grant table" }
    }

    /**
     * Add a foreign key constraint to the scope_id column and edit the primary key to be the scope_id,
     * actor_definition_id, and scope_type.
     */
    fun alterTable(ctx: DSLContext) {
      // make workspace_id column nullable
      ctx
        .alterTable(ACTOR_DEFINITION_WORKSPACE_GRANT)
        .alterColumn(WORKSPACE_ID_COLUMN)
        .dropNotNull()
        .execute()

      // drop workspace foreign key
      ctx
        .alterTable(ACTOR_DEFINITION_WORKSPACE_GRANT)
        .dropConstraintIfExists("actor_definition_workspace_grant_workspace_id_fkey")
        .execute()

      // drop unique constraint
      ctx
        .alterTable(ACTOR_DEFINITION_WORKSPACE_GRANT)
        .dropConstraintIfExists("actor_definition_workspace_gr_workspace_id_actor_definition_key")
        .execute()

      // re-add unique constraint
      ctx
        .alterTable(ACTOR_DEFINITION_WORKSPACE_GRANT)
        .add(DSL.unique(ACTOR_DEFINITION_ID_COLUMN, SCOPE_ID_COLUMN, NEW_SCOPE_TYPE_COLUMN))
        .execute()

      log.info { "actor_definition_workspace_grant table altered" }
    }

    /**
     * Migrate the existing table so that all the current rows have the scope_id and scope_type set to
     * the workspace_id and workspace respectively.
     */
    private fun migrateExistingRows(ctx: DSLContext) {
      val actorDefinitionIdToWorkspaceIdList: MutableList<List<UUID>> = ArrayList()
      ctx
        .select(ACTOR_DEFINITION_ID_COLUMN, WORKSPACE_ID_COLUMN)
        .from(ACTOR_DEFINITION_WORKSPACE_GRANT)
        .stream()
        .forEach { record: Record2<UUID, UUID> ->
          actorDefinitionIdToWorkspaceIdList.add(
            java.util.List.of(
              record.getValue(ACTOR_DEFINITION_ID_COLUMN),
              record.getValue(WORKSPACE_ID_COLUMN),
            ),
          )
        }

      for (actorDefinitionWorkspaceIdPair in actorDefinitionIdToWorkspaceIdList) {
        val actorDefinitionId = actorDefinitionWorkspaceIdPair[0]
        val workspaceId = actorDefinitionWorkspaceIdPair[1]
        ctx
          .update(DSL.table(ACTOR_DEFINITION_WORKSPACE_GRANT))
          .set(SCOPE_ID_COLUMN, workspaceId)
          .set(NEW_SCOPE_TYPE_COLUMN, ScopeTypeEnum.workspace)
          .where(ACTOR_DEFINITION_ID_COLUMN.eq(actorDefinitionId).and(WORKSPACE_ID_COLUMN.eq(workspaceId)))
          .execute()
      }

      log.info { "Existing rows migrated" }
    }
  }
}
