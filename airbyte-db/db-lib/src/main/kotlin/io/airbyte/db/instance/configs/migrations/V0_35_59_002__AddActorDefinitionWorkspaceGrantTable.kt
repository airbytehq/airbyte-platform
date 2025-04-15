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
 * Add actor definition workspace grant table migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_59_002__AddActorDefinitionWorkspaceGrantTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx = DSL.using(context.connection)
    createActorDefinitionWorkspaceGrant(ctx)
  }

  companion object {
    @JvmStatic
    fun createActorDefinitionWorkspaceGrant(ctx: DSLContext) {
      val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
      val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(false))

      ctx
        .createTableIfNotExists("actor_definition_workspace_grant")
        .columns(
          actorDefinitionId,
          workspaceId,
        ).constraints(
          DSL.unique(workspaceId, actorDefinitionId),
          DSL.foreignKey(actorDefinitionId).references("actor_definition", "id").onDeleteCascade(),
          DSL.foreignKey(workspaceId).references("workspace", "id").onDeleteCascade(),
        ).execute()

      log.info { "actor_definition_workspace_grant table created" }
    }
  }
}
