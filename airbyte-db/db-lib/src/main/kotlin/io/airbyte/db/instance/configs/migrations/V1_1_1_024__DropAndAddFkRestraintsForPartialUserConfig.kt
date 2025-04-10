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

@Suppress("ktlint:standard:class-naming")
class V1_1_1_024__DropAndAddFkRestraintsForPartialUserConfig : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    doMigration(ctx)
  }

  companion object {
    const val PARTIAL_USER_CONFIG_TABLE_NAME: String = "partial_user_config"
    const val PARTIAL_USER_CONFIG_WORKSPACE_ID_FK: String = "partial_user_config_workspace_id_fkey"
    const val WORKSPACE_ID_FIELD: String = "workspace_id"
    const val WORKSPACE_TABLE: String = "workspace"
    const val ID_FIELD: String = "id"

    fun doMigration(ctx: DSLContext) {
      ctx
        .alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .dropIfExists(DSL.constraint(PARTIAL_USER_CONFIG_WORKSPACE_ID_FK))
        .execute()

      ctx
        .alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .add(
          DSL
            .constraint(PARTIAL_USER_CONFIG_WORKSPACE_ID_FK)
            .foreignKey(WORKSPACE_ID_FIELD)
            .references(WORKSPACE_TABLE, ID_FIELD)
            .onDeleteCascade(),
        ).execute()
    }
  }
}
