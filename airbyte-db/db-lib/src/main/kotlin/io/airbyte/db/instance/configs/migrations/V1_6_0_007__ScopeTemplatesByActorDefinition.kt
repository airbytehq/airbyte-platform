/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_007__ScopeTemplatesByActorDefinition : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    updateConfigTemplateTable(ctx)
  }

  companion object {
    private const val TABLE_NAME = "config_template"

    private const val ACTOR_DEF_ID_COL = "actor_definition_id"
    private const val ACTOR_DEF_VERSION_ID_COL = "actor_definition_version_id"

    private const val FK_ACTOR_DEF_VERSION_CONSTRAINT = "config_template_actor_definition_version_id_fkey"
    private const val UNIQUE_ACTOR_DEF_VERSION_CONSTRAINT = "config_template_actor_definition_version_id_unique"

    @VisibleForTesting
    fun updateConfigTemplateTable(ctx: DSLContext) {
      log.info { "Updating $TABLE_NAME table" }

      ctx
        .alterTable(TABLE_NAME)
        .dropConstraint(UNIQUE_ACTOR_DEF_VERSION_CONSTRAINT)
        .execute()

      ctx
        .alterTable(TABLE_NAME)
        .dropConstraint(FK_ACTOR_DEF_VERSION_CONSTRAINT)
        .execute()

      ctx
        .alterTable(TABLE_NAME)
        .dropColumnIfExists(
          ACTOR_DEF_VERSION_ID_COL,
        ).execute()

      ctx
        .alterTable(TABLE_NAME)
        .alterColumn(ACTOR_DEF_ID_COL)
        .setNotNull()
        .execute()

      log.info { "Successfully updated $TABLE_NAME table" }
    }
  }
}
