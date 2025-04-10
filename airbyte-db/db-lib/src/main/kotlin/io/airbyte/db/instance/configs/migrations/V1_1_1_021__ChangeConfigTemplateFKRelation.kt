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
class V1_1_1_021__ChangeConfigTemplateFKRelation : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    doMigration(ctx)
  }

  companion object {
    const val CONFIG_TEMPLATE_TABLE_NAME: String = "config_template"
    const val CONFIG_TEMPLATE_ACTOR_FK: String = "config_template_actor_definition_id_fkey"
    const val ACTOR_DEFINITION_ID_FIELD: String = "actor_definition_id"
    const val ACTOR_DEFINITION_TABLE: String = "actor_definition"
    const val ID_FIELD: String = "id"

    fun doMigration(ctx: DSLContext) {
      ctx
        .alterTable(CONFIG_TEMPLATE_TABLE_NAME)
        .dropIfExists(DSL.constraint(CONFIG_TEMPLATE_ACTOR_FK))
        .execute()

      ctx
        .alterTable(CONFIG_TEMPLATE_TABLE_NAME)
        .add(
          DSL
            .constraint(CONFIG_TEMPLATE_ACTOR_FK)
            .foreignKey(ACTOR_DEFINITION_ID_FIELD)
            .references(ACTOR_DEFINITION_TABLE, ID_FIELD)
            .onDeleteCascade(),
        ).execute()
    }
  }
}
