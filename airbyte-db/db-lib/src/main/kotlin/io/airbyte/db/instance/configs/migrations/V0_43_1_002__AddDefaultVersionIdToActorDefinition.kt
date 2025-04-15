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
 * Add default_version_id to actor_definition table, referencing actor_definition_version table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_43_1_002__AddDefaultVersionIdToActorDefinition : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addDefaultVersionIdToActorDefinition(ctx)
  }

  companion object {
    private fun addDefaultVersionIdToActorDefinition(ctx: DSLContext) {
      // This is nullable so that if we need to create a new actor_definition, we can do so before
      // subsequently creating the associated actor_definition_version and filling in the
      // default_version_id
      // field pointing to the new actor_definition_version.
      val defaultVersionId = DSL.field("default_version_id", SQLDataType.UUID.nullable(true))

      ctx
        .alterTable("actor_definition")
        .addIfNotExists(defaultVersionId)
        .execute()
      ctx
        .alterTable("actor_definition")
        .add(
          DSL
            .constraint("actor_definition_default_version_id_fkey")
            .foreignKey(defaultVersionId)
            .references("actor_definition_version", "id")
            .onDeleteRestrict(),
        ).execute()
    }
  }
}
