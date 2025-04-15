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
 * Inserts an default_version_id column to the actor table. The default_version_id is a foreign key
 * to the id of the actor_definition_version table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_6_002__AddDefaultVersionIdToActor : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addDefaultVersionIdColumnToActor(ctx)
  }

  companion object {
    @JvmStatic
    fun addDefaultVersionIdColumnToActor(ctx: DSLContext) {
      // TODO (connector-ops) Remove nullable
      val defaultVersionId = DSL.field("default_version_id", SQLDataType.UUID.nullable(true))

      ctx.alterTable("actor").addColumnIfNotExists(defaultVersionId).execute()
      ctx
        .alterTable("actor")
        .add(DSL.foreignKey(defaultVersionId).references("actor_definition_version"))
        .execute()

      log.info { "default_version_id column added to actor table" }
    }
  }
}
