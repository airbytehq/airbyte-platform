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
 * Add normalization integration type to actor definition migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_23_002__AddNormalizationIntegrationTypeToActorDefinition : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addIntegrationTypeColumn(ctx)
  }

  companion object {
    fun addIntegrationTypeColumn(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition")
        .addColumnIfNotExists(DSL.field("normalization_integration_type", SQLDataType.VARCHAR(255).nullable(true)))
        .execute()
    }
  }
}
