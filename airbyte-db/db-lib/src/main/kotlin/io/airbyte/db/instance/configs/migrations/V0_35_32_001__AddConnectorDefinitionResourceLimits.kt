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
 * Add connectors definition resource limits migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_32_001__AddConnectorDefinitionResourceLimits : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addResourceReqsToActorDefs(ctx)
  }

  companion object {
    fun addResourceReqsToActorDefs(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition")
        .addColumnIfNotExists(DSL.field("resource_requirements", SQLDataType.JSONB.nullable(true)))
        .execute()
    }
  }
}
