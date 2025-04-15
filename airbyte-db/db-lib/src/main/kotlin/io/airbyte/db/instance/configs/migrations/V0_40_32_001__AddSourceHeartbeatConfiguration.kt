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
 * Add override for what the heartbeat should be for a specific connector definition.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_32_001__AddSourceHeartbeatConfiguration : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addSourceHeartbeatConfiguration(ctx)
  }

  companion object {
    private fun addSourceHeartbeatConfiguration(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition")
        .addColumnIfNotExists(DSL.field("max_seconds_between_messages", SQLDataType.INTEGER.nullable(true)))
        .execute()

      ctx
        .commentOnColumn(DSL.name("actor_definition", "max_seconds_between_messages"))
        .`is`("Define the number of seconds allowed between 2 messages emitted by the connector before timing out")
        .execute()
    }
  }
}
