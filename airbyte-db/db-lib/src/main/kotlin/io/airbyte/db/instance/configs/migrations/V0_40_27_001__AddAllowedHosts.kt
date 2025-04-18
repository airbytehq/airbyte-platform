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
 * Add allowed hosts migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_27_001__AddAllowedHosts : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addAllowedHostsToActorDefinition(ctx)
  }

  companion object {
    private fun addAllowedHostsToActorDefinition(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition")
        .addColumnIfNotExists(DSL.field("allowed_hosts", SQLDataType.JSONB.nullable(true)))
        .execute()
    }
  }
}
