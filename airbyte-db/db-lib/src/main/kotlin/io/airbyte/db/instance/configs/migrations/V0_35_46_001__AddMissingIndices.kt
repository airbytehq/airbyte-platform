/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Add missing indices.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_46_001__AddMissingIndices : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    ctx.createIndexIfNotExists("actor_workspace_id_idx").on("actor", "workspace_id").execute()
    ctx
      .createIndexIfNotExists("connection_operation_connection_id_idx")
      .on("connection_operation", "connection_id")
      .execute()
  }
}
