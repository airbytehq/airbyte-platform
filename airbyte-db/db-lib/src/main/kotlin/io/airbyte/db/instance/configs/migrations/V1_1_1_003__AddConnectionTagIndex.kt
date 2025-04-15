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
class V1_1_1_003__AddConnectionTagIndex : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    migrate(ctx)
  }

  companion object {
    private const val CONNECTION_TAG_TABLE = "connection_tag"

    fun migrate(ctx: DSLContext) {
      ctx
        .createIndexIfNotExists("connection_tag_connection_id_idx")
        .on(CONNECTION_TAG_TABLE, "connection_id")
        .execute()
    }
  }
}
