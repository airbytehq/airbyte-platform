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
class V1_6_0_018__DropDataplaneGroupIdNotNullConstraintFromConnection : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    dropDataplaneGroupIdFromConnection(ctx)
  }

  companion object {
    private const val CONNECTION_TABLE_NAME: String = "connection"
    private const val DATAPLANE_GROUP_ID_COLUMN_NAME: String = "dataplane_group_id"

    fun dropDataplaneGroupIdFromConnection(ctx: DSLContext) {
      ctx.execute(
        """
        ALTER TABLE $CONNECTION_TABLE_NAME
        ALTER COLUMN $DATAPLANE_GROUP_ID_COLUMN_NAME DROP NOT NULL
        """.trimIndent(),
      )
    }
  }
}
