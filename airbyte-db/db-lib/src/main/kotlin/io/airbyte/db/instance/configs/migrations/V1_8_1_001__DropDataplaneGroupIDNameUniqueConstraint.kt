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
class V1_8_1_001__DropDataplaneGroupIDNameUniqueConstraint : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    dropDataplaneGroupIdNameUniqueConstraint(ctx)
  }

  companion object {
    private const val DATAPLANE_TABLE_NAME = "dataplane"
    private const val DATAPLANE_GROUP_ID_NAME_KEY = "dataplane_dataplane_group_id_name_key"

    fun dropDataplaneGroupIdNameUniqueConstraint(ctx: DSLContext) {
      ctx.execute(
        """
        ALTER TABLE $DATAPLANE_TABLE_NAME DROP CONSTRAINT IF EXISTS $DATAPLANE_GROUP_ID_NAME_KEY
        """.trimIndent(),
      )
    }
  }
}
