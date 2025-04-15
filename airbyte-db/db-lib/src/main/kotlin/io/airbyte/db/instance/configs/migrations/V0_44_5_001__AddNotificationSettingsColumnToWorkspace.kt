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
 * Adds notification_settings column to workspace table. This intends to replace notification column
 * in the near future.
 */
@Suppress("ktlint:standard:class-naming")
class V0_44_5_001__AddNotificationSettingsColumnToWorkspace : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addNotificationSettingsColumnToWorkspace(ctx)
  }

  companion object {
    private fun addNotificationSettingsColumnToWorkspace(ctx: DSLContext) {
      val notificationSettings = DSL.field("notification_settings", SQLDataType.JSONB.nullable(true))

      ctx
        .alterTable("workspace")
        .addIfNotExists(notificationSettings)
        .execute()
    }
  }
}
