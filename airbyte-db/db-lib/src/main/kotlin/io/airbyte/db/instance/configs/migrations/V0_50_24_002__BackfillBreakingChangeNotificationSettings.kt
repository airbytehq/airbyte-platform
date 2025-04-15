/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.db.instance.DatabaseConstants.WORKSPACE_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Set default value for breaking change notifications for workspaces.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_24_002__BackfillBreakingChangeNotificationSettings : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    backfillNotificationSettings(ctx)
  }

  companion object {
    private val NOTIFICATION_SETTINGS_COLUMN = DSL.field("notification_settings", SQLDataType.JSONB)

    private const val BC_SYNC_DISABLED_NOTIFICATION_KEY = "sendOnBreakingChangeSyncsDisabled"
    private const val BC_WARNING_NOTIFICATION_KEY = "sendOnBreakingChangeWarning"
    private const val CUSTOMERIO_NOTIFICATION_ITEM_VALUE = "customerio"

    @JvmStatic
    @VisibleForTesting
    fun backfillNotificationSettings(ctx: DSLContext) {
      enableEmailNotificationsForUnsetSetting(ctx, BC_SYNC_DISABLED_NOTIFICATION_KEY)
      enableEmailNotificationsForUnsetSetting(ctx, BC_WARNING_NOTIFICATION_KEY)
    }

    private fun enableEmailNotificationsForUnsetSetting(
      ctx: DSLContext,
      notificationSettingKey: String,
    ) {
      val cioNotificationItem = Jsons.serialize(mapOf("notificationType" to listOf(CUSTOMERIO_NOTIFICATION_ITEM_VALUE)))

      ctx
        .update(DSL.table(WORKSPACE_TABLE))
        .set(
          NOTIFICATION_SETTINGS_COLUMN,
          DSL.field(
            "jsonb_set(?, ARRAY[?], ?::jsonb)",
            JSONB::class.java,
            NOTIFICATION_SETTINGS_COLUMN,
            notificationSettingKey,
            cioNotificationItem,
          ),
        ).where(
          DSL
            .field(
              "jsonb_typeof(?)",
              String::class.java,
              NOTIFICATION_SETTINGS_COLUMN,
            ).eq("object"),
        ).and(
          DSL.not(
            DSL.field(
              "? ?? ?",
              Boolean::class.java,
              NOTIFICATION_SETTINGS_COLUMN,
              notificationSettingKey,
            ),
          ),
        ).execute()
    }
  }
}
