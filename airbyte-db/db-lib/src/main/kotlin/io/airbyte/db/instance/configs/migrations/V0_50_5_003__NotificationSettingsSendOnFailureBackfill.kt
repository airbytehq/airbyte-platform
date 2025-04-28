/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.fasterxml.jackson.core.type.TypeReference
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Notification
import io.airbyte.config.NotificationSettings
import io.airbyte.db.instance.DatabaseConstants.WORKSPACE_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.Record2
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Set default value for sendOnFailure for notificationColumn under workspace table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_5_003__NotificationSettingsSendOnFailureBackfill : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx = DSL.using(context.connection)
    backfillNotificationSettings(ctx)
  }

  companion object {
    private val ID_COLUMN = DSL.field("id", SQLDataType.UUID)
    private val NOTIFICATION_SETTINGS_COLUMN = DSL.field("notification_settings", SQLDataType.JSONB)

    @JvmStatic
    fun backfillNotificationSettings(ctx: DSLContext) {
      val workspaceWithNotificationSettings: List<Record2<UUID, JSONB?>> =
        ctx
          .select(ID_COLUMN, NOTIFICATION_SETTINGS_COLUMN)
          .from(WORKSPACE_TABLE)
          .stream()
          .toList()

      workspaceWithNotificationSettings.forEach { workspaceRecord: Record2<UUID, JSONB?> ->
        val workspaceId = workspaceRecord.getValue(ID_COLUMN)
        if (workspaceRecord.get<JSONB?>(NOTIFICATION_SETTINGS_COLUMN) == null) {
          // This case does not exist in prod, but adding check to satisfy testing requirements.
          log.warn { "Workspace $workspaceId does not have notification column" }
          return@forEach
        }

        val notificationSetting =
          Jsons.deserialize<NotificationSettings>(
            workspaceRecord.get<JSONB>(NOTIFICATION_SETTINGS_COLUMN).data(),
            object : TypeReference<NotificationSettings>() {},
          ) ?: return@forEach

        // By default the following notification are all sent via emails. At this moment customers do not have an option to turn it off.
        val item = notificationSetting.sendOnFailure ?: return@forEach

        val existingType = item.notificationType ?: return@forEach

        if (existingType.contains(Notification.NotificationType.CUSTOMERIO)) {
          // They already have CUSTOMERIO enabled for sendOnFailure; skip operation.
          return@forEach
        }
        existingType.add(Notification.NotificationType.CUSTOMERIO)
        item.notificationType = existingType
        notificationSetting.sendOnFailure = item
        ctx
          .update<Record>(DSL.table(WORKSPACE_TABLE))
          .set<JSONB>(
            NOTIFICATION_SETTINGS_COLUMN,
            JSONB.valueOf(Jsons.serialize<NotificationSettings>(notificationSetting)),
          ).where(ID_COLUMN.eq(workspaceId))
          .execute()
      }
    }
  }
}
