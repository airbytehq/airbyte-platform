/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.fasterxml.jackson.core.type.TypeReference
import com.google.common.collect.Iterators
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Notification
import io.airbyte.config.NotificationItem
import io.airbyte.config.NotificationSettings
import io.airbyte.db.instance.DatabaseConstants.WORKSPACE_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record2
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * backfills notification_settings column based on notification column in workspace table. We are
 * aiming to move logic to use notification_settings column only.
 */
@Suppress("ktlint:standard:class-naming")
class V0_44_5_004__BackFillNotificationSettingsColumn : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    backfillNotificationSettings(ctx)
  }

  companion object {
    private val ID_COLUMN = DSL.field("id", SQLDataType.UUID)
    private val NOTIFICATION_COLUMN = DSL.field("notifications", SQLDataType.JSONB)
    private val NOTIFICATION_SETTINGS_COLUMN = DSL.field("notification_settings", SQLDataType.JSONB)

    @JvmStatic
    fun backfillNotificationSettings(ctx: DSLContext) {
      val workspaceWithNotificationSettings: List<Record2<UUID, JSONB?>> =
        ctx
          .select(ID_COLUMN, NOTIFICATION_COLUMN)
          .from(WORKSPACE_TABLE)
          .stream()
          .toList()

      workspaceWithNotificationSettings.forEach { workspaceRecord: Record2<UUID, JSONB?> ->
        val workspaceId = workspaceRecord.getValue(ID_COLUMN)
        if (workspaceRecord.get<JSONB?>(NOTIFICATION_COLUMN) == null) {
          // This case does not exist in prod, but adding check to satisfy testing requirements.
          log.warn { "Workspace $workspaceId does not have notification column" }
          return@forEach
        }

        val originalNotificationList: List<Notification> =
          Jsons.deserialize(
            workspaceRecord.get(NOTIFICATION_COLUMN).data(),
            object : TypeReference<List<Notification>>() {},
          )
        val notificationSettings = NotificationSettings()
        if (originalNotificationList.isNotEmpty()) {
          val originalNotification = Iterators.getOnlyElement(originalNotificationList.listIterator())
          val notificationType = originalNotification.notificationType
          val slackConfiguration = originalNotification.slackConfiguration

          if (originalNotification.sendOnFailure) {
            notificationSettings
              .withSendOnFailure(
                NotificationItem()
                  .withNotificationType(listOf(notificationType))
                  .withSlackConfiguration(slackConfiguration),
              )
          }
          if (originalNotification.sendOnSuccess) {
            notificationSettings
              .withSendOnSuccess(
                NotificationItem()
                  .withNotificationType(listOf(notificationType))
                  .withSlackConfiguration(slackConfiguration),
              )
          }
        }
        // By default the following notification are all sent via emails. At this moment customers do not have an option to turn it off.
        notificationSettings.sendOnConnectionUpdateActionRequired =
          NotificationItem().withNotificationType(java.util.List.of(Notification.NotificationType.CUSTOMERIO))
        notificationSettings.sendOnConnectionUpdate =
          NotificationItem().withNotificationType(java.util.List.of(Notification.NotificationType.CUSTOMERIO))
        notificationSettings.sendOnSyncDisabled =
          NotificationItem().withNotificationType(java.util.List.of(Notification.NotificationType.CUSTOMERIO))
        notificationSettings.sendOnSyncDisabledWarning =
          NotificationItem().withNotificationType(java.util.List.of(Notification.NotificationType.CUSTOMERIO))
        ctx
          .update(DSL.table(WORKSPACE_TABLE))
          .set(NOTIFICATION_SETTINGS_COLUMN, JSONB.valueOf(Jsons.serialize(notificationSettings)))
          .where(ID_COLUMN.eq(workspaceId))
          .execute()
      }
    }
  }
}
