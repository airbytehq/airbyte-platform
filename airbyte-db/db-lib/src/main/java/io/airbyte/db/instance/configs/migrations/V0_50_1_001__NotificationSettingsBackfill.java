/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.WORKSPACE_TABLE;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Iterators;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.Notification;
import io.airbyte.config.Notification.NotificationType;
import io.airbyte.config.NotificationItem;
import io.airbyte.config.NotificationSettings;
import io.airbyte.config.SlackNotificationConfiguration;
import java.util.List;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Set default value for sendOnSuccess/sendOnFailure for notificationColumn under workspace table.
 * Backfill for null values for notificationColumn under workspace caused by a bug.
 */
public class V0_50_1_001__NotificationSettingsBackfill extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_1_001__NotificationSettingsBackfill.class);

  private static final Field<UUID> ID_COLUMN = DSL.field("id", SQLDataType.UUID);
  private static final Field<JSONB> NOTIFICATION_COLUMN = DSL.field("notifications", SQLDataType.JSONB);
  private static final Field<JSONB> NOTIFICATION_SETTINGS_COLUMN = DSL.field("notification_settings", SQLDataType.JSONB);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    backfillNotificationSettings(ctx);
  }

  static void backfillNotificationSettings(final DSLContext ctx) {
    final var workspaceWithNotificationSettings = ctx.select(ID_COLUMN, NOTIFICATION_COLUMN)
        .from(WORKSPACE_TABLE)
        .stream()
        .toList();

    workspaceWithNotificationSettings.forEach(workspaceRecord -> {
      final UUID workspaceId = workspaceRecord.getValue(ID_COLUMN);
      if (workspaceRecord.get(NOTIFICATION_COLUMN) == null) {
        // This case does not exist in prod, but adding check to satisfy testing requirements.
        LOGGER.warn("Workspace {} does not have notification column", workspaceId);
        return;
      }
      final var originalNotificationList =
          Jsons.deserialize(workspaceRecord.get(NOTIFICATION_COLUMN).data(), new TypeReference<List<Notification>>() {});
      final var notificationSettings = new NotificationSettings();
      // By default the following notifactions are all sent via emails. At this moment customers do not
      // have an option to turn
      // it off.
      notificationSettings.setSendOnConnectionUpdateActionRequired(new NotificationItem().withNotificationType(List.of(NotificationType.CUSTOMERIO)));
      notificationSettings.setSendOnConnectionUpdate(new NotificationItem().withNotificationType(List.of(NotificationType.CUSTOMERIO)));
      notificationSettings.setSendOnSyncDisabled(new NotificationItem().withNotificationType(List.of(NotificationType.CUSTOMERIO)));
      notificationSettings.setSendOnSyncDisabledWarning(new NotificationItem().withNotificationType(List.of(NotificationType.CUSTOMERIO)));

      // By default we do not send sendOnSuccess or sendOnFailure notifications.
      notificationSettings.setSendOnSuccess(new NotificationItem().withNotificationType(List.of()));
      notificationSettings.setSendOnFailure(new NotificationItem().withNotificationType(List.of()));

      if (!originalNotificationList.isEmpty()) {
        final var originalNotification = Iterators.getOnlyElement(originalNotificationList.listIterator());
        final NotificationType notificationType = originalNotification.getNotificationType();
        final SlackNotificationConfiguration slackConfiguration = originalNotification.getSlackConfiguration();

        if (originalNotification.getSendOnFailure()) {
          notificationSettings
              .withSendOnFailure(new NotificationItem().withNotificationType(List.of(notificationType)).withSlackConfiguration(slackConfiguration));
        }
        if (originalNotification.getSendOnSuccess()) {
          notificationSettings
              .withSendOnSuccess(new NotificationItem().withNotificationType(List.of(notificationType)).withSlackConfiguration(slackConfiguration));
        }
      }

      ctx.update(DSL.table(WORKSPACE_TABLE))
          .set(NOTIFICATION_SETTINGS_COLUMN, JSONB.valueOf(Jsons.serialize(notificationSettings)))
          .where(ID_COLUMN.eq(workspaceId))
          .execute();
    });
  }

}
