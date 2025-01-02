/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.WORKSPACE_TABLE;

import com.fasterxml.jackson.core.type.TypeReference;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.Notification.NotificationType;
import io.airbyte.config.NotificationItem;
import io.airbyte.config.NotificationSettings;
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
 * Set default value for sendOnFailure for notificationColumn under workspace table.
 */
public class V0_50_5_003__NotificationSettingsSendOnFailureBackfill extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_5_003__NotificationSettingsSendOnFailureBackfill.class);

  private static final Field<UUID> ID_COLUMN = DSL.field("id", SQLDataType.UUID);
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
    final var workspaceWithNotificationSettings = ctx.select(ID_COLUMN, NOTIFICATION_SETTINGS_COLUMN)
        .from(WORKSPACE_TABLE)
        .stream()
        .toList();

    workspaceWithNotificationSettings.forEach(workspaceRecord -> {
      final UUID workspaceId = workspaceRecord.getValue(ID_COLUMN);

      if (workspaceRecord.get(NOTIFICATION_SETTINGS_COLUMN) == null) {
        // This case does not exist in prod, but adding check to satisfy testing requirements.
        LOGGER.warn("Workspace {} does not have notification column", workspaceId);
        return;
      }

      final var notificationSetting =
          Jsons.deserialize(workspaceRecord.get(NOTIFICATION_SETTINGS_COLUMN).data(), new TypeReference<NotificationSettings>() {});
      if (notificationSetting == null) {
        return;
      }

      // By default the following notifactions are all sent via emails. At this moment customers do not
      // have an option to turn
      // it off.
      NotificationItem item = notificationSetting.getSendOnFailure();

      if (item == null) {
        return;
      }
      List<NotificationType> existingType = item.getNotificationType();

      if (existingType == null) {
        return;
      }
      if (existingType.contains(NotificationType.CUSTOMERIO)) {
        // They already have CUSTOMERIO enabled for sendOnFailure; skip operation.
        return;
      }
      existingType.add(NotificationType.CUSTOMERIO);
      item.setNotificationType(existingType);
      notificationSetting.setSendOnFailure(item);

      ctx.update(DSL.table(WORKSPACE_TABLE))
          .set(NOTIFICATION_SETTINGS_COLUMN, JSONB.valueOf(Jsons.serialize(notificationSetting)))
          .where(ID_COLUMN.eq(workspaceId))
          .execute();
    });
  }

}
