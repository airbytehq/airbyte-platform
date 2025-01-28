/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.WORKSPACE_TABLE;
import static org.jooq.impl.DSL.not;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import java.util.List;
import java.util.Map;
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
 * Set default value for breaking change notifications for workspaces.
 */
public class V0_50_24_002__BackfillBreakingChangeNotificationSettings extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_24_002__BackfillBreakingChangeNotificationSettings.class);

  private static final Field<JSONB> NOTIFICATION_SETTINGS_COLUMN = DSL.field("notification_settings", SQLDataType.JSONB);

  private static final String BC_SYNC_DISABLED_NOTIFICATION_KEY = "sendOnBreakingChangeSyncsDisabled";
  private static final String BC_WARNING_NOTIFICATION_KEY = "sendOnBreakingChangeWarning";
  private static final String CUSTOMERIO_NOTIFICATION_ITEM_VALUE = "customerio";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    backfillNotificationSettings(ctx);
  }

  @VisibleForTesting
  static void backfillNotificationSettings(final DSLContext ctx) {
    enableEmailNotificationsForUnsetSetting(ctx, BC_SYNC_DISABLED_NOTIFICATION_KEY);
    enableEmailNotificationsForUnsetSetting(ctx, BC_WARNING_NOTIFICATION_KEY);
  }

  private static void enableEmailNotificationsForUnsetSetting(final DSLContext ctx, final String notificationSettingKey) {
    final var cioNotificationItem = Jsons.serialize(Map.of("notificationType", List.of(CUSTOMERIO_NOTIFICATION_ITEM_VALUE)));
    ctx.update(DSL.table(WORKSPACE_TABLE))
        .set(NOTIFICATION_SETTINGS_COLUMN,
            DSL.field("jsonb_set(?, ARRAY[?], ?::jsonb)", JSONB.class, NOTIFICATION_SETTINGS_COLUMN, notificationSettingKey, cioNotificationItem))
        .where(DSL.field("jsonb_typeof(?)", String.class, NOTIFICATION_SETTINGS_COLUMN).eq("object"))
        .and(not(DSL.field("? ?? ?", Boolean.class, NOTIFICATION_SETTINGS_COLUMN, notificationSettingKey)))
        .execute();
  }

}
