/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.List;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V0_50_24_002__BackfillBreakingChangeNotificationSettingsTest extends AbstractConfigsDatabaseTest {

  private static final String WORKSPACE_TABLE = "workspace";
  private static final String NOTIFICATION_SETTINGS_COLUMN = "notification_settings";

  private static final NotificationItem EMPTY_NOTIFICATION_ITEM = new NotificationItem(List.of());
  private static final NotificationItem EMAIL_NOTIFICATION_ITEM = new NotificationItem(List.of(NotificationType.customerio));
  private static final NotificationItem SLACK_NOTIFICATION_ITEM = new NotificationItem(List.of(NotificationType.webhook));
  private static final NotificationItem EMAIL_AND_SLACK_NOTIFICATION_ITEM = new NotificationItem(List.of(
      NotificationType.customerio,
      NotificationType.webhook));

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_24_002__BackfillBreakingChangeNotificationSettingsTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_24_002__BackfillBreakingChangeNotificationSettings();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void testBackfillBreakingChangeNotificationSettings() {
    final DSLContext ctx = getDslContext();
    final UUID workspaceId = UUID.randomUUID();
    final UUID workspaceId2 = UUID.randomUUID();
    final UUID workspaceId3 = UUID.randomUUID();
    final UUID workspaceId4 = UUID.randomUUID();
    final UUID workspaceId5 = UUID.randomUUID();

    final NotificationSettings stdNotificationSettings = buildNotificationSettingsWithBCItem(null);
    final NotificationSettings settingsWithDisabledBreakingChangeConfig = buildNotificationSettingsWithBCItem(EMPTY_NOTIFICATION_ITEM);
    final NotificationSettings settingsWithBreakingChangeConfig = buildNotificationSettingsWithBCItem(EMAIL_NOTIFICATION_ITEM);

    ctx.insertInto(DSL.table(WORKSPACE_TABLE))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("slug"),
            DSL.field("initial_setup_complete"),
            DSL.field(NOTIFICATION_SETTINGS_COLUMN))
        .values(
            workspaceId,
            "name1",
            "wk1-slug",
            true,
            JSONB.valueOf(Jsons.serialize(stdNotificationSettings)))
        .values(
            workspaceId2,
            "name2",
            "wk2-slug",
            true,
            JSONB.valueOf(Jsons.serialize(settingsWithBreakingChangeConfig)))
        .values(
            workspaceId3,
            "name3",
            "wk3-slug",
            true,
            JSONB.valueOf(Jsons.serialize(settingsWithDisabledBreakingChangeConfig)))
        .values(
            workspaceId4,
            "name4",
            "wk4-slug",
            true,
            JSONB.valueOf("null"))
        .values(
            workspaceId5,
            "name5",
            "wk5-slug",
            true,
            null)
        .execute();

    V0_50_24_002__BackfillBreakingChangeNotificationSettings.backfillNotificationSettings(ctx);

    final String wkspc1Result = fetchNotificationSettingsData(ctx, workspaceId);
    assertEquals(settingsWithBreakingChangeConfig, Jsons.deserialize(wkspc1Result, NotificationSettings.class));

    final String wkspc2Result = fetchNotificationSettingsData(ctx, workspaceId2);
    assertEquals(settingsWithBreakingChangeConfig, Jsons.deserialize(wkspc2Result, NotificationSettings.class));

    final String wkspc3Result = fetchNotificationSettingsData(ctx, workspaceId3);
    assertEquals(settingsWithDisabledBreakingChangeConfig, Jsons.deserialize(wkspc3Result, NotificationSettings.class));

    final String wkspc4Result = fetchNotificationSettingsData(ctx, workspaceId4);
    assertNull(Jsons.deserialize(wkspc4Result, NotificationSettings.class));

    final String wkspc5Result = fetchNotificationSettingsData(ctx, workspaceId5);
    assertNull(wkspc5Result);

  }

  private static String fetchNotificationSettingsData(final DSLContext ctx, final UUID id) {
    final Record record = ctx.fetchOne(DSL.select(DSL.field(NOTIFICATION_SETTINGS_COLUMN, JSONB.class))
        .from(WORKSPACE_TABLE)
        .where(DSL.field("id").eq(id)));

    assert record != null;
    final JSONB notificationSettingsCol = record.get(NOTIFICATION_SETTINGS_COLUMN, JSONB.class);
    return notificationSettingsCol != null ? notificationSettingsCol.data() : null;
  }

  private static NotificationSettings buildNotificationSettingsWithBCItem(final NotificationItem breakingChangeNotificationItem) {
    return new NotificationSettings(
        EMAIL_NOTIFICATION_ITEM,
        EMAIL_AND_SLACK_NOTIFICATION_ITEM,
        SLACK_NOTIFICATION_ITEM,
        EMAIL_NOTIFICATION_ITEM,
        breakingChangeNotificationItem,
        breakingChangeNotificationItem);
  }

  /**
   * Below this line: Local versions of generated config classes to avoid breaking tests if the
   * generated classes change.
   */

  private enum NotificationType {
    customerio,
    webhook
  }

  private record NotificationItem(List<NotificationType> notificationType) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private record NotificationSettings(
                                      NotificationItem sendOnSyncDisabled,
                                      NotificationItem sendOnConnectionUpdate,
                                      NotificationItem sendOnSyncDisabledWarning,
                                      NotificationItem sendOnConnectionUpdateActionRequired,
                                      NotificationItem sendOnBreakingChangeWarning,
                                      NotificationItem sendOnBreakingChangeSyncsDisabled) {}

}
