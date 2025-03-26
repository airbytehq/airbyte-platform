/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.Notification.NotificationType;
import io.airbyte.config.NotificationItem;
import io.airbyte.config.NotificationSettings;
import io.airbyte.config.SlackNotificationConfiguration;
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
import org.skyscreamer.jsonassert.JSONAssert;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class V0_50_1_001__NotificationSettingsBackfillTest extends AbstractConfigsDatabaseTest {

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V0_50_1_001__NotificationSettingsBackfillTest", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);
    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);

    final BaseJavaMigration previousMigration = new V0_50_1_001__NotificationSettingsBackfill();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();
  }

  @Test
  void testMigrateEmptyValues() throws Exception {
    final DSLContext ctx = getDslContext();

    // Insert data to workspace

    final UUID workspaceId = UUID.randomUUID();

    ctx.insertInto(DSL.table("workspace"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("slug"),
            DSL.field("initial_setup_complete"),
            DSL.field("notifications"))
        .values(
            workspaceId,
            "name1",
            "default",
            true,
            JSONB.valueOf("[]"))
        .execute();

    V0_50_1_001__NotificationSettingsBackfill.backfillNotificationSettings(ctx);

    final String result = fetchNotificationSettingsData(ctx, workspaceId);

    final NotificationSettings expectedNotification = new NotificationSettings()
        .withSendOnSuccess(new NotificationItem()
            .withNotificationType(List.of()))
        .withSendOnFailure(new NotificationItem()
            .withNotificationType(List.of()))
        .withSendOnConnectionUpdate(new NotificationItem()
            .withNotificationType(List.of(NotificationType.CUSTOMERIO)))
        .withSendOnConnectionUpdateActionRequired(new NotificationItem()
            .withNotificationType(List.of(NotificationType.CUSTOMERIO)))
        .withSendOnSyncDisabled(new NotificationItem()
            .withNotificationType(List.of(NotificationType.CUSTOMERIO)))
        .withSendOnSyncDisabledWarning(new NotificationItem()
            .withNotificationType(List.of(NotificationType.CUSTOMERIO)));

    final String expectedNotificationJson = Jsons.serialize(expectedNotification);

    JSONAssert.assertEquals(expectedNotificationJson, result, /* strict= */ true);
  }

  @Test
  void testMigrateSlackConfigs() throws Exception {
    final DSLContext ctx = getDslContext();

    // Insert data to workspace
    final UUID workspaceId = UUID.randomUUID();

    ctx.insertInto(DSL.table("workspace"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("slug"),
            DSL.field("initial_setup_complete"),
            DSL.field("notifications"))
        .values(
            workspaceId,
            "name1",
            "default",
            true,
            JSONB.valueOf(
                "[{\"sendOnFailure\": true, \"sendOnSuccess\": false, \"notificationType\": \"slack\", \"slackConfiguration\": {\"webhook\": \"https://hooks.slack.com/testtesttest\"}}]"))
        .execute();

    V0_50_1_001__NotificationSettingsBackfill.backfillNotificationSettings(ctx);

    final String result = fetchNotificationSettingsData(ctx, workspaceId);
    final NotificationSettings expectedNotification = new NotificationSettings()
        .withSendOnSuccess(new NotificationItem()
            .withNotificationType(List.of()))
        .withSendOnFailure(new NotificationItem()
            .withNotificationType(List.of(NotificationType.SLACK))
            .withSlackConfiguration(new SlackNotificationConfiguration().withWebhook("https://hooks.slack.com/testtesttest")))
        .withSendOnConnectionUpdate(new NotificationItem()
            .withNotificationType(List.of(NotificationType.CUSTOMERIO)))
        .withSendOnConnectionUpdateActionRequired(new NotificationItem()
            .withNotificationType(List.of(NotificationType.CUSTOMERIO)))
        .withSendOnSyncDisabled(new NotificationItem()
            .withNotificationType(List.of(NotificationType.CUSTOMERIO)))
        .withSendOnSyncDisabledWarning(new NotificationItem()
            .withNotificationType(List.of(NotificationType.CUSTOMERIO)));

    final String expectedNotificationJson = Jsons.serialize(expectedNotification);

    JSONAssert.assertEquals(expectedNotificationJson, result, /* strict= */ true);
  }

  protected static String fetchNotificationSettingsData(final DSLContext ctx, final UUID id) {
    final Record record = ctx.fetchOne(DSL.select(DSL.field("notification_settings", JSONB.class))
        .from("workspace")
        .where(DSL.field("id").eq(id)));

    return record.get("notification_settings", JSONB.class).data();
  }

}
