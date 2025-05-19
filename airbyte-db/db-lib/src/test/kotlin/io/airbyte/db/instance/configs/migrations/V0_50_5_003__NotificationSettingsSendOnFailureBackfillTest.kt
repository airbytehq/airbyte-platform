/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.commons.json.Jsons
import io.airbyte.config.Notification
import io.airbyte.config.NotificationItem
import io.airbyte.config.NotificationSettings
import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.impl.DSL
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.skyscreamer.jsonassert.JSONAssert
import java.util.UUID
import kotlin.collections.listOf

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_5_003__NotificationSettingsSendOnFailureBackfillTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_5_002__NotificationSettingsSendOnFailureBackfillTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_50_1_001__NotificationSettingsBackfill()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  @Throws(Exception::class)
  fun testBackfillCustomerIoValues() {
    val ctx = getDslContext()

    // Insert data to workspace
    val workspaceId = UUID.randomUUID()

    ctx
      .insertInto(DSL.table("workspace"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("slug"),
        DSL.field("initial_setup_complete"),
        DSL.field(NOTIFICATION_SETTINGS_COLUMN),
      ).values(
        workspaceId,
        "name1",
        "default",
        true,
        JSONB.valueOf(
          (
            "{\"sendOnFailure\": {\"notificationType\": []}, " +
              "\"sendOnSuccess\": {\"notificationType\": []}, " +
              "\"sendOnSyncDisabled\": {\"notificationType\": [\"customerio\"]}, " +
              "\"sendOnConnectionUpdate\": {\"notificationType\": [\"customerio\"]}, " +
              "\"sendOnSyncDisabledWarning\": {\"notificationType\": [\"customerio\"]}, " +
              "\"sendOnConnectionUpdateActionRequired\": {\"notificationType\": [\"customerio\"]}}"
          ),
        ),
      ).execute()

    V0_50_5_003__NotificationSettingsSendOnFailureBackfill.backfillNotificationSettings(ctx)

    val result = fetchNotificationSettingsData(ctx, workspaceId)

    val expectedNotification =
      NotificationSettings()
        .withSendOnSuccess(
          NotificationItem()
            .withNotificationType(listOf()),
        ).withSendOnFailure(
          NotificationItem()
            .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
        ).withSendOnConnectionUpdate(
          NotificationItem()
            .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
        ).withSendOnConnectionUpdateActionRequired(
          NotificationItem()
            .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
        ).withSendOnSyncDisabled(
          NotificationItem()
            .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
        ).withSendOnSyncDisabledWarning(
          NotificationItem()
            .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
        )

    val expectedNotificationJson = Jsons.serialize(expectedNotification)

    JSONAssert.assertEquals(expectedNotificationJson, result, true)
  }

  @Test
  @Throws(Exception::class)
  fun testKeepCustomerIoValues() {
    val ctx = getDslContext()

    // Insert data to workspace
    val workspaceId = UUID.randomUUID()

    ctx
      .insertInto(DSL.table("workspace"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("slug"),
        DSL.field("initial_setup_complete"),
        DSL.field(NOTIFICATION_SETTINGS_COLUMN),
      ).values(
        workspaceId,
        "name1",
        "default",
        true,
        JSONB.valueOf(
          (
            "{\"sendOnFailure\": {\"notificationType\": [\"customerio\"]}, " +
              "\"sendOnSuccess\": {\"notificationType\": []}, " +
              "\"sendOnSyncDisabled\": {\"notificationType\": [\"customerio\"]}, " +
              "\"sendOnConnectionUpdate\": {\"notificationType\": [\"customerio\"]}, " +
              "\"sendOnSyncDisabledWarning\": {\"notificationType\": [\"customerio\"]}, " +
              "\"sendOnConnectionUpdateActionRequired\": {\"notificationType\": [\"customerio\"]}}"
          ),
        ),
      ).execute()

    V0_50_5_003__NotificationSettingsSendOnFailureBackfill.backfillNotificationSettings(ctx)

    val result = fetchNotificationSettingsData(ctx, workspaceId)
    val expectedNotification =
      NotificationSettings()
        .withSendOnSuccess(
          NotificationItem()
            .withNotificationType(listOf()),
        ).withSendOnFailure(
          NotificationItem()
            .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
        ).withSendOnConnectionUpdate(
          NotificationItem()
            .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
        ).withSendOnConnectionUpdateActionRequired(
          NotificationItem()
            .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
        ).withSendOnSyncDisabled(
          NotificationItem()
            .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
        ).withSendOnSyncDisabledWarning(
          NotificationItem()
            .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
        )

    val expectedNotificationJson = Jsons.serialize(expectedNotification)

    JSONAssert.assertEquals(expectedNotificationJson, result, true)
  }

  companion object {
    private const val NOTIFICATION_SETTINGS_COLUMN = "notification_settings"

    protected fun fetchNotificationSettingsData(
      ctx: DSLContext,
      id: UUID?,
    ): String {
      val record: Record? =
        ctx.fetchOne(
          DSL
            .select(
              DSL.field(
                NOTIFICATION_SETTINGS_COLUMN,
                JSONB::class.java,
              ),
            ).from("workspace")
            .where(DSL.field("id").eq(id)),
        )

      return record!!
        .get(
          NOTIFICATION_SETTINGS_COLUMN,
          JSONB::class.java,
        ).data()
    }
  }
}
