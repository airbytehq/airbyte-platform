/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.fasterxml.jackson.annotation.JsonInclude
import io.airbyte.commons.json.Jsons
import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_24_002__BackfillBreakingChangeNotificationSettingsTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_24_002__BackfillBreakingChangeNotificationSettingsTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V0_50_24_002__BackfillBreakingChangeNotificationSettings()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun testBackfillBreakingChangeNotificationSettings() {
    val ctx = dslContext!!
    val workspaceId = UUID.randomUUID()
    val workspaceId2 = UUID.randomUUID()
    val workspaceId3 = UUID.randomUUID()
    val workspaceId4 = UUID.randomUUID()
    val workspaceId5 = UUID.randomUUID()

    val stdNotificationSettings = buildNotificationSettingsWithBCItem(null)
    val settingsWithDisabledBreakingChangeConfig = buildNotificationSettingsWithBCItem(EMPTY_NOTIFICATION_ITEM)
    val settingsWithBreakingChangeConfig = buildNotificationSettingsWithBCItem(EMAIL_NOTIFICATION_ITEM)

    ctx
      .insertInto(DSL.table(WORKSPACE_TABLE))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("slug"),
        DSL.field("initial_setup_complete"),
        DSL.field(NOTIFICATION_SETTINGS_COLUMN),
      ).values(
        workspaceId,
        "name1",
        "wk1-slug",
        true,
        JSONB.valueOf(Jsons.serialize(stdNotificationSettings)),
      ).values(
        workspaceId2,
        "name2",
        "wk2-slug",
        true,
        JSONB.valueOf(Jsons.serialize(settingsWithBreakingChangeConfig)),
      ).values(
        workspaceId3,
        "name3",
        "wk3-slug",
        true,
        JSONB.valueOf(Jsons.serialize(settingsWithDisabledBreakingChangeConfig)),
      ).values(
        workspaceId4,
        "name4",
        "wk4-slug",
        true,
        JSONB.valueOf("null"),
      ).values(
        workspaceId5,
        "name5",
        "wk5-slug",
        true,
        null,
      ).execute()

    V0_50_24_002__BackfillBreakingChangeNotificationSettings.backfillNotificationSettings(ctx)

    val wkspc1Result = fetchNotificationSettingsData(ctx, workspaceId)
    Assertions.assertEquals(
      settingsWithBreakingChangeConfig,
      Jsons.deserialize(
        wkspc1Result!!,
        NotificationSettings::class.java,
      ),
    )

    val wkspc2Result = fetchNotificationSettingsData(ctx, workspaceId2)
    Assertions.assertEquals(
      settingsWithBreakingChangeConfig,
      Jsons.deserialize(
        wkspc2Result!!,
        NotificationSettings::class.java,
      ),
    )

    val wkspc3Result = fetchNotificationSettingsData(ctx, workspaceId3)
    Assertions.assertEquals(
      settingsWithDisabledBreakingChangeConfig,
      Jsons.deserialize(
        wkspc3Result!!,
        NotificationSettings::class.java,
      ),
    )

    val wkspc4Result = fetchNotificationSettingsData(ctx, workspaceId4)
    Assertions.assertNull(
      Jsons.deserialize(
        wkspc4Result!!,
        NotificationSettings::class.java,
      ),
    )

    val wkspc5Result = fetchNotificationSettingsData(ctx, workspaceId5)
    Assertions.assertNull(wkspc5Result)
  }

  /**
   * Below this line: Local versions of generated config classes to avoid breaking tests if the
   * generated classes change.
   */
  @Suppress("ktlint:standard:enum-entry-name-case")
  private enum class NotificationType {
    customerio,
    webhook,
  }

  @JvmRecord
  private data class NotificationItem(
    val notificationType: List<NotificationType>,
  )

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JvmRecord
  private data class NotificationSettings(
    val sendOnSyncDisabled: NotificationItem,
    val sendOnConnectionUpdate: NotificationItem,
    val sendOnSyncDisabledWarning: NotificationItem,
    val sendOnConnectionUpdateActionRequired: NotificationItem,
    val sendOnBreakingChangeWarning: NotificationItem?,
    val sendOnBreakingChangeSyncsDisabled: NotificationItem?,
  )

  companion object {
    private const val WORKSPACE_TABLE = "workspace"
    private const val NOTIFICATION_SETTINGS_COLUMN = "notification_settings"

    private val EMPTY_NOTIFICATION_ITEM = NotificationItem(listOf())
    private val EMAIL_NOTIFICATION_ITEM = NotificationItem(listOf(NotificationType.customerio))
    private val SLACK_NOTIFICATION_ITEM = NotificationItem(listOf(NotificationType.webhook))
    private val EMAIL_AND_SLACK_NOTIFICATION_ITEM =
      NotificationItem(
        listOf(
          NotificationType.customerio,
          NotificationType.webhook,
        ),
      )

    private fun fetchNotificationSettingsData(
      ctx: DSLContext,
      id: UUID,
    ): String? {
      val record: Record =
        checkNotNull(
          ctx.fetchOne(
            DSL
              .select(
                DSL.field(
                  NOTIFICATION_SETTINGS_COLUMN,
                  JSONB::class.java,
                ),
              ).from(WORKSPACE_TABLE)
              .where(DSL.field("id").eq(id)),
          ),
        )

      val notificationSettingsCol =
        record.get(
          NOTIFICATION_SETTINGS_COLUMN,
          JSONB::class.java,
        )
      return notificationSettingsCol?.data()
    }

    private fun buildNotificationSettingsWithBCItem(breakingChangeNotificationItem: NotificationItem?): NotificationSettings =
      NotificationSettings(
        EMAIL_NOTIFICATION_ITEM,
        EMAIL_AND_SLACK_NOTIFICATION_ITEM,
        SLACK_NOTIFICATION_ITEM,
        EMAIL_NOTIFICATION_ITEM,
        breakingChangeNotificationItem,
        breakingChangeNotificationItem,
      )
  }
}
