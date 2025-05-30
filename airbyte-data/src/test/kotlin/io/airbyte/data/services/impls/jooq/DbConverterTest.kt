/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import com.google.common.io.Resources
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.protocol.DefaultProtocolSerializer
import io.airbyte.config.Notification
import io.airbyte.config.Notification.NotificationType
import io.airbyte.config.NotificationSettings
import io.airbyte.db.instance.configs.jooq.generated.Tables.DATAPLANE_GROUP
import io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.charset.Charset
import java.time.OffsetDateTime
import java.util.UUID
import io.airbyte.config.ConfiguredAirbyteCatalog as InternalConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog as ProtocolConfiguredAirbyteCatalog

internal class DbConverterTest {
  @ValueSource(
    strings = [
      "non-reg-configured-catalog/facebook.json",
      "non-reg-configured-catalog/ga.json",
      "non-reg-configured-catalog/hubspot.json",
      "non-reg-configured-catalog/internal-pg.json",
      "non-reg-configured-catalog/stripe.json",
    ],
  )
  @ParameterizedTest
  fun testConfiguredAirbyteCatalogDeserNonReg(resourceName: String) {
    val rawCatalog = Resources.toString(Resources.getResource(resourceName), Charset.defaultCharset())

    val protocolCatalog = parseConfiguredAirbyteCatalogAsProtocol(rawCatalog)
    val internalCatalog = parseConfiguredAirbyteCatalog(rawCatalog)

    assertCatalogsAreEqual(internalCatalog, protocolCatalog)
  }

  private fun assertCatalogsAreEqual(
    internal: InternalConfiguredAirbyteCatalog,
    protocol: ProtocolConfiguredAirbyteCatalog,
  ) {
    val internalToProtocol: ProtocolConfiguredAirbyteCatalog =
      parseConfiguredAirbyteCatalogAsProtocol(DefaultProtocolSerializer().serialize(internal, false))

    // This is to ease the defaults in the comparison. The frozen catalogs do not have includeFiles,
    // we default to false in the new model
    protocol.streams.map {
      assertNull(it.includeFiles)
      it.withIncludeFiles(false)
    }
    assertEquals(protocol, internalToProtocol)
  }

  companion object {
    // This is hardcoded here on purpose for regression tests
    // Until we defined our internal model (<=0.58), this was how we were loading catalog strings from
    // our persistence layer.
    // This is to ensure we remain backward compatible so it should remain as is until we drop support
    private fun parseConfiguredAirbyteCatalogAsProtocol(catalogString: String): ProtocolConfiguredAirbyteCatalog =
      Jsons.deserialize(catalogString, ProtocolConfiguredAirbyteCatalog::class.java)

    private fun parseConfiguredAirbyteCatalog(catalogString: String): InternalConfiguredAirbyteCatalog {
      // TODO this should be using the proper SerDe stack once migrated to support our internal models
      // This is making sure our new format can still load older serialization format
      return Jsons.deserialize(catalogString, InternalConfiguredAirbyteCatalog::class.java)
    }
  }

  @Test
  fun `test buildStandardWorkspace`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val customerId = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()
    val createdAt = OffsetDateTime.now()
    val updatedAt = OffsetDateTime.now()

    val notificationsJson =
      """
      [
        {
          "notificationType": "customerio",
          "sendOnSuccess": false,
          "sendOnFailure": true,
          "slackConfiguration": null,
          "customerioConfiguration": null
        }
      ]
      """.trimIndent()
    val notificationSettingsJson = """{"customerio": true, "slack": false}"""
    val webhookOperationConfigsJson = """{"enabled": false}"""

    val mockRecord: Record =
      DSL
        .using(org.jooq.SQLDialect.POSTGRES)
        .newRecord(*WORKSPACE.fields(), DATAPLANE_GROUP.ID)
        .apply {
          set(WORKSPACE.ID, workspaceId)
          set(WORKSPACE.NAME, "Test Workspace")
          set(WORKSPACE.SLUG, "test-workspace")
          set(WORKSPACE.INITIAL_SETUP_COMPLETE, true)
          set(WORKSPACE.CUSTOMER_ID, customerId)
          set(WORKSPACE.EMAIL, "test@example.com")
          set(WORKSPACE.ANONYMOUS_DATA_COLLECTION, false)
          set(WORKSPACE.SEND_NEWSLETTER, true)
          set(WORKSPACE.SEND_SECURITY_UPDATES, false)
          set(WORKSPACE.DISPLAY_SETUP_WIZARD, true)
          set(WORKSPACE.TOMBSTONE, false)
          set(WORKSPACE.NOTIFICATIONS, JSONB.jsonb(notificationsJson))
          set(WORKSPACE.NOTIFICATION_SETTINGS, JSONB.jsonb(notificationSettingsJson))
          set(WORKSPACE.FIRST_SYNC_COMPLETE, true)
          set(WORKSPACE.FEEDBACK_COMPLETE, false)
          set(WORKSPACE.DATAPLANE_GROUP_ID, dataplaneGroupId)
          set(WORKSPACE.WEBHOOK_OPERATION_CONFIGS, JSONB.jsonb(webhookOperationConfigsJson))
          set(WORKSPACE.ORGANIZATION_ID, organizationId)
          set(WORKSPACE.CREATED_AT, createdAt)
          set(WORKSPACE.UPDATED_AT, updatedAt)
        }

    val workspace = DbConverter.buildStandardWorkspace(mockRecord)

    assertEquals(workspaceId, workspace.workspaceId)
    assertEquals("Test Workspace", workspace.name)
    assertEquals("test-workspace", workspace.slug)
    assertEquals(true, workspace.initialSetupComplete)
    assertEquals(customerId, workspace.customerId)
    assertEquals("test@example.com", workspace.email)
    assertEquals(false, workspace.anonymousDataCollection)
    assertEquals(true, workspace.news)
    assertEquals(false, workspace.securityUpdates)
    assertEquals(true, workspace.displaySetupWizard)
    assertEquals(false, workspace.tombstone)
    assertEquals(true, workspace.firstCompletedSync)
    assertEquals(false, workspace.feedbackDone)
    assertEquals(dataplaneGroupId, workspace.dataplaneGroupId)
    assertEquals(organizationId, workspace.organizationId)
    assertEquals(createdAt.toEpochSecond(), workspace.createdAt)
    assertEquals(updatedAt.toEpochSecond(), workspace.updatedAt)

    val expectedNotifications =
      listOf(
        Notification()
          .withNotificationType(NotificationType.CUSTOMERIO)
          .withSendOnSuccess(false)
          .withSendOnFailure(true)
          .withSlackConfiguration(null)
          .withCustomerioConfiguration(null),
      )
    assertEquals(expectedNotifications, workspace.notifications)

    val expectedNotificationSettings = Jsons.deserialize(notificationSettingsJson, NotificationSettings::class.java)
    assertEquals(expectedNotificationSettings, workspace.notificationSettings)

    assertEquals(Jsons.deserialize(webhookOperationConfigsJson), workspace.webhookOperationConfigs)
  }
}
