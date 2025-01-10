/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init

import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorType
import io.airbyte.config.Notification
import io.airbyte.config.NotificationItem
import io.airbyte.config.NotificationSettings
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.init.BreakingChangeNotificationHelper.BreakingChangeNotificationData
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.NotifyOnConnectorBreakingChanges
import io.airbyte.featureflag.Workspace
import io.airbyte.notification.NotificationClient
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifySequence
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID

internal class BreakingChangeNotificationHelperTest {
  private val mWorkspaceService: WorkspaceService = mockk()
  private val mNotificationClient: NotificationClient = mockk()
  private val mFeatureFlagClient: FeatureFlagClient = mockk()
  private lateinit var breakingChangeNotificationHelper: BreakingChangeNotificationHelper

  @BeforeEach
  fun setup() {
    breakingChangeNotificationHelper = BreakingChangeNotificationHelper(mWorkspaceService, mFeatureFlagClient, mNotificationClient)

    every { mFeatureFlagClient.boolVariation(NotifyOnConnectorBreakingChanges, any()) } returns true
  }

  @Test
  @Throws(IOException::class)
  fun `no notifications should be sent if the feature flag is disabled`() {
    every { mFeatureFlagClient.boolVariation(NotifyOnConnectorBreakingChanges, Workspace(WORKSPACE_ID_1)) } returns false
    every { mWorkspaceService.listStandardWorkspacesWithIds(listOf(WORKSPACE_ID_1), false) } returns
      listOf(
        StandardWorkspace()
          .withWorkspaceId(WORKSPACE_ID_1)
          .withEmail(WORKSPACE_EMAIL_1)
          .withNotificationSettings(
            NotificationSettings()
              .withSendOnBreakingChangeSyncsDisabled(
                NotificationItem()
                  .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
              ),
          ),
      )
    val notifications =
      listOf(BreakingChangeNotificationData(ActorType.SOURCE, CONNECTOR_NAME, listOf(WORKSPACE_ID_1), ActorDefinitionBreakingChange()))

    breakingChangeNotificationHelper.notifyDeprecatedSyncs(notifications)
    breakingChangeNotificationHelper.notifyDisabledSyncs(notifications)

    verify(exactly = 2) { mWorkspaceService.listStandardWorkspacesWithIds(listOf(WORKSPACE_ID_1), false) }
    verifySequence {
      mWorkspaceService.listStandardWorkspacesWithIds(listOf(WORKSPACE_ID_1), false)
      mWorkspaceService.listStandardWorkspacesWithIds(listOf(WORKSPACE_ID_1), false)
    }
    confirmVerified(mWorkspaceService)
    confirmVerified(mNotificationClient)
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun `notifications are sent for disabled syncs`() {
    val workspaceIds = listOf(WORKSPACE_ID_1, WORKSPACE_ID_2)
    val breakingChange = ActorDefinitionBreakingChange().withMessage("some breaking change")
    val notifications =
      listOf(
        BreakingChangeNotificationData(
          ActorType.SOURCE,
          CONNECTOR_NAME,
          workspaceIds,
          breakingChange,
        ),
      )

    every { mWorkspaceService.listStandardWorkspacesWithIds(workspaceIds, false) } returns
      listOf(
        StandardWorkspace()
          .withWorkspaceId(WORKSPACE_ID_1)
          .withEmail(WORKSPACE_EMAIL_1)
          .withNotificationSettings(
            NotificationSettings()
              .withSendOnBreakingChangeSyncsDisabled(
                NotificationItem()
                  .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
              ),
          ),
        StandardWorkspace()
          .withWorkspaceId(WORKSPACE_ID_2)
          .withEmail(WORKSPACE_EMAIL_2).withNotificationSettings(NotificationSettings()),
      )
    breakingChangeNotificationHelper.notifyDisabledSyncs(notifications)

    verify { mNotificationClient.notifyBreakingChangeSyncsDisabled(listOf(WORKSPACE_EMAIL_1), CONNECTOR_NAME, ActorType.SOURCE, breakingChange) }
    verify { mWorkspaceService.listStandardWorkspacesWithIds(workspaceIds, false) }
    confirmVerified(mWorkspaceService)
  }

  @Test
  @Throws(IOException::class, InterruptedException::class)
  fun `notifications are sent for deprecated syncs`() {
    val workspaceIds = listOf(WORKSPACE_ID_1, WORKSPACE_ID_2)
    val breakingChange =
      ActorDefinitionBreakingChange()
        .withVersion(Version("1.0.0"))
        .withMessage("some breaking change")
    val notifications =
      listOf(
        BreakingChangeNotificationData(
          ActorType.DESTINATION,
          CONNECTOR_NAME,
          workspaceIds,
          breakingChange,
        ),
      )

    every { mWorkspaceService.listStandardWorkspacesWithIds(workspaceIds, false) } returns
      listOf(
        StandardWorkspace()
          .withWorkspaceId(WORKSPACE_ID_1)
          .withEmail(WORKSPACE_EMAIL_1)
          .withNotificationSettings(
            NotificationSettings()
              .withSendOnBreakingChangeWarning(
                NotificationItem()
                  .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO)),
              ),
          ),
        StandardWorkspace()
          .withWorkspaceId(WORKSPACE_ID_2)
          .withEmail(WORKSPACE_EMAIL_2)
          .withNotificationSettings(NotificationSettings()),
      )
    breakingChangeNotificationHelper.notifyDeprecatedSyncs(notifications)

    verify { mNotificationClient.notifyBreakingChangeWarning(listOf(WORKSPACE_EMAIL_1), CONNECTOR_NAME, ActorType.DESTINATION, breakingChange) }
    verify { mWorkspaceService.listStandardWorkspacesWithIds(workspaceIds, false) }
    confirmVerified(mWorkspaceService)
  }

  companion object {
    private val WORKSPACE_ID_1: UUID = UUID.randomUUID()
    private val WORKSPACE_ID_2: UUID = UUID.randomUUID()
    private const val WORKSPACE_EMAIL_1 = "workspace1@airbyte.com"
    private const val WORKSPACE_EMAIL_2 = "workspace2@airbyte.com"
    private const val CONNECTOR_NAME = "PostgreSQL"
  }
}
