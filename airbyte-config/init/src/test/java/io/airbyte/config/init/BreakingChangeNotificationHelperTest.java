/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorType;
import io.airbyte.config.Notification.NotificationType;
import io.airbyte.config.NotificationItem;
import io.airbyte.config.NotificationSettings;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.init.BreakingChangeNotificationHelper.BreakingChangeNotificationData;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.NotifyOnConnectorBreakingChanges;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.notification.NotificationClient;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BreakingChangeNotificationHelperTest {

  private static final UUID WORKSPACE_ID_1 = UUID.randomUUID();
  private static final UUID WORKSPACE_ID_2 = UUID.randomUUID();
  private static final String WORKSPACE_EMAIL_1 = "workspace1@airbyte.com";
  private static final String WORKSPACE_EMAIL_2 = "workspace2@airbyte.com";
  private static final String CONNECTOR_NAME = "PostgreSQL";

  private ConfigRepository mConfigRepository;
  private NotificationClient mNotificationClient;
  private FeatureFlagClient mFeatureFlagClient;
  private BreakingChangeNotificationHelper breakingChangeNotificationHelper;

  @BeforeEach
  void setup() {
    mConfigRepository = mock(ConfigRepository.class);
    mNotificationClient = mock(NotificationClient.class);
    mFeatureFlagClient = mock(TestClient.class);
    breakingChangeNotificationHelper = new BreakingChangeNotificationHelper(mConfigRepository, mFeatureFlagClient, mNotificationClient);

    when(mFeatureFlagClient.boolVariation(eq(NotifyOnConnectorBreakingChanges.INSTANCE), any())).thenReturn(true);
  }

  @Test
  void testNoNotificationsOnFFDisabled() throws IOException {
    when(mFeatureFlagClient.boolVariation(NotifyOnConnectorBreakingChanges.INSTANCE, new Workspace(WORKSPACE_ID_1))).thenReturn(false);
    when(mConfigRepository.listStandardWorkspacesWithIds(List.of(WORKSPACE_ID_1), false))
        .thenReturn(List.of(new StandardWorkspace()
            .withWorkspaceId(WORKSPACE_ID_1)
            .withEmail(WORKSPACE_EMAIL_1)
            .withNotificationSettings(new NotificationSettings()
                .withSendOnBreakingChangeSyncsDisabled(new NotificationItem()
                    .withNotificationType(List.of(NotificationType.CUSTOMERIO))))));

    final List<BreakingChangeNotificationData> notifications = List
        .of(new BreakingChangeNotificationData(ActorType.SOURCE, CONNECTOR_NAME, List.of(WORKSPACE_ID_1), new ActorDefinitionBreakingChange()));

    breakingChangeNotificationHelper.notifyDeprecatedSyncs(notifications);
    breakingChangeNotificationHelper.notifyDisabledSyncs(notifications);

    verify(mConfigRepository, times(2)).listStandardWorkspacesWithIds(List.of(WORKSPACE_ID_1), false);
    verifyNoMoreInteractions(mConfigRepository);
    verifyNoInteractions(mNotificationClient);
  }

  @Test
  void testNotifyDisabledSyncs() throws IOException, InterruptedException {
    final List<UUID> workspaceIds = List.of(WORKSPACE_ID_1, WORKSPACE_ID_2);
    final ActorDefinitionBreakingChange breakingChange = new ActorDefinitionBreakingChange().withMessage("some breaking change");
    final List<BreakingChangeNotificationData> notifications = List.of(new BreakingChangeNotificationData(
        ActorType.SOURCE, CONNECTOR_NAME, workspaceIds, breakingChange));

    when(mConfigRepository.listStandardWorkspacesWithIds(workspaceIds, false)).thenReturn(List.of(
        new StandardWorkspace()
            .withWorkspaceId(WORKSPACE_ID_1)
            .withEmail(WORKSPACE_EMAIL_1)
            .withNotificationSettings(new NotificationSettings()
                .withSendOnBreakingChangeSyncsDisabled(new NotificationItem()
                    .withNotificationType(List.of(NotificationType.CUSTOMERIO)))),
        new StandardWorkspace()
            .withWorkspaceId(WORKSPACE_ID_2)
            .withEmail(WORKSPACE_EMAIL_2).withNotificationSettings(new NotificationSettings())));

    breakingChangeNotificationHelper.notifyDisabledSyncs(notifications);

    verify(mNotificationClient).notifyBreakingChangeSyncsDisabled(List.of(WORKSPACE_EMAIL_1), CONNECTOR_NAME, ActorType.SOURCE, breakingChange);
    verify(mConfigRepository).listStandardWorkspacesWithIds(workspaceIds, false);
    verifyNoMoreInteractions(mConfigRepository);
  }

  @Test
  void testNotifyDeprecatedSyncs() throws IOException, InterruptedException {
    final List<UUID> workspaceIds = List.of(WORKSPACE_ID_1, WORKSPACE_ID_2);
    final ActorDefinitionBreakingChange breakingChange = new ActorDefinitionBreakingChange()
        .withVersion(new Version("1.0.0"))
        .withMessage("some breaking change");
    final List<BreakingChangeNotificationData> notifications = List.of(new BreakingChangeNotificationData(
        ActorType.DESTINATION, CONNECTOR_NAME, workspaceIds, breakingChange));

    when(mConfigRepository.listStandardWorkspacesWithIds(workspaceIds, false)).thenReturn(List.of(
        new StandardWorkspace()
            .withWorkspaceId(WORKSPACE_ID_1)
            .withEmail(WORKSPACE_EMAIL_1)
            .withNotificationSettings(new NotificationSettings()
                .withSendOnBreakingChangeWarning(new NotificationItem()
                    .withNotificationType(List.of(NotificationType.CUSTOMERIO)))),
        new StandardWorkspace()
            .withWorkspaceId(WORKSPACE_ID_2)
            .withEmail(WORKSPACE_EMAIL_2)
            .withNotificationSettings(new NotificationSettings())));

    breakingChangeNotificationHelper.notifyDeprecatedSyncs(notifications);

    verify(mNotificationClient).notifyBreakingChangeWarning(List.of(WORKSPACE_EMAIL_1), CONNECTOR_NAME, ActorType.DESTINATION, breakingChange);
    verify(mConfigRepository).listStandardWorkspacesWithIds(workspaceIds, false);
    verifyNoMoreInteractions(mConfigRepository);
  }

}
