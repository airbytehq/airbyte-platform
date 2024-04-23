/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.Notification;
import io.airbyte.config.Notification.NotificationType;
import io.airbyte.config.NotificationItem;
import io.airbyte.config.NotificationSettings;
import io.airbyte.config.SlackNotificationConfiguration;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.notification.NotificationClient;
import io.airbyte.notification.messages.SyncSummary;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.persistence.job.models.JobStatus;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class JobNotifierTest {

  private static final String WEBAPP_URL = "http://localhost:8000";
  private static final Instant NOW = Instant.now();
  private static final String TEST_DOCKER_REPO = "airbyte/test-image";
  private static final String TEST_DOCKER_TAG = "0.1.0";
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final UUID DESTINATION_ID = UUID.randomUUID();

  private static final String SOURCE_NAME = "SOURCE";

  private static final String DESTINATION_NAME = "DESTINATION";

  private final WebUrlHelper webUrlHelper = new WebUrlHelper(WEBAPP_URL);

  private ConfigRepository configRepository;
  private WorkspaceHelper workspaceHelper;
  private JobNotifier jobNotifier;
  private NotificationClient notificationClient;
  private NotificationClient customerIoNotificationClient;
  private TrackingClient trackingClient;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;

  private Job job;
  private StandardSourceDefinition sourceDefinition;
  private ActorDefinitionVersion actorDefinitionVersion;
  private StandardDestinationDefinition destinationDefinition;

  @BeforeEach
  void setup() throws Exception {
    configRepository = mock(ConfigRepository.class);
    workspaceHelper = mock(WorkspaceHelper.class);
    trackingClient = mock(TrackingClient.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);

    jobNotifier = Mockito.spy(new JobNotifier(webUrlHelper, configRepository, workspaceHelper, trackingClient, actorDefinitionVersionHelper));
    notificationClient = mock(NotificationClient.class);
    customerIoNotificationClient = mock(NotificationClient.class);
    when(jobNotifier.getNotificationClientsFromNotificationItem(slackNotificationItem())).thenReturn(List.of(notificationClient));
    when(jobNotifier.getNotificationClientsFromNotificationItem(customerioAndSlackNotificationItem()))
        .thenReturn(List.of(notificationClient, customerIoNotificationClient));
    when(jobNotifier.getNotificationClientsFromNotificationItem(customerioNotificationItem())).thenReturn(List.of(customerIoNotificationClient));
    when(jobNotifier.getNotificationClientsFromNotificationItem(noNotificationItem())).thenReturn(List.of());

    job = createJob();
    sourceDefinition = new StandardSourceDefinition()
        .withName("source-test")
        .withSourceDefinitionId(UUID.randomUUID());
    destinationDefinition = new StandardDestinationDefinition()
        .withName("destination-test")
        .withDestinationDefinitionId(UUID.randomUUID());
    actorDefinitionVersion = new ActorDefinitionVersion()
        .withDockerImageTag(TEST_DOCKER_TAG)
        .withDockerRepository(TEST_DOCKER_REPO);
    when(configRepository.getStandardSync(UUID.fromString(job.getScope())))
        .thenReturn(new StandardSync().withSourceId(SOURCE_ID).withDestinationId(DESTINATION_ID));
    when(configRepository.getSourceConnection(SOURCE_ID))
        .thenReturn(new SourceConnection().withWorkspaceId(WORKSPACE_ID).withSourceId(SOURCE_ID).withName(SOURCE_NAME));
    when(configRepository.getDestinationConnection(DESTINATION_ID))
        .thenReturn(new DestinationConnection().withWorkspaceId(WORKSPACE_ID).withDestinationId(DESTINATION_ID).withName(DESTINATION_NAME));
    when(configRepository.getSourceDefinitionFromConnection(any())).thenReturn(sourceDefinition);
    when(configRepository.getDestinationDefinitionFromConnection(any())).thenReturn(destinationDefinition);
    when(configRepository.getStandardSourceDefinition(any())).thenReturn(sourceDefinition);
    when(configRepository.getStandardDestinationDefinition(any())).thenReturn(destinationDefinition);
    when(configRepository.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true)).thenReturn(getWorkspace());
    when(workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(job.getId())).thenReturn(WORKSPACE_ID);
    when(notificationClient.notifyJobFailure(any(), ArgumentMatchers.anyString())).thenReturn(true);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID)).thenReturn(actorDefinitionVersion);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, DESTINATION_ID)).thenReturn(actorDefinitionVersion);
  }

  @Test
  void testFailJob() throws IOException, InterruptedException, JsonValidationException, ConfigNotFoundException {
    List<JobPersistence.AttemptStats> attemptStats = new ArrayList<>();
    jobNotifier.failJob(job, attemptStats);
    final DateTimeFormatter formatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.FULL).withZone(ZoneId.systemDefault());
    SyncSummary summary = SyncSummary.builder().build();
    verify(notificationClient).notifyJobFailure(any(), ArgumentMatchers.eq(null));

    final Builder<String, Object> metadata = ImmutableMap.builder();
    metadata.put("connection_id", UUID.fromString(job.getScope()));
    metadata.put("connector_source_definition_id", sourceDefinition.getSourceDefinitionId());
    metadata.put("connector_source", "source-test");
    metadata.put("connector_source_version", TEST_DOCKER_TAG);
    metadata.put("connector_source_docker_repository", actorDefinitionVersion.getDockerRepository());
    metadata.put("connector_destination_definition_id", destinationDefinition.getDestinationDefinitionId());
    metadata.put("connector_destination", "destination-test");
    metadata.put("connector_destination_version", TEST_DOCKER_TAG);
    metadata.put("connector_destination_docker_repository", actorDefinitionVersion.getDockerRepository());
    metadata.put("notification_type", List.of(NotificationType.SLACK.toString()));
    verify(trackingClient).track(WORKSPACE_ID, JobNotifier.FAILURE_NOTIFICATION, metadata.build());
  }

  @Test
  void testSuccessfulJobDoNotSendNotificationPerSettings()
      throws IOException, InterruptedException {
    List<JobPersistence.AttemptStats> attemptStats = new ArrayList<>();
    jobNotifier.successJob(job, attemptStats);
  }

  @Test
  void testSuccessfulJobSendNotification() throws IOException, InterruptedException, JsonValidationException, ConfigNotFoundException {

    NotificationItem item = new NotificationItem()
        .withNotificationType(List.of(NotificationType.SLACK))
        .withSlackConfiguration(new SlackNotificationConfiguration()
            .withWebhook("http://webhook"));
    StandardWorkspace workspace = getWorkspace();
    NotificationSettings sendNotificationOnSuccessSetting = new NotificationSettings()
        .withSendOnSuccess(item);
    workspace.setNotificationSettings(sendNotificationOnSuccessSetting);
    when(jobNotifier.getNotificationClientsFromNotificationItem(item)).thenReturn(List.of(notificationClient));

    when(configRepository.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true)).thenReturn(workspace);
    List<JobPersistence.AttemptStats> attemptStats = new ArrayList<>();
    jobNotifier.successJob(job, attemptStats);
    verify(notificationClient).notifyJobSuccess(any(), any());
  }

  @Test
  void testSendOnSyncDisabledWarning()
      throws IOException, InterruptedException {
    List<JobPersistence.AttemptStats> attemptStats = new ArrayList<>();
    jobNotifier.autoDisableConnectionWarning(job, attemptStats);
    verify(notificationClient, never()).notifyConnectionDisableWarning(any(), any());
    verify(customerIoNotificationClient).notifyConnectionDisableWarning(any(), any());
  }

  @Test
  void testSendOnSyncDisabled()
      throws IOException, InterruptedException, JsonValidationException, ConfigNotFoundException {
    List<JobPersistence.AttemptStats> attemptStats = new ArrayList<>();
    jobNotifier.autoDisableConnection(job, attemptStats);
    verify(notificationClient).notifyConnectionDisabled(any(), any());
    verify(customerIoNotificationClient).notifyConnectionDisabled(any(), any());
  }

  @Test
  void testBuildNotificationMetadata() {
    NotificationItem notificationItem = new NotificationItem()
        .withNotificationType(List.of(NotificationType.SLACK, NotificationType.CUSTOMERIO))
        .withSlackConfiguration(new SlackNotificationConfiguration().withWebhook("http://someurl"));
    UUID connectionId = UUID.randomUUID();
    var metadata = jobNotifier.buildNotificationMetadata(connectionId, notificationItem);
    assert metadata.get("connection_id").toString().equals(connectionId.toString());
    assert metadata.containsKey("notification_type");
  }

  private static StandardWorkspace getWorkspace() {
    return new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withCustomerId(UUID.randomUUID())
        .withNotifications(List.of(getSlackNotification()))
        .withNotificationSettings(new NotificationSettings()
            .withSendOnFailure(slackNotificationItem())
            .withSendOnSuccess(noNotificationItem())
            .withSendOnConnectionUpdate(noNotificationItem())
            .withSendOnSyncDisabled(customerioAndSlackNotificationItem())
            .withSendOnSyncDisabledWarning(customerioNotificationItem())
            .withSendOnConnectionUpdateActionRequired(noNotificationItem()));
  }

  private static NotificationItem slackNotificationItem() {
    return new NotificationItem()
        .withNotificationType(List.of(NotificationType.SLACK))
        .withSlackConfiguration(
            new SlackNotificationConfiguration()
                .withWebhook("http://random.webhook.url/hooks.slack.com/"));
  }

  private static NotificationItem noNotificationItem() {
    return new NotificationItem()
        .withNotificationType(List.of());
  }

  private static NotificationItem customerioNotificationItem() {
    return new NotificationItem()
        .withNotificationType(List.of(NotificationType.CUSTOMERIO));
  }

  private static NotificationItem customerioAndSlackNotificationItem() {
    return new NotificationItem()
        .withNotificationType(List.of(NotificationType.SLACK, NotificationType.CUSTOMERIO))
        .withSlackConfiguration(
            new SlackNotificationConfiguration()
                .withWebhook("http://random.webhook.url/hooks.slack.com/"));
  }

  private static Job createJob() {
    return new Job(
        10L,
        ConfigType.SYNC,
        UUID.randomUUID().toString(),
        new JobConfig(),
        Collections.emptyList(),
        JobStatus.FAILED,
        NOW.getEpochSecond(),
        NOW.getEpochSecond(),
        NOW.getEpochSecond() + 123456L);
  }

  private static Notification getSlackNotification() {
    return new Notification()
        .withNotificationType(NotificationType.SLACK)
        .withSlackConfiguration(new SlackNotificationConfiguration()
            .withWebhook("http://random.webhook.url/hooks.slack.com/"));
  }

}
