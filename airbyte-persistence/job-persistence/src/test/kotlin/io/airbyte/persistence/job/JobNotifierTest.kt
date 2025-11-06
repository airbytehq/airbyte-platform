/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import io.airbyte.analytics.TrackingClient
import io.airbyte.api.client.WebUrlHelper
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobStatus
import io.airbyte.config.Notification
import io.airbyte.config.NotificationItem
import io.airbyte.config.NotificationSettings
import io.airbyte.config.ScopeType
import io.airbyte.config.SlackNotificationConfiguration
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.notification.NotificationClient
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import java.time.Instant
import java.util.UUID

internal class JobNotifierTest {
  private val airbyteConfig = AirbyteConfig(airbyteUrl = WEBAPP_URL)
  private val webUrlHelper = WebUrlHelper(airbyteConfig)

  private lateinit var jobNotifier: JobNotifier
  private lateinit var notificationClient: NotificationClient
  private lateinit var customerIoNotificationClient: NotificationClient
  private lateinit var trackingClient: TrackingClient

  private lateinit var job: Job
  private lateinit var sourceDefinition: StandardSourceDefinition
  private lateinit var actorDefinitionVersion: ActorDefinitionVersion
  private lateinit var destinationDefinition: StandardDestinationDefinition
  private lateinit var connectionService: ConnectionService
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var workspaceService: WorkspaceService

  @BeforeEach
  fun setup() {
    trackingClient = Mockito.mock(TrackingClient::class.java)
    connectionService = Mockito.mock(ConnectionService::class.java)
    sourceService = Mockito.mock(SourceService::class.java)
    destinationService = Mockito.mock(DestinationService::class.java)
    workspaceService = Mockito.mock(WorkspaceService::class.java)

    val actorDefinitionVersionHelper = Mockito.mock(ActorDefinitionVersionHelper::class.java)
    val workspaceHelper = Mockito.mock(WorkspaceHelper::class.java)
    val metricClient = Mockito.mock(MetricClient::class.java)
    jobNotifier =
      Mockito.spy(
        JobNotifier(
          webUrlHelper,
          connectionService,
          sourceService,
          destinationService,
          workspaceService,
          workspaceHelper,
          trackingClient,
          actorDefinitionVersionHelper,
          metricClient,
        ),
      )
    notificationClient = Mockito.mock(NotificationClient::class.java)
    customerIoNotificationClient = Mockito.mock(NotificationClient::class.java)
    Mockito
      .`when`(jobNotifier.getNotificationClientsFromNotificationItem(slackNotificationItem()))
      .thenReturn(java.util.List.of(notificationClient))
    Mockito
      .`when`(jobNotifier.getNotificationClientsFromNotificationItem(customerioAndSlackNotificationItem()))
      .thenReturn(java.util.List.of(notificationClient, customerIoNotificationClient))
    Mockito
      .`when`(jobNotifier.getNotificationClientsFromNotificationItem(customerioNotificationItem()))
      .thenReturn(java.util.List.of(customerIoNotificationClient))
    Mockito.`when`(jobNotifier.getNotificationClientsFromNotificationItem(noNotificationItem())).thenReturn(listOf())

    job = createJob()
    sourceDefinition =
      StandardSourceDefinition()
        .withName("source-test")
        .withSourceDefinitionId(UUID.randomUUID())
    destinationDefinition =
      StandardDestinationDefinition()
        .withName("destination-test")
        .withDestinationDefinitionId(UUID.randomUUID())
    actorDefinitionVersion =
      ActorDefinitionVersion()
        .withDockerImageTag(TEST_DOCKER_TAG)
        .withDockerRepository(TEST_DOCKER_REPO)
    Mockito
      .`when`(connectionService.getStandardSync(UUID.fromString(job.scope)))
      .thenReturn(
        StandardSync()
          .withName(
            "connection",
          ).withConnectionId(UUID.fromString(job.scope))
          .withSourceId(SOURCE_ID)
          .withDestinationId(DESTINATION_ID),
      )
    Mockito
      .`when`(sourceService.getSourceConnection(SOURCE_ID))
      .thenReturn(SourceConnection().withWorkspaceId(WORKSPACE_ID).withSourceId(SOURCE_ID).withName(SOURCE_NAME))
    Mockito
      .`when`(destinationService.getDestinationConnection(DESTINATION_ID))
      .thenReturn(DestinationConnection().withWorkspaceId(WORKSPACE_ID).withDestinationId(DESTINATION_ID).withName(DESTINATION_NAME))
    Mockito.`when`(sourceService.getSourceDefinitionFromConnection(org.mockito.kotlin.anyOrNull())).thenReturn(sourceDefinition)
    Mockito.`when`(destinationService.getDestinationDefinitionFromConnection(org.mockito.kotlin.anyOrNull())).thenReturn(destinationDefinition)
    Mockito.`when`(sourceService.getStandardSourceDefinition(org.mockito.kotlin.anyOrNull())).thenReturn(sourceDefinition)
    Mockito.`when`(destinationService.getStandardDestinationDefinition(org.mockito.kotlin.anyOrNull())).thenReturn(destinationDefinition)
    Mockito.`when`(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true)).thenReturn(
      workspace,
    )
    Mockito.`when`(workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(job.id)).thenReturn(WORKSPACE_ID)
    Mockito.`when`(notificationClient.notifyJobFailure(org.mockito.kotlin.anyOrNull(), ArgumentMatchers.anyString())).thenReturn(true)
    Mockito.`when`(notificationClient.getNotificationClientType()).thenReturn("client")
    Mockito.`when`(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID)).thenReturn(actorDefinitionVersion)
    Mockito
      .`when`(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, DESTINATION_ID))
      .thenReturn(actorDefinitionVersion)
  }

  @Test
  fun testFailJob() {
    val attemptStats: List<JobPersistence.AttemptStats> = ArrayList()
    jobNotifier.failJob(job, attemptStats)
    Mockito.verify(notificationClient).notifyJobFailure(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull())

    val metadata =
      mapOf(
        "connection_id" to UUID.fromString(job.scope),
        "connector_source_definition_id" to sourceDefinition.sourceDefinitionId,
        "connector_source" to "source-test",
        "connector_source_version" to TEST_DOCKER_TAG,
        "connector_source_docker_repository" to actorDefinitionVersion.dockerRepository,
        "connector_destination_definition_id" to destinationDefinition.destinationDefinitionId,
        "connector_destination" to "destination-test",
        "connector_destination_version" to TEST_DOCKER_TAG,
        "connector_destination_docker_repository" to actorDefinitionVersion.dockerRepository,
        "notification_type" to listOf(Notification.NotificationType.SLACK.toString()),
      )
    Mockito.verify(trackingClient).track(WORKSPACE_ID, ScopeType.WORKSPACE, JobNotifier.FAILURE_NOTIFICATION, metadata)
  }

  @Test
  fun testSuccessfulJobDoNotSendNotificationPerSettings() {
    val attemptStats: List<JobPersistence.AttemptStats> = ArrayList()
    jobNotifier.successJob(job, attemptStats)
  }

  @Test
  fun testSuccessfulJobSendNotification() {
    val item =
      NotificationItem()
        .withNotificationType(java.util.List.of(Notification.NotificationType.SLACK))
        .withSlackConfiguration(
          SlackNotificationConfiguration()
            .withWebhook("http://webhook"),
        )
    val workspace = workspace
    val sendNotificationOnSuccessSetting =
      NotificationSettings()
        .withSendOnSuccess(item)
    workspace.notificationSettings = sendNotificationOnSuccessSetting
    Mockito.`when`(jobNotifier.getNotificationClientsFromNotificationItem(item)).thenReturn(java.util.List.of(notificationClient))

    Mockito.`when`(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true)).thenReturn(workspace)
    val attemptStats: List<JobPersistence.AttemptStats> = ArrayList()
    jobNotifier.successJob(job, attemptStats)
    Mockito.verify(notificationClient).notifyJobSuccess(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull())
  }

  @Test
  fun testSendOnSyncDisabledWarning() {
    val attemptStats: List<JobPersistence.AttemptStats> = ArrayList()
    jobNotifier.autoDisableConnectionWarning(job, attemptStats)
    Mockito
      .verify(
        notificationClient,
        Mockito.never(),
      ).notifyConnectionDisableWarning(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull())
    Mockito.verify(customerIoNotificationClient).notifyConnectionDisableWarning(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull())
  }

  @Test
  fun testSendOnSyncDisabled() {
    val attemptStats: List<JobPersistence.AttemptStats> = ArrayList()
    jobNotifier.autoDisableConnection(job, attemptStats)
    Mockito.verify(notificationClient).notifyConnectionDisabled(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull())
    Mockito.verify(customerIoNotificationClient).notifyConnectionDisabled(org.mockito.kotlin.anyOrNull(), org.mockito.kotlin.anyOrNull())
  }

  @Test
  fun testBuildNotificationMetadata() {
    val notificationItem =
      NotificationItem()
        .withNotificationType(java.util.List.of(Notification.NotificationType.SLACK, Notification.NotificationType.CUSTOMERIO))
        .withSlackConfiguration(SlackNotificationConfiguration().withWebhook("http://someurl"))
    val connectionId = UUID.randomUUID()
    val metadata = jobNotifier.buildNotificationMetadata(connectionId, notificationItem)
    assert(metadata["connection_id"].toString() == connectionId.toString())
    assert(metadata.containsKey("notification_type"))
  }

  companion object {
    private const val WEBAPP_URL = "http://localhost:8000"
    private val NOW: Instant = Instant.now()
    private const val TEST_DOCKER_REPO = "airbyte/test-image"
    private const val TEST_DOCKER_TAG = "0.1.0"
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val SOURCE_ID: UUID = UUID.randomUUID()
    private val DESTINATION_ID: UUID = UUID.randomUUID()

    private const val SOURCE_NAME = "SOURCE"

    private const val DESTINATION_NAME = "DESTINATION"

    private val workspace: StandardWorkspace
      get() =
        StandardWorkspace()
          .withWorkspaceId(WORKSPACE_ID)
          .withCustomerId(UUID.randomUUID())
          .withNotifications(java.util.List.of(slackNotification))
          .withEmail("")
          .withNotificationSettings(
            NotificationSettings()
              .withSendOnFailure(slackNotificationItem())
              .withSendOnSuccess(noNotificationItem())
              .withSendOnConnectionUpdate(noNotificationItem())
              .withSendOnSyncDisabled(customerioAndSlackNotificationItem())
              .withSendOnSyncDisabledWarning(customerioNotificationItem())
              .withSendOnConnectionUpdateActionRequired(noNotificationItem()),
          )

    private fun slackNotificationItem(): NotificationItem =
      NotificationItem()
        .withNotificationType(java.util.List.of(Notification.NotificationType.SLACK))
        .withSlackConfiguration(
          SlackNotificationConfiguration()
            .withWebhook("http://random.webhook.url/hooks.slack.com/"),
        )

    private fun noNotificationItem(): NotificationItem =
      NotificationItem()
        .withNotificationType(listOf())

    private fun customerioNotificationItem(): NotificationItem =
      NotificationItem()
        .withNotificationType(java.util.List.of(Notification.NotificationType.CUSTOMERIO))

    private fun customerioAndSlackNotificationItem(): NotificationItem =
      NotificationItem()
        .withNotificationType(java.util.List.of(Notification.NotificationType.SLACK, Notification.NotificationType.CUSTOMERIO))
        .withSlackConfiguration(
          SlackNotificationConfiguration()
            .withWebhook("http://random.webhook.url/hooks.slack.com/"),
        )

    private fun createJob(): Job =
      Job(
        10L,
        ConfigType.SYNC,
        UUID.randomUUID().toString(),
        JobConfig(),
        emptyList(),
        JobStatus.FAILED,
        NOW.epochSecond,
        NOW.epochSecond,
        NOW.epochSecond + 123456L,
        true,
      )

    private val slackNotification: Notification
      get() =
        Notification()
          .withNotificationType(Notification.NotificationType.SLACK)
          .withSlackConfiguration(
            SlackNotificationConfiguration()
              .withWebhook("http://random.webhook.url/hooks.slack.com/"),
          )
  }
}
