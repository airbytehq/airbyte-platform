/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
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
    trackingClient = mockk<TrackingClient>(relaxed = true)
    connectionService = mockk<ConnectionService>()
    sourceService = mockk<SourceService>()
    destinationService = mockk<DestinationService>()
    workspaceService = mockk<WorkspaceService>()

    val actorDefinitionVersionHelper = mockk<ActorDefinitionVersionHelper>()
    val workspaceHelper = mockk<WorkspaceHelper>()
    val metricClient = mockk<MetricClient>(relaxed = true)
    jobNotifier =
      spyk(
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
    notificationClient = mockk<NotificationClient>(relaxed = true)
    customerIoNotificationClient = mockk<NotificationClient>(relaxed = true)
    every { jobNotifier.getNotificationClientsFromNotificationItem(slackNotificationItem()) } returns listOf(notificationClient)
    every {
      jobNotifier.getNotificationClientsFromNotificationItem(customerioAndSlackNotificationItem())
    } returns listOf(notificationClient, customerIoNotificationClient)
    every { jobNotifier.getNotificationClientsFromNotificationItem(customerioNotificationItem()) } returns listOf(customerIoNotificationClient)
    every { jobNotifier.getNotificationClientsFromNotificationItem(noNotificationItem()) } returns listOf()

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
    every { connectionService.getStandardSync(UUID.fromString(job.scope)) } returns
      StandardSync()
        .withName(
          "connection",
        ).withConnectionId(UUID.fromString(job.scope))
        .withSourceId(SOURCE_ID)
        .withDestinationId(DESTINATION_ID)
    every { sourceService.getSourceConnection(SOURCE_ID) } returns
      SourceConnection().withWorkspaceId(WORKSPACE_ID).withSourceId(SOURCE_ID).withName(SOURCE_NAME)
    every { destinationService.getDestinationConnection(DESTINATION_ID) } returns
      DestinationConnection().withWorkspaceId(WORKSPACE_ID).withDestinationId(DESTINATION_ID).withName(DESTINATION_NAME)
    every { sourceService.getSourceDefinitionFromConnection(any()) } returns sourceDefinition
    every { destinationService.getDestinationDefinitionFromConnection(any()) } returns destinationDefinition
    every { sourceService.getStandardSourceDefinition(any()) } returns sourceDefinition
    every { destinationService.getStandardDestinationDefinition(any()) } returns destinationDefinition
    every { workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true) } returns workspace
    every { workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(job.id) } returns WORKSPACE_ID
    every { notificationClient.notifyJobFailure(any(), any<String>()) } returns true
    every { notificationClient.getNotificationClientType() } returns "client"
    every { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID) } returns actorDefinitionVersion
    every {
      actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, DESTINATION_ID)
    } returns actorDefinitionVersion
  }

  @Test
  fun testFailJob() {
    val attemptStats: List<JobPersistence.AttemptStats> = ArrayList()
    jobNotifier.failJob(job, attemptStats)
    verify { notificationClient.notifyJobFailure(any(), any()) }

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
    verify { trackingClient.track(WORKSPACE_ID, ScopeType.WORKSPACE, JobNotifier.FAILURE_NOTIFICATION, metadata) }
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
        .withNotificationType(listOf(Notification.NotificationType.SLACK))
        .withSlackConfiguration(
          SlackNotificationConfiguration()
            .withWebhook("http://webhook"),
        )
    val workspace = workspace
    val sendNotificationOnSuccessSetting =
      NotificationSettings()
        .withSendOnSuccess(item)
    workspace.notificationSettings = sendNotificationOnSuccessSetting
    every { jobNotifier.getNotificationClientsFromNotificationItem(item) } returns listOf(notificationClient)

    every { workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true) } returns workspace
    val attemptStats: List<JobPersistence.AttemptStats> = ArrayList()
    jobNotifier.successJob(job, attemptStats)
    verify { notificationClient.notifyJobSuccess(any(), any()) }
  }

  @Test
  fun testSendOnSyncDisabledWarning() {
    val attemptStats: List<JobPersistence.AttemptStats> = ArrayList()
    jobNotifier.autoDisableConnectionWarning(job, attemptStats)
    verify(exactly = 0) { notificationClient.notifyConnectionDisableWarning(any(), any()) }
    verify { customerIoNotificationClient.notifyConnectionDisableWarning(any(), any()) }
  }

  @Test
  fun testSendOnSyncDisabled() {
    val attemptStats: List<JobPersistence.AttemptStats> = ArrayList()
    jobNotifier.autoDisableConnection(job, attemptStats)
    verify { notificationClient.notifyConnectionDisabled(any(), any()) }
    verify { customerIoNotificationClient.notifyConnectionDisabled(any(), any()) }
  }

  @Test
  fun testBuildNotificationMetadata() {
    val notificationItem =
      NotificationItem()
        .withNotificationType(listOf(Notification.NotificationType.SLACK, Notification.NotificationType.CUSTOMERIO))
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
          .withNotifications(listOf(slackNotification))
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
        .withNotificationType(listOf(Notification.NotificationType.SLACK))
        .withSlackConfiguration(
          SlackNotificationConfiguration()
            .withWebhook("http://random.webhook.url/hooks.slack.com/"),
        )

    private fun noNotificationItem(): NotificationItem =
      NotificationItem()
        .withNotificationType(listOf())

    private fun customerioNotificationItem(): NotificationItem =
      NotificationItem()
        .withNotificationType(listOf(Notification.NotificationType.CUSTOMERIO))

    private fun customerioAndSlackNotificationItem(): NotificationItem =
      NotificationItem()
        .withNotificationType(listOf(Notification.NotificationType.SLACK, Notification.NotificationType.CUSTOMERIO))
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
