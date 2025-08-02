/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.collect.Lists
import io.airbyte.analytics.TrackingClient
import io.airbyte.api.model.generated.ActorListCursorPaginatedRequestBody
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionReadList
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationReadList
import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody
import io.airbyte.api.model.generated.NotificationConfig
import io.airbyte.api.model.generated.NotificationType
import io.airbyte.api.model.generated.NotificationsConfig
import io.airbyte.api.model.generated.Pagination
import io.airbyte.api.model.generated.SlugRequestBody
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.SourceReadList
import io.airbyte.api.model.generated.WebhookConfigRead
import io.airbyte.api.model.generated.WebhookConfigWrite
import io.airbyte.api.model.generated.WebhookNotificationConfig
import io.airbyte.api.model.generated.WorkspaceCreate
import io.airbyte.api.model.generated.WorkspaceCreateWithId
import io.airbyte.api.model.generated.WorkspaceGiveFeedback
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.api.model.generated.WorkspaceUpdate
import io.airbyte.api.model.generated.WorkspaceUpdateName
import io.airbyte.api.model.generated.WorkspaceUpdateOrganization
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.server.converters.NotificationConverter.toApiList
import io.airbyte.commons.server.converters.NotificationSettingsConverter.toApi
import io.airbyte.commons.server.limits.ConsumptionService
import io.airbyte.commons.server.limits.ProductLimitsProvider
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.CustomerioNotificationConfiguration
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.Notification
import io.airbyte.config.NotificationItem
import io.airbyte.config.NotificationSettings
import io.airbyte.config.Organization
import io.airbyte.config.SlackNotificationConfiguration
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.WebhookConfig
import io.airbyte.config.WebhookOperationConfigs
import io.airbyte.config.helpers.patchNotificationSettingsWithDefaultValue
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ResourcesByOrganizationQueryPaginated
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.validation.json.JsonValidationException
import jakarta.validation.Valid
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.argumentCaptor
import java.io.IOException
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

internal class WorkspacesHandlerTest {
  lateinit var connectionsHandler: ConnectionsHandler
  lateinit var destinationHandler: DestinationHandler
  lateinit var sourceHandler: SourceHandler
  lateinit var uuidSupplier: Supplier<UUID>
  lateinit var workspace: StandardWorkspace
  lateinit var secretPersistence: SecretPersistence

  lateinit var permissionHandler: PermissionHandler
  lateinit var workspacePersistence: WorkspacePersistence
  lateinit var workspaceService: WorkspaceService
  lateinit var dataplaneGroupService: DataplaneGroupService
  lateinit var organizationPersistence: OrganizationPersistence
  lateinit var trackingClient: TrackingClient
  lateinit var limitsProvider: ProductLimitsProvider
  lateinit var consumptionService: ConsumptionService
  lateinit var ffClient: FeatureFlagClient

  @BeforeEach
  fun setUp() {
    workspacePersistence = Mockito.mock(WorkspacePersistence::class.java)
    organizationPersistence = Mockito.mock(OrganizationPersistence::class.java)
    secretPersistence = Mockito.mock(SecretPersistence::class.java)
    permissionHandler = Mockito.mock(PermissionHandler::class.java)
    connectionsHandler = Mockito.mock(ConnectionsHandler::class.java)
    destinationHandler = Mockito.mock(DestinationHandler::class.java)
    sourceHandler = Mockito.mock(SourceHandler::class.java)
    uuidSupplier = Mockito.mock(Supplier::class.java) as Supplier<UUID>
    workspaceService = Mockito.mock(WorkspaceService::class.java)
    dataplaneGroupService = Mockito.mock(DataplaneGroupService::class.java)
    trackingClient = Mockito.mock(TrackingClient::class.java)
    limitsProvider = Mockito.mock(ProductLimitsProvider::class.java)
    consumptionService = Mockito.mock(ConsumptionService::class.java)
    ffClient = Mockito.mock(TestClient::class.java)

    workspace = generateWorkspace()
  }

  private fun getWorkspacesHandler(airbyteEdition: AirbyteEdition): WorkspacesHandler =
    WorkspacesHandler(
      workspacePersistence,
      organizationPersistence,
      permissionHandler,
      connectionsHandler,
      destinationHandler,
      sourceHandler,
      uuidSupplier,
      workspaceService,
      dataplaneGroupService,
      trackingClient,
      limitsProvider,
      consumptionService,
      ffClient,
      airbyteEdition,
    )

  private fun generateWorkspace(): StandardWorkspace =
    StandardWorkspace()
      .withWorkspaceId(UUID.randomUUID())
      .withCustomerId(UUID.randomUUID())
      .withEmail(TEST_EMAIL)
      .withName(TEST_WORKSPACE_NAME)
      .withSlug(TEST_WORKSPACE_SLUG)
      .withInitialSetupComplete(false)
      .withDisplaySetupWizard(true)
      .withNews(false)
      .withAnonymousDataCollection(false)
      .withSecurityUpdates(false)
      .withTombstone(false)
      .withNotifications(listOf<Notification?>(generateNotification()))
      .withNotificationSettings(generateNotificationSettings())
      .withDataplaneGroupId(DATAPLANE_GROUP_ID_1)
      .withOrganizationId(ORGANIZATION_ID)

  private fun generateOrganization(ssoRealm: String?): Organization =
    Organization()
      .withOrganizationId(ORGANIZATION_ID)
      .withName(TEST_ORGANIZATION_NAME)
      .withEmail(TEST_EMAIL)
      .withSsoRealm(ssoRealm)

  private fun generateNotification(): Notification =
    Notification()
      .withNotificationType(Notification.NotificationType.SLACK)
      .withSlackConfiguration(
        SlackNotificationConfiguration()
          .withWebhook(FAILURE_NOTIFICATION_WEBHOOK),
      )

  private fun generateNotificationSettings(): NotificationSettings =
    patchNotificationSettingsWithDefaultValue(
      NotificationSettings()
        .withSendOnFailure(
          NotificationItem()
            .withNotificationType(
              ArrayList(listOf<Notification.NotificationType?>(Notification.NotificationType.SLACK)),
            ).withSlackConfiguration(
              SlackNotificationConfiguration()
                .withWebhook(FAILURE_NOTIFICATION_WEBHOOK),
            ),
        ),
    )

  private fun generateApiNotification(): io.airbyte.api.model.generated.Notification =
    io.airbyte.api.model.generated
      .Notification()
      .notificationType(NotificationType.SLACK)
      .slackConfiguration(
        io.airbyte.api.model.generated
          .SlackNotificationConfiguration()
          .webhook(FAILURE_NOTIFICATION_WEBHOOK),
      )

  private fun generateApiNotificationSettings(): io.airbyte.api.model.generated.NotificationSettings? =
    io.airbyte.api.model.generated
      .NotificationSettings()
      .sendOnFailure(
        io.airbyte.api.model.generated
          .NotificationItem()
          .notificationType(listOf<NotificationType?>(NotificationType.SLACK))
          .slackConfiguration(
            io.airbyte.api.model.generated
              .SlackNotificationConfiguration()
              .webhook(FAILURE_NOTIFICATION_WEBHOOK),
          ),
      )

  private fun generateApiNotificationSettingsWithDefaultValue(): io.airbyte.api.model.generated.NotificationSettings? =
    io.airbyte.api.model.generated
      .NotificationSettings()
      .sendOnFailure(
        io.airbyte.api.model.generated
          .NotificationItem()
          .notificationType(listOf<NotificationType?>(NotificationType.SLACK))
          .slackConfiguration(
            io.airbyte.api.model.generated
              .SlackNotificationConfiguration()
              .webhook(FAILURE_NOTIFICATION_WEBHOOK),
          ),
      ).sendOnSuccess(
        io.airbyte.api.model.generated
          .NotificationItem()
          .notificationType(mutableListOf<NotificationType?>()),
      ).sendOnConnectionUpdate(
        io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
          NotificationType.CUSTOMERIO,
        ),
      ).sendOnConnectionUpdateActionRequired(
        io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
          NotificationType.CUSTOMERIO,
        ),
      ).sendOnSyncDisabled(
        io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
          NotificationType.CUSTOMERIO,
        ),
      ).sendOnSyncDisabledWarning(
        io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
          NotificationType.CUSTOMERIO,
        ),
      ).sendOnBreakingChangeWarning(
        io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
          NotificationType.CUSTOMERIO,
        ),
      ).sendOnBreakingChangeSyncsDisabled(
        io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
          NotificationType.CUSTOMERIO,
        ),
      )

  private fun generateDefaultApiNotificationSettings(): io.airbyte.api.model.generated.NotificationSettings? =
    io.airbyte.api.model.generated
      .NotificationSettings()
      .sendOnSuccess(
        io.airbyte.api.model.generated
          .NotificationItem()
          .notificationType(mutableListOf<NotificationType?>()),
      ).sendOnFailure(
        io.airbyte.api.model.generated
          .NotificationItem()
          .notificationType(listOf<NotificationType?>(NotificationType.CUSTOMERIO)),
      ).sendOnConnectionUpdate(
        io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
          NotificationType.CUSTOMERIO,
        ),
      ).sendOnConnectionUpdateActionRequired(
        io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
          NotificationType.CUSTOMERIO,
        ),
      ).sendOnSyncDisabled(
        io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
          NotificationType.CUSTOMERIO,
        ),
      ).sendOnSyncDisabledWarning(
        io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
          NotificationType.CUSTOMERIO,
        ),
      ).sendOnBreakingChangeWarning(
        io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
          NotificationType.CUSTOMERIO,
        ),
      ).sendOnBreakingChangeSyncsDisabled(
        io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
          NotificationType.CUSTOMERIO,
        ),
      )

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testCreateWorkspace(airbyteEdition: AirbyteEdition) {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
    Mockito
      .`when`(
        workspaceService.getStandardWorkspaceNoSecrets(
          anyOrNull(),
          eq(false),
        ),
      ).thenReturn(workspace)

    val uuid = UUID.randomUUID()
    Mockito.`when`(uuidSupplier.get()).thenReturn(uuid)

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val workspaceCreate =
      WorkspaceCreate()
        .name(NEW_WORKSPACE)
        .email(TEST_EMAIL)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettings())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .webhookConfigs(listOf<@Valid WebhookConfigWrite?>(WebhookConfigWrite().name(TEST_NAME).authToken(TEST_AUTH_TOKEN)))
        .organizationId(ORGANIZATION_ID)

    Mockito.`when`(secretPersistence.read(anyOrNull())).thenReturn("")

    val actualRead = getWorkspacesHandler(airbyteEdition).createWorkspace(workspaceCreate)
    val expectedRead =
      WorkspaceRead()
        .workspaceId(uuid)
        .customerId(uuid)
        .email(TEST_EMAIL)
        .name(NEW_WORKSPACE)
        .slug(NEW_WORKSPACE_SLUG)
        .initialSetupComplete(false)
        .displaySetupWizard(false)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .webhookConfigs(listOf<@Valid WebhookConfigRead?>(WebhookConfigRead().id(uuid).name(TEST_NAME)))
        .organizationId(ORGANIZATION_ID)
        .tombstone(false)

    Assertions.assertEquals(expectedRead, actualRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testCreateWorkspaceIfNotExist(airbyteEdition: AirbyteEdition) {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
    Mockito
      .`when`(
        workspaceService.getStandardWorkspaceNoSecrets(
          anyOrNull(),
          eq(false),
        ),
      ).thenReturn(workspace)

    val uuid = UUID.randomUUID()
    Mockito.`when`(uuidSupplier.get()).thenReturn(uuid)

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val notSuppliedUUID = UUID.randomUUID()

    val workspaceCreateWithId =
      WorkspaceCreateWithId()
        .id(notSuppliedUUID)
        .name(NEW_WORKSPACE)
        .email(TEST_EMAIL)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettings())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .webhookConfigs(listOf<@Valid WebhookConfigWrite?>(WebhookConfigWrite().name(TEST_NAME).authToken(TEST_AUTH_TOKEN)))
        .organizationId(ORGANIZATION_ID)

    Mockito.`when`(secretPersistence.read(anyOrNull())).thenReturn("")

    val actualRead = getWorkspacesHandler(airbyteEdition).createWorkspaceIfNotExist(workspaceCreateWithId)
    val secondActualRead = getWorkspacesHandler(airbyteEdition).createWorkspaceIfNotExist(workspaceCreateWithId)
    val expectedRead =
      WorkspaceRead()
        .workspaceId(notSuppliedUUID)
        .customerId(uuid)
        .email(TEST_EMAIL)
        .name(NEW_WORKSPACE)
        .slug(NEW_WORKSPACE_SLUG)
        .initialSetupComplete(false)
        .displaySetupWizard(false)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .webhookConfigs(listOf<@Valid WebhookConfigRead?>(WebhookConfigRead().id(uuid).name(TEST_NAME)))
        .organizationId(ORGANIZATION_ID)
        .tombstone(false)

    Assertions.assertEquals(expectedRead, actualRead)
    Assertions.assertEquals(expectedRead, secondActualRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testCreateWorkspaceWithMinimumInput(airbyteEdition: AirbyteEdition) {
    Mockito
      .`when`(
        workspaceService.getStandardWorkspaceNoSecrets(
          anyOrNull(),
          eq(false),
        ),
      ).thenReturn(workspace)
    Mockito.`when`(dataplaneGroupService.getDefaultDataplaneGroupForAirbyteEdition(airbyteEdition)).thenReturn(
      DataplaneGroup().withId(DATAPLANE_GROUP_ID_1),
    )

    val uuid = UUID.randomUUID()
    Mockito.`when`(uuidSupplier.get()).thenReturn(uuid)

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val workspaceCreate =
      WorkspaceCreate()
        .name(NEW_WORKSPACE)
        .email(TEST_EMAIL)
        .organizationId(ORGANIZATION_ID)

    val actualRead = getWorkspacesHandler(airbyteEdition).createWorkspace(workspaceCreate)

    val expectedRead =
      WorkspaceRead()
        .workspaceId(actualRead.getWorkspaceId())
        .customerId(actualRead.getCustomerId())
        .email(TEST_EMAIL)
        .name(NEW_WORKSPACE)
        .slug(actualRead.getSlug())
        .initialSetupComplete(false)
        .displaySetupWizard(false)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(mutableListOf<@Valid io.airbyte.api.model.generated.Notification?>())
        .notificationSettings(generateDefaultApiNotificationSettings())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .webhookConfigs(mutableListOf<@Valid WebhookConfigRead?>())
        .tombstone(false)
        .organizationId(ORGANIZATION_ID)

    Assertions.assertEquals(expectedRead, actualRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testCreateWorkspaceDuplicateSlug(airbyteEdition: AirbyteEdition) {
    Mockito
      .`when`(
        workspaceService.getWorkspaceBySlugOptional(
          any(),
          eq(true),
        ),
      ).thenReturn(Optional.of<StandardWorkspace>(workspace))
      .thenReturn(Optional.of<StandardWorkspace>(workspace))
      .thenReturn(Optional.empty<StandardWorkspace>())
    Mockito
      .`when`(
        workspaceService.getStandardWorkspaceNoSecrets(
          anyOrNull(),
          eq(false),
        ),
      ).thenReturn(workspace)

    val dataplaneGroupId = UUID.randomUUID()
    Mockito.`when`(dataplaneGroupService.getDefaultDataplaneGroupForAirbyteEdition(airbyteEdition)).thenReturn(
      DataplaneGroup().withId(dataplaneGroupId),
    )

    val uuid = UUID.randomUUID()
    Mockito.`when`(uuidSupplier.get()).thenReturn(uuid)

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val workspaceCreate =
      WorkspaceCreate()
        .name(workspace.getName())
        .email(TEST_EMAIL)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(mutableListOf<@Valid io.airbyte.api.model.generated.Notification?>())
        .organizationId(ORGANIZATION_ID)

    val actualRead = getWorkspacesHandler(airbyteEdition).createWorkspace(workspaceCreate)
    val expectedRead =
      WorkspaceRead()
        .workspaceId(uuid)
        .customerId(uuid)
        .email(TEST_EMAIL)
        .name(workspace.getName())
        .slug(workspace.getSlug())
        .initialSetupComplete(false)
        .displaySetupWizard(false)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(mutableListOf<@Valid io.airbyte.api.model.generated.Notification?>())
        .notificationSettings(generateDefaultApiNotificationSettings())
        .dataplaneGroupId(dataplaneGroupId)
        .webhookConfigs(mutableListOf<@Valid WebhookConfigRead?>())
        .tombstone(false)
        .organizationId(ORGANIZATION_ID)

    Assertions.assertTrue(actualRead.getSlug().startsWith(workspace.getSlug()))
    Assertions.assertNotEquals(workspace.getSlug(), actualRead.getSlug())
    Assertions.assertEquals(clone<WorkspaceRead>(expectedRead).slug(null), clone(actualRead).slug(null))
    val slugCaptor = argumentCaptor<String>()
    Mockito.verify(workspaceService, Mockito.times(3)).getWorkspaceBySlugOptional(slugCaptor.capture(), eq(true))
    Assertions.assertEquals(3, slugCaptor.allValues.size)
    Assertions.assertEquals(workspace.getSlug(), slugCaptor.allValues[0])
    Assertions.assertTrue(slugCaptor.allValues[1].startsWith(workspace.getSlug()))
    Assertions.assertTrue(slugCaptor.allValues[2].startsWith(workspace.getSlug()))
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testDeleteWorkspace() {
    val workspaceIdRequestBody = WorkspaceIdRequestBody().workspaceId(workspace.getWorkspaceId())

    val connection = ConnectionRead().connectionId(UUID.randomUUID())
    val destination = DestinationRead()
    val source = SourceRead()

    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
      .thenReturn(workspace)

    Mockito.`when`(workspaceService.listStandardWorkspaces(false)).thenReturn(
      mutableListOf(
        workspace,
      ),
    )

    Mockito
      .`when`(connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody))
      .thenReturn(ConnectionReadList().connections(mutableListOf<@Valid ConnectionRead?>(connection)))

    Mockito
      .`when`(
        destinationHandler
          .listDestinationsForWorkspace(ActorListCursorPaginatedRequestBody().workspaceId(workspaceIdRequestBody.getWorkspaceId())),
      ).thenReturn(DestinationReadList().destinations(mutableListOf<@Valid DestinationRead?>(destination)))

    Mockito
      .`when`(
        sourceHandler.listSourcesForWorkspace(
          ActorListCursorPaginatedRequestBody().workspaceId(
            workspaceIdRequestBody.getWorkspaceId(),
          ),
        ),
      ).thenReturn(SourceReadList().sources(mutableListOf<@Valid SourceRead?>(source)))

    getWorkspacesHandler(AirbyteEdition.COMMUNITY).deleteWorkspace(workspaceIdRequestBody)

    Mockito.verify(connectionsHandler).deleteConnection(connection.getConnectionId())
    Mockito.verify(destinationHandler).deleteDestination(destination)
    Mockito.verify(sourceHandler).deleteSource(source)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testListWorkspaces() {
    val workspace2 = generateWorkspace()

    Mockito.`when`(workspaceService.listStandardWorkspaces(false)).thenReturn(
      Lists.newArrayList(
        workspace,
        workspace2,
      ),
    )

    val expectedWorkspaceRead1 =
      WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(workspace.getEmail())
        .name(workspace.getName())
        .slug(workspace.getSlug())
        .initialSetupComplete(workspace.getInitialSetupComplete())
        .displaySetupWizard(workspace.getDisplaySetupWizard())
        .news(workspace.getNews())
        .anonymousDataCollection(workspace.getAnonymousDataCollection())
        .securityUpdates(workspace.getSecurityUpdates())
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .organizationId(ORGANIZATION_ID)
        .tombstone(false)

    val expectedWorkspaceRead2 =
      WorkspaceRead()
        .workspaceId(workspace2.getWorkspaceId())
        .customerId(workspace2.getCustomerId())
        .email(workspace2.getEmail())
        .name(workspace2.getName())
        .slug(workspace2.getSlug())
        .initialSetupComplete(workspace2.getInitialSetupComplete())
        .displaySetupWizard(workspace2.getDisplaySetupWizard())
        .news(workspace2.getNews())
        .anonymousDataCollection(workspace2.getAnonymousDataCollection())
        .securityUpdates(workspace2.getSecurityUpdates())
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .organizationId(ORGANIZATION_ID)
        .tombstone(false)

    val actualWorkspaceReadList = getWorkspacesHandler(AirbyteEdition.COMMUNITY).listWorkspaces()

    Assertions.assertEquals(
      Lists.newArrayList<WorkspaceRead?>(expectedWorkspaceRead1, expectedWorkspaceRead2),
      actualWorkspaceReadList.getWorkspaces(),
    )
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testGetWorkspace() {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
      .thenReturn(workspace)

    val workspaceIdRequestBody = WorkspaceIdRequestBody().workspaceId(workspace.getWorkspaceId())

    val workspaceRead =
      WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(TEST_EMAIL)
        .name(TEST_WORKSPACE_NAME)
        .slug(TEST_WORKSPACE_SLUG)
        .initialSetupComplete(false)
        .displaySetupWizard(true)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .webhookConfigs(listOf<@Valid WebhookConfigRead?>(WebhookConfigRead().id(WEBHOOK_CONFIG_ID).name(TEST_NAME)))
        .organizationId(ORGANIZATION_ID)
        .tombstone(false)

    Assertions.assertEquals(workspaceRead, getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspace(workspaceIdRequestBody))
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class)
  fun testGetWorkspaceBySlug() {
    Mockito.`when`(workspaceService.getWorkspaceBySlug("default", false)).thenReturn(workspace)

    val slugRequestBody = SlugRequestBody().slug("default")
    val workspaceRead = getWorkspaceReadPerWorkspace(workspace)

    Assertions.assertEquals(workspaceRead, getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceBySlug(slugRequestBody))
  }

  @Test
  @Throws(ConfigNotFoundException::class)
  fun testGetWorkspaceByConnectionId() {
    val connectionId = UUID.randomUUID()
    Mockito.`when`(workspaceService.getStandardWorkspaceFromConnection(connectionId, false)).thenReturn(workspace)
    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(connectionId)
    val workspaceRead = getWorkspaceReadPerWorkspace(workspace)

    Assertions.assertEquals(
      workspaceRead,
      getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceByConnectionId(connectionIdRequestBody, false),
    )
  }

  private fun getWorkspaceReadPerWorkspace(workspace: StandardWorkspace): WorkspaceRead =
    WorkspaceRead()
      .workspaceId(workspace.getWorkspaceId())
      .customerId(workspace.getCustomerId())
      .email(TEST_EMAIL)
      .name(workspace.getName())
      .slug(workspace.getSlug())
      .initialSetupComplete(workspace.getInitialSetupComplete())
      .displaySetupWizard(workspace.getDisplaySetupWizard())
      .news(workspace.getNews())
      .anonymousDataCollection(workspace.getAnonymousDataCollection())
      .securityUpdates(workspace.getSecurityUpdates())
      .notifications(toApiList(workspace.getNotifications()))
      .notificationSettings(toApi(workspace.getNotificationSettings()))
      .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
      .organizationId(ORGANIZATION_ID)
      .tombstone(workspace.getTombstone())

  @Test
  @Throws(ConfigNotFoundException::class)
  fun testGetWorkspaceByConnectionIdOnConfigNotFound() {
    val connectionId = UUID.randomUUID()
    Mockito
      .`when`(workspaceService.getStandardWorkspaceFromConnection(connectionId, false))
      .thenThrow(ConfigNotFoundException("something", connectionId.toString()))
    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(connectionId)
    Assertions.assertThrows(
      ConfigNotFoundException::class.java,
    ) { getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceByConnectionId(connectionIdRequestBody, false) }
  }

  @ParameterizedTest
  @CsvSource("true", "false")
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testGetWorkspaceOrganizationInfo(isSso: Boolean) {
    val organization = generateOrganization(if (isSso) "test-realm" else null)
    val workspaceIdRequestBody = WorkspaceIdRequestBody().workspaceId(workspace.getWorkspaceId())

    Mockito.`when`(organizationPersistence.getOrganizationByWorkspaceId(workspace.getWorkspaceId())).thenReturn(
      Optional.of<Organization>(organization),
    )

    val orgInfo =
      getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceOrganizationInfo(workspaceIdRequestBody)

    Assertions.assertEquals(organization.getOrganizationId(), orgInfo.getOrganizationId())
    Assertions.assertEquals(organization.getName(), orgInfo.getOrganizationName())
    Assertions.assertEquals(isSso, orgInfo.getSso()) // sso is true if ssoRealm is set
  }

  @Test
  @Throws(IOException::class)
  fun testGerWorkspaceOrganizationInfoConfigNotFound() {
    val workspaceIdRequestBody = WorkspaceIdRequestBody().workspaceId(workspace.getWorkspaceId())

    Mockito.`when`(organizationPersistence.getOrganizationByWorkspaceId(workspace.getWorkspaceId())).thenReturn(
      Optional.empty<Organization>(),
    )

    Assertions.assertThrows(
      ConfigNotFoundException::class.java,
    ) { getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceOrganizationInfo(workspaceIdRequestBody) }
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testUpdateWorkspace(airbyteEdition: AirbyteEdition) {
    val apiNotification = generateApiNotification()
    apiNotification.getSlackConfiguration().webhook(UPDATED)
    val workspaceUpdate =
      WorkspaceUpdate()
        .workspaceId(workspace.getWorkspaceId())
        .anonymousDataCollection(true)
        .securityUpdates(false)
        .news(false)
        .name(NEW_WORKSPACE)
        .initialSetupComplete(true)
        .displaySetupWizard(false)
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(apiNotification))
        .notificationSettings(generateApiNotificationSettings())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .webhookConfigs(listOf<@Valid WebhookConfigWrite?>(WebhookConfigWrite().name(TEST_NAME).authToken("test-auth-token")))

    val expectedNotification = generateNotification()
    expectedNotification.getSlackConfiguration().withWebhook(UPDATED)

    val expectedWorkspace =
      StandardWorkspace()
        .withWorkspaceId(workspace.getWorkspaceId())
        .withCustomerId(workspace.getCustomerId())
        .withEmail(TEST_EMAIL)
        .withName(NEW_WORKSPACE)
        .withSlug(NEW_WORKSPACE_SLUG)
        .withAnonymousDataCollection(true)
        .withSecurityUpdates(false)
        .withNews(false)
        .withInitialSetupComplete(true)
        .withDisplaySetupWizard(false)
        .withTombstone(false)
        .withNotifications(listOf<Notification?>(expectedNotification))
        .withNotificationSettings(generateNotificationSettings())
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
        .withOrganizationId(ORGANIZATION_ID)

    Mockito.`when`(uuidSupplier.get()).thenReturn(WEBHOOK_CONFIG_ID)

    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
      .thenReturn(workspace)
      .thenReturn(expectedWorkspace)

    Mockito.`when`(secretPersistence.read(anyOrNull())).thenReturn("")

    val actualWorkspaceRead = getWorkspacesHandler(airbyteEdition).updateWorkspace(workspaceUpdate)

    val expectedNotificationRead = generateApiNotification()
    expectedNotificationRead.getSlackConfiguration().webhook(UPDATED)

    val expectedWorkspaceRead =
      WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(TEST_EMAIL)
        .name(NEW_WORKSPACE)
        .slug(NEW_WORKSPACE_SLUG)
        .initialSetupComplete(true)
        .displaySetupWizard(false)
        .news(false)
        .anonymousDataCollection(true)
        .securityUpdates(false)
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(expectedNotificationRead))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .webhookConfigs(listOf<@Valid WebhookConfigRead?>(WebhookConfigRead().name(TEST_NAME).id(WEBHOOK_CONFIG_ID)))
        .organizationId(ORGANIZATION_ID)
        .tombstone(false)

    val expectedWorkspaceWithSecrets =
      StandardWorkspace()
        .withWorkspaceId(workspace.getWorkspaceId())
        .withCustomerId(workspace.getCustomerId())
        .withEmail(TEST_EMAIL)
        .withName(NEW_WORKSPACE)
        .withSlug(NEW_WORKSPACE_SLUG)
        .withAnonymousDataCollection(true)
        .withSecurityUpdates(false)
        .withNews(false)
        .withInitialSetupComplete(true)
        .withDisplaySetupWizard(false)
        .withTombstone(false)
        .withNotifications(listOf<Notification?>(expectedNotification))
        .withNotificationSettings(generateNotificationSettings())
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .withWebhookOperationConfigs(SECRET_WEBHOOK_CONFIGS)
        .withOrganizationId(ORGANIZATION_ID)
        .withTombstone(false)

    Mockito.verify(workspaceService).writeWorkspaceWithSecrets(expectedWorkspaceWithSecrets)

    Assertions.assertEquals(expectedWorkspaceRead, actualWorkspaceRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testUpdateWorkspaceWithoutWebhookConfigs(airbyteEdition: AirbyteEdition?) {
    val apiNotification = generateApiNotification()
    apiNotification.getSlackConfiguration().webhook(UPDATED)
    val workspaceUpdate =
      WorkspaceUpdate()
        .workspaceId(workspace.getWorkspaceId())
        .anonymousDataCollection(false)

    val expectedNotification = generateNotification()
    expectedNotification.getSlackConfiguration().withWebhook(UPDATED)
    val expectedWorkspace =
      StandardWorkspace()
        .withWorkspaceId(workspace.getWorkspaceId())
        .withCustomerId(workspace.getCustomerId())
        .withEmail(TEST_EMAIL)
        .withName(TEST_WORKSPACE_NAME)
        .withSlug(TEST_WORKSPACE_SLUG)
        .withAnonymousDataCollection(true)
        .withSecurityUpdates(false)
        .withNews(false)
        .withInitialSetupComplete(true)
        .withDisplaySetupWizard(false)
        .withTombstone(false)
        .withNotifications(listOf<Notification?>(expectedNotification))
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)

    Mockito.`when`(uuidSupplier.get()).thenReturn(WEBHOOK_CONFIG_ID)

    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
      .thenReturn(expectedWorkspace)
      .thenReturn(expectedWorkspace.withAnonymousDataCollection(false))

    getWorkspacesHandler(AirbyteEdition.COMMUNITY).updateWorkspace(workspaceUpdate)

    Mockito.verify(workspaceService).writeStandardWorkspaceNoSecrets(expectedWorkspace)
  }

  @Test
  @DisplayName("Updating workspace name should update name and slug")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testUpdateWorkspaceNoNameUpdate() {
    val workspaceUpdate =
      WorkspaceUpdateName()
        .workspaceId(workspace.getWorkspaceId())
        .name("New Workspace Name")

    val expectedWorkspace =
      StandardWorkspace()
        .withWorkspaceId(workspace.getWorkspaceId())
        .withCustomerId(workspace.getCustomerId())
        .withEmail(TEST_EMAIL)
        .withName("New Workspace Name")
        .withSlug("new-workspace-name")
        .withAnonymousDataCollection(workspace.getAnonymousDataCollection())
        .withSecurityUpdates(workspace.getSecurityUpdates())
        .withNews(workspace.getNews())
        .withInitialSetupComplete(workspace.getInitialSetupComplete())
        .withDisplaySetupWizard(workspace.getDisplaySetupWizard())
        .withTombstone(false)
        .withNotifications(workspace.getNotifications())
        .withNotificationSettings(workspace.getNotificationSettings())
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .withOrganizationId(ORGANIZATION_ID)

    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
      .thenReturn(workspace)
      .thenReturn(expectedWorkspace)

    val actualWorkspaceRead = getWorkspacesHandler(AirbyteEdition.COMMUNITY).updateWorkspaceName(workspaceUpdate)

    val expectedWorkspaceRead =
      WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(TEST_EMAIL)
        .name("New Workspace Name")
        .slug("new-workspace-name")
        .initialSetupComplete(workspace.getInitialSetupComplete())
        .displaySetupWizard(workspace.getDisplaySetupWizard())
        .news(workspace.getNews())
        .anonymousDataCollection(workspace.getAnonymousDataCollection())
        .securityUpdates(workspace.getSecurityUpdates())
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .organizationId(ORGANIZATION_ID)
        .tombstone(false)

    Mockito.verify(workspaceService).writeStandardWorkspaceNoSecrets(expectedWorkspace)

    Assertions.assertEquals(expectedWorkspaceRead, actualWorkspaceRead)
  }

  @Test
  @DisplayName("Update organization in workspace")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testWorkspaceUpdateOrganization() {
    val newOrgId = UUID.randomUUID()
    val workspaceUpdateOrganization =
      WorkspaceUpdateOrganization()
        .workspaceId(workspace.getWorkspaceId())
        .organizationId(newOrgId)
    val expectedWorkspace = clone(workspace).withOrganizationId(newOrgId)
    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
      .thenReturn(expectedWorkspace)
    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
      .thenReturn(expectedWorkspace)
    // The same as the original workspace, with only the organization ID updated.
    val expectedWorkspaceRead =
      WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(workspace.getEmail())
        .name(workspace.getName())
        .slug(workspace.getSlug())
        .initialSetupComplete(workspace.getInitialSetupComplete())
        .displaySetupWizard(workspace.getDisplaySetupWizard())
        .news(workspace.getNews())
        .anonymousDataCollection(false)
        .securityUpdates(workspace.getSecurityUpdates())
        .notifications(toApiList(workspace.getNotifications()))
        .notificationSettings(toApi(workspace.getNotificationSettings()))
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .organizationId(newOrgId)
        .tombstone(false)

    val actualWorkspaceRead =
      getWorkspacesHandler(AirbyteEdition.COMMUNITY).updateWorkspaceOrganization(workspaceUpdateOrganization)
    Mockito.verify(workspaceService).writeStandardWorkspaceNoSecrets(expectedWorkspace)
    Assertions.assertEquals(expectedWorkspaceRead, actualWorkspaceRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @DisplayName("Partial patch update should preserve unchanged fields")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testWorkspacePatchUpdate(airbyteEdition: AirbyteEdition) {
    val expectedNewEmail = "expected-new-email@example.com"
    val workspaceUpdate =
      WorkspaceUpdate()
        .workspaceId(workspace.getWorkspaceId())
        .anonymousDataCollection(true) // originally was DATAPLANE_GROUP_ID_1
        .dataplaneGroupId(DATAPLANE_GROUP_ID_2)
        .email(expectedNewEmail)

    val expectedWorkspace =
      clone(workspace)
        .withEmail(expectedNewEmail)
        .withAnonymousDataCollection(true)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_2)
        .withWebhookOperationConfigs(
          jsonNode<WebhookOperationConfigs?>(
            WebhookOperationConfigs()
              .withWebhookConfigs(mutableListOf<WebhookConfig?>()),
          ),
        )

    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
      .thenReturn(workspace)
      .thenReturn(expectedWorkspace)

    // The same as the original workspace, with only the email and data collection flags changed.
    val expectedWorkspaceRead =
      WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(expectedNewEmail)
        .name(workspace.getName())
        .slug(workspace.getSlug())
        .initialSetupComplete(workspace.getInitialSetupComplete())
        .displaySetupWizard(workspace.getDisplaySetupWizard())
        .news(workspace.getNews())
        .anonymousDataCollection(true)
        .securityUpdates(workspace.getSecurityUpdates())
        .notifications(toApiList(workspace.getNotifications()))
        .notificationSettings(toApi(workspace.getNotificationSettings()))
        .dataplaneGroupId(DATAPLANE_GROUP_ID_2)
        .organizationId(ORGANIZATION_ID)
        .webhookConfigs(mutableListOf<@Valid WebhookConfigRead?>())
        .tombstone(false)

    val actualWorkspaceRead = getWorkspacesHandler(airbyteEdition).updateWorkspace(workspaceUpdate)
    Mockito.verify(workspaceService).writeStandardWorkspaceNoSecrets(expectedWorkspace)
    Assertions.assertEquals(expectedWorkspaceRead, actualWorkspaceRead)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testWorkspacePatchUpdateWithPublicNotificationConfig() {
    // This is the workspace that exists before the update. It has a customerio notification for both
    // success and failure, and a webhook ("slack") notification for failure.
    //
    // This test shows that the notifications can be patched, and leave the existing settings in place.

    val existingWorkspace = clone(workspace)
    existingWorkspace
      .getNotificationSettings()
      .withSendOnSuccess(
        NotificationItem()
          .withCustomerioConfiguration(CustomerioNotificationConfiguration())
          .withNotificationType(
            ArrayList(listOf<Notification.NotificationType?>(Notification.NotificationType.CUSTOMERIO)),
          ),
      ).withSendOnFailure(
        NotificationItem()
          .withSlackConfiguration(SlackNotificationConfiguration().withWebhook("http://foo.bar/failure"))
          .withCustomerioConfiguration(CustomerioNotificationConfiguration())
          .withNotificationType(
            ArrayList(
              listOf<Notification.NotificationType?>(
                Notification.NotificationType.CUSTOMERIO,
                Notification.NotificationType.SLACK,
              ),
            ),
          ),
      )

    // The update from the public API adds a webhook ("slack") notification for success,
    // and disables the webhook ("slack") notification for failure.
    val workspaceUpdate =
      WorkspaceUpdate()
        .workspaceId(existingWorkspace.getWorkspaceId())
        .notificationsConfig(
          NotificationsConfig()
            .success(
              NotificationConfig()
                .webhook(WebhookNotificationConfig().enabled(true).url("http://foo.bar/success")),
            ).failure(
              NotificationConfig()
                .webhook(WebhookNotificationConfig().enabled(false)),
            ),
        )

    val expectedWorkspace = clone(existingWorkspace)
    expectedWorkspace
      .getNotificationSettings()
      .withSendOnSuccess(
        NotificationItem()
          .withSlackConfiguration(SlackNotificationConfiguration().withWebhook("http://foo.bar/success"))
          .withCustomerioConfiguration(CustomerioNotificationConfiguration())
          .withNotificationType(
            listOf<Notification.NotificationType?>(
              Notification.NotificationType.CUSTOMERIO,
              Notification.NotificationType.SLACK,
            ),
          ),
      ).withSendOnFailure(
        NotificationItem()
          .withSlackConfiguration(SlackNotificationConfiguration().withWebhook("http://foo.bar/failure"))
          .withCustomerioConfiguration(CustomerioNotificationConfiguration())
          .withNotificationType(listOf<Notification.NotificationType?>(Notification.NotificationType.CUSTOMERIO)),
      )

    expectedWorkspace.withWebhookOperationConfigs(
      jsonNode<WebhookOperationConfigs?>(
        WebhookOperationConfigs()
          .withWebhookConfigs(mutableListOf<WebhookConfig?>()),
      ),
    )

    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
      .thenReturn(existingWorkspace)
      .thenReturn(expectedWorkspace)

    getWorkspacesHandler(AirbyteEdition.COMMUNITY).updateWorkspace(workspaceUpdate)
    Mockito.verify(workspaceService).writeStandardWorkspaceNoSecrets(expectedWorkspace)
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class)
  fun testSetFeedbackDone() {
    val workspaceGiveFeedback =
      WorkspaceGiveFeedback()
        .workspaceId(UUID.randomUUID())

    getWorkspacesHandler(AirbyteEdition.COMMUNITY).setFeedbackDone(workspaceGiveFeedback)

    Mockito.verify(workspaceService).setFeedback(workspaceGiveFeedback.getWorkspaceId())
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testWorkspaceIsWrittenThroughSecretsWriter(airbyteEdition: AirbyteEdition) {
    val workspacesHandler =
      WorkspacesHandler(
        workspacePersistence,
        organizationPersistence,
        permissionHandler,
        connectionsHandler,
        destinationHandler,
        sourceHandler,
        uuidSupplier,
        workspaceService,
        dataplaneGroupService,
        trackingClient,
        limitsProvider,
        consumptionService,
        ffClient,
        airbyteEdition,
      )

    val uuid = UUID.randomUUID()
    Mockito.`when`(uuidSupplier.get()).thenReturn(uuid)

    val workspaceCreate =
      WorkspaceCreate()
        .name(NEW_WORKSPACE)
        .email(TEST_EMAIL)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettings())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .organizationId(ORGANIZATION_ID)

    val actualRead = workspacesHandler.createWorkspace(workspaceCreate)
    val expectedRead =
      WorkspaceRead()
        .workspaceId(uuid)
        .customerId(uuid)
        .email(TEST_EMAIL)
        .name(NEW_WORKSPACE)
        .slug(NEW_WORKSPACE_SLUG)
        .initialSetupComplete(false)
        .displaySetupWizard(false)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .webhookConfigs(mutableListOf<@Valid WebhookConfigRead?>())
        .tombstone(false)
        .organizationId(ORGANIZATION_ID)

    Assertions.assertEquals(expectedRead, actualRead)
    Mockito.verify(workspaceService, Mockito.times(1)).writeWorkspaceWithSecrets(anyOrNull())
  }

  @Test
  @Throws(Exception::class)
  fun testListWorkspacesInOrgNoKeyword() {
    val request =
      ListWorkspacesInOrganizationRequestBody().organizationId(ORGANIZATION_ID).pagination(Pagination().pageSize(100).rowOffset(0))
    val expectedWorkspaces = listOf(generateWorkspace(), generateWorkspace())

    Mockito
      .`when`(
        workspacePersistence.listWorkspacesByOrganizationIdPaginated(
          ResourcesByOrganizationQueryPaginated(ORGANIZATION_ID, false, 100, 0),
          Optional.empty<String>(),
        ),
      ).thenReturn(expectedWorkspaces)
    val result = getWorkspacesHandler(AirbyteEdition.COMMUNITY).listWorkspacesInOrganization(request)
    Assertions.assertEquals(2, result.getWorkspaces().size)
  }

  @Test
  @Throws(Exception::class)
  fun testListWorkspacesInOrgWithKeyword() {
    val request =
      ListWorkspacesInOrganizationRequestBody()
        .organizationId(ORGANIZATION_ID)
        .nameContains("nameContains")
        .pagination(Pagination().pageSize(100).rowOffset(0))
    val expectedWorkspaces = listOf(generateWorkspace(), generateWorkspace())

    Mockito
      .`when`(
        workspacePersistence.listWorkspacesByOrganizationIdPaginated(
          ResourcesByOrganizationQueryPaginated(ORGANIZATION_ID, false, 100, 0),
          Optional.of<String>("nameContains"),
        ),
      ).thenReturn(expectedWorkspaces)
    val result = getWorkspacesHandler(AirbyteEdition.COMMUNITY).listWorkspacesInOrganization(request)
    Assertions.assertEquals(2, result.getWorkspaces().size)
  }

  companion object {
    const val UPDATED: String = "updated"
    private const val FAILURE_NOTIFICATION_WEBHOOK = "http://airbyte.notifications/failure"
    private const val NEW_WORKSPACE = "new workspace"
    private const val NEW_WORKSPACE_SLUG = "new-workspace"
    private const val TEST_NAME = "test-name"
    private val ORGANIZATION_ID: UUID = UUID.randomUUID()
    private const val TEST_AUTH_TOKEN = "test-auth-token"
    private val WEBHOOK_CONFIG_ID: UUID = UUID.randomUUID()
    private val PERSISTED_WEBHOOK_CONFIGS =
      deserialize(
        String.format(
          "{\"webhookConfigs\": [{\"id\": \"%s\", \"name\": \"%s\", \"authToken\": {\"_secret\": \"a-secret_v1\"}}]}",
          WEBHOOK_CONFIG_ID,
          TEST_NAME,
        ),
      )

    private val SECRET_WEBHOOK_CONFIGS =
      deserialize(
        String.format(
          "{\"webhookConfigs\": [{\"id\": \"%s\", \"name\": \"%s\", \"authToken\": \"%s\"}]}",
          WEBHOOK_CONFIG_ID,
          TEST_NAME,
          TEST_AUTH_TOKEN,
        ),
      )
    private const val TEST_EMAIL = "test@airbyte.io"
    private const val TEST_WORKSPACE_NAME = "test workspace"
    private const val TEST_WORKSPACE_SLUG = "test-workspace"
    private const val TEST_ORGANIZATION_NAME = "test organization"
    private val DATAPLANE_GROUP_ID_1: UUID = UUID.randomUUID()
    private val DATAPLANE_GROUP_ID_2: UUID = UUID.randomUUID()
  }
}
