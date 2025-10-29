/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

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
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.server.authorization.RoleResolver
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
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ResourcesByOrganizationQueryPaginated
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.validation.json.JsonValidationException
import jakarta.validation.Valid
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
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
  lateinit var organizationService: OrganizationService
  lateinit var trackingClient: TrackingClient
  lateinit var limitsProvider: ProductLimitsProvider
  lateinit var consumptionService: ConsumptionService
  lateinit var ffClient: FeatureFlagClient
  lateinit var roleResolver: RoleResolver
  lateinit var roleRequest: RoleResolver.Request

  @BeforeEach
  fun setUp() {
    workspacePersistence = Mockito.mock(WorkspacePersistence::class.java)
    organizationService = Mockito.mock(OrganizationService::class.java)
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
    roleResolver = Mockito.mock(RoleResolver::class.java)
    roleRequest = Mockito.mock(RoleResolver.Request::class.java)

    // Setup roleResolver mock chain
    Mockito.`when`(roleResolver.newRequest()).thenReturn(roleRequest)
    Mockito.`when`(roleRequest.withCurrentUser()).thenReturn(roleRequest)
    Mockito.`when`(roleRequest.withOrg(any())).thenReturn(roleRequest)
    Mockito.doNothing().`when`(roleRequest).requireRole(any())

    workspace = generateWorkspace()
  }

  private fun getWorkspacesHandler(airbyteEdition: AirbyteEdition): WorkspacesHandler =
    WorkspacesHandler(
      workspacePersistence,
      organizationService,
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
      roleResolver,
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

    // Mock dataplane group service for DATAPLANE_GROUP_ID_1
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    Mockito.`when`(dataplaneGroupService.listDefaultDataplaneGroups()).thenReturn(defaultDataplaneGroups)

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

    assertEquals(expectedRead, actualRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
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

    // Mock dataplane group service for DATAPLANE_GROUP_ID_1
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    Mockito.`when`(dataplaneGroupService.listDefaultDataplaneGroups()).thenReturn(defaultDataplaneGroups)

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

    assertEquals(expectedRead, actualRead)
    assertEquals(expectedRead, secondActualRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  fun testCreateWorkspaceWithMinimumInput(airbyteEdition: AirbyteEdition) {
    Mockito
      .`when`(
        workspaceService.getStandardWorkspaceNoSecrets(
          anyOrNull(),
          eq(false),
        ),
      ).thenReturn(workspace)
    Mockito.`when`(dataplaneGroupService.getDefaultDataplaneGroup()).thenReturn(
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
        .workspaceId(actualRead.workspaceId)
        .customerId(actualRead.customerId)
        .email(TEST_EMAIL)
        .name(NEW_WORKSPACE)
        .slug(actualRead.slug)
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

    assertEquals(expectedRead, actualRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
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
    Mockito.`when`(dataplaneGroupService.getDefaultDataplaneGroup()).thenReturn(
      DataplaneGroup().withId(dataplaneGroupId),
    )

    val uuid = UUID.randomUUID()
    Mockito.`when`(uuidSupplier.get()).thenReturn(uuid)

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val workspaceCreate =
      WorkspaceCreate()
        .name(workspace.name)
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
        .name(workspace.name)
        .slug(workspace.slug)
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

    assertTrue(actualRead.slug.startsWith(workspace.slug))
    assertNotEquals(workspace.slug, actualRead.slug)
    assertEquals(clone<WorkspaceRead>(expectedRead).slug(null), clone(actualRead).slug(null))
    val slugCaptor = argumentCaptor<String>()
    Mockito.verify(workspaceService, Mockito.times(3)).getWorkspaceBySlugOptional(slugCaptor.capture(), eq(true))
    assertEquals(3, slugCaptor.allValues.size)
    assertEquals(workspace.slug, slugCaptor.allValues[0])
    assertTrue(slugCaptor.allValues[1].startsWith(workspace.slug))
    assertTrue(slugCaptor.allValues[2].startsWith(workspace.slug))
  }

  @Test
  fun testDeleteWorkspace() {
    val workspaceIdRequestBody = WorkspaceIdRequestBody().workspaceId(workspace.workspaceId)

    val connection = ConnectionRead().connectionId(UUID.randomUUID())
    val destination = DestinationRead()
    val source = SourceRead()

    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false))
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
          .listDestinationsForWorkspace(ActorListCursorPaginatedRequestBody().workspaceId(workspaceIdRequestBody.workspaceId)),
      ).thenReturn(DestinationReadList().destinations(mutableListOf<@Valid DestinationRead?>(destination)))

    Mockito
      .`when`(
        sourceHandler.listSourcesForWorkspace(
          ActorListCursorPaginatedRequestBody().workspaceId(
            workspaceIdRequestBody.workspaceId,
          ),
        ),
      ).thenReturn(SourceReadList().sources(mutableListOf<@Valid SourceRead?>(source)))

    getWorkspacesHandler(AirbyteEdition.COMMUNITY).deleteWorkspace(workspaceIdRequestBody)

    Mockito.verify(connectionsHandler).deleteConnection(connection.connectionId)
    Mockito.verify(destinationHandler).deleteDestination(destination)
    Mockito.verify(sourceHandler).deleteSource(source)
  }

  @Test
  fun testListWorkspaces() {
    val workspace2 = generateWorkspace()

    Mockito.`when`(workspaceService.listStandardWorkspaces(false)).thenReturn(
      listOf(
        workspace,
        workspace2,
      ),
    )

    val expectedWorkspaceRead1 =
      WorkspaceRead()
        .workspaceId(workspace.workspaceId)
        .customerId(workspace.customerId)
        .email(workspace.email)
        .name(workspace.name)
        .slug(workspace.slug)
        .initialSetupComplete(workspace.initialSetupComplete)
        .displaySetupWizard(workspace.displaySetupWizard)
        .news(workspace.news)
        .anonymousDataCollection(workspace.anonymousDataCollection)
        .securityUpdates(workspace.securityUpdates)
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .organizationId(ORGANIZATION_ID)
        .tombstone(false)

    val expectedWorkspaceRead2 =
      WorkspaceRead()
        .workspaceId(workspace2.workspaceId)
        .customerId(workspace2.customerId)
        .email(workspace2.email)
        .name(workspace2.name)
        .slug(workspace2.slug)
        .initialSetupComplete(workspace2.initialSetupComplete)
        .displaySetupWizard(workspace2.displaySetupWizard)
        .news(workspace2.news)
        .anonymousDataCollection(workspace2.anonymousDataCollection)
        .securityUpdates(workspace2.securityUpdates)
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .organizationId(ORGANIZATION_ID)
        .tombstone(false)

    val actualWorkspaceReadList = getWorkspacesHandler(AirbyteEdition.COMMUNITY).listWorkspaces()

    assertEquals(
      listOf<WorkspaceRead?>(expectedWorkspaceRead1, expectedWorkspaceRead2),
      actualWorkspaceReadList.workspaces,
    )
  }

  @Test
  fun testGetWorkspace() {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false))
      .thenReturn(workspace)

    val workspaceIdRequestBody = WorkspaceIdRequestBody().workspaceId(workspace.workspaceId)

    val workspaceRead =
      WorkspaceRead()
        .workspaceId(workspace.workspaceId)
        .customerId(workspace.customerId)
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

    assertEquals(workspaceRead, getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspace(workspaceIdRequestBody))
  }

  @Test
  fun testGetWorkspaceBySlug() {
    Mockito.`when`(workspaceService.getWorkspaceBySlug("default", false)).thenReturn(workspace)

    val slugRequestBody = SlugRequestBody().slug("default")
    val workspaceRead = getWorkspaceReadPerWorkspace(workspace)

    assertEquals(workspaceRead, getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceBySlug(slugRequestBody))
  }

  @Test
  fun testGetWorkspaceByConnectionId() {
    val connectionId = UUID.randomUUID()
    Mockito.`when`(workspaceService.getStandardWorkspaceFromConnection(connectionId, false)).thenReturn(workspace)
    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(connectionId)
    val workspaceRead = getWorkspaceReadPerWorkspace(workspace)

    assertEquals(
      workspaceRead,
      getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceByConnectionId(connectionIdRequestBody, false),
    )
  }

  private fun getWorkspaceReadPerWorkspace(workspace: StandardWorkspace): WorkspaceRead =
    WorkspaceRead()
      .workspaceId(workspace.workspaceId)
      .customerId(workspace.customerId)
      .email(TEST_EMAIL)
      .name(workspace.name)
      .slug(workspace.slug)
      .initialSetupComplete(workspace.initialSetupComplete)
      .displaySetupWizard(workspace.displaySetupWizard)
      .news(workspace.news)
      .anonymousDataCollection(workspace.anonymousDataCollection)
      .securityUpdates(workspace.securityUpdates)
      .notifications(toApiList(workspace.notifications))
      .notificationSettings(toApi(workspace.notificationSettings))
      .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
      .organizationId(ORGANIZATION_ID)
      .tombstone(workspace.tombstone)

  @Test
  fun testGetWorkspaceByConnectionIdOnConfigNotFound() {
    val connectionId = UUID.randomUUID()
    Mockito
      .`when`(workspaceService.getStandardWorkspaceFromConnection(connectionId, false))
      .thenAnswer { throw ConfigNotFoundException("something", connectionId.toString()) }
    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(connectionId)
    assertThrows(
      ConfigNotFoundException::class.java,
    ) { getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceByConnectionId(connectionIdRequestBody, false) }
  }

  @ParameterizedTest
  @CsvSource("true", "false")
  fun testGetWorkspaceOrganizationInfo(isSso: Boolean) {
    val organization = generateOrganization(if (isSso) "test-realm" else null)
    val workspaceIdRequestBody = WorkspaceIdRequestBody().workspaceId(workspace.workspaceId)

    Mockito.`when`(organizationService.getOrganizationForWorkspaceId(workspace.getWorkspaceId())).thenReturn(
      Optional.of<Organization>(organization),
    )

    val orgInfo =
      getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceOrganizationInfo(workspaceIdRequestBody)

    assertEquals(organization.organizationId, orgInfo.organizationId)
    assertEquals(organization.name, orgInfo.organizationName)
    assertEquals(isSso, orgInfo.sso) // sso is true if ssoRealm is set
  }

  @Test
  fun testGerWorkspaceOrganizationInfoConfigNotFound() {
    val workspaceIdRequestBody = WorkspaceIdRequestBody().workspaceId(workspace.workspaceId)

    Mockito.`when`(organizationService.getOrganizationForWorkspaceId(workspace.getWorkspaceId())).thenReturn(
      Optional.empty<Organization>(),
    )

    assertThrows(
      ConfigNotFoundException::class.java,
    ) { getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceOrganizationInfo(workspaceIdRequestBody) }
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  fun testUpdateWorkspace(airbyteEdition: AirbyteEdition) {
    val apiNotification = generateApiNotification()
    apiNotification.getSlackConfiguration().webhook(UPDATED)
    val workspaceUpdate =
      WorkspaceUpdate()
        .workspaceId(workspace.workspaceId)
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
        .withWorkspaceId(workspace.workspaceId)
        .withCustomerId(workspace.customerId)
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

    // Mock dataplane group validation for the update
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    Mockito.`when`(dataplaneGroupService.listDefaultDataplaneGroups()).thenReturn(defaultDataplaneGroups)

    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false))
      .thenReturn(workspace)
      .thenReturn(expectedWorkspace)

    Mockito.`when`(secretPersistence.read(anyOrNull())).thenReturn("")

    val actualWorkspaceRead = getWorkspacesHandler(airbyteEdition).updateWorkspace(workspaceUpdate)

    val expectedNotificationRead = generateApiNotification()
    expectedNotificationRead.getSlackConfiguration().webhook(UPDATED)

    val expectedWorkspaceRead =
      WorkspaceRead()
        .workspaceId(workspace.workspaceId)
        .customerId(workspace.customerId)
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
        .withWorkspaceId(workspace.workspaceId)
        .withCustomerId(workspace.customerId)
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

    assertEquals(expectedWorkspaceRead, actualWorkspaceRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  fun testUpdateWorkspaceWithoutWebhookConfigs(airbyteEdition: AirbyteEdition?) {
    val apiNotification = generateApiNotification()
    apiNotification.slackConfiguration.webhook(UPDATED)
    val workspaceUpdate =
      WorkspaceUpdate()
        .workspaceId(workspace.workspaceId)
        .anonymousDataCollection(false)

    val expectedNotification = generateNotification()
    expectedNotification.slackConfiguration.withWebhook(UPDATED)
    val expectedWorkspace =
      StandardWorkspace()
        .withWorkspaceId(workspace.workspaceId)
        .withCustomerId(workspace.customerId)
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
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false))
      .thenReturn(expectedWorkspace)
      .thenReturn(expectedWorkspace.withAnonymousDataCollection(false))

    getWorkspacesHandler(AirbyteEdition.COMMUNITY).updateWorkspace(workspaceUpdate)

    Mockito.verify(workspaceService).writeStandardWorkspaceNoSecrets(expectedWorkspace)
  }

  @Test
  @DisplayName("Updating workspace name should update name and slug")
  fun testUpdateWorkspaceNoNameUpdate() {
    val workspaceUpdate =
      WorkspaceUpdateName()
        .workspaceId(workspace.workspaceId)
        .name("New Workspace Name")

    val expectedWorkspace =
      StandardWorkspace()
        .withWorkspaceId(workspace.workspaceId)
        .withCustomerId(workspace.customerId)
        .withEmail(TEST_EMAIL)
        .withName("New Workspace Name")
        .withSlug("new-workspace-name")
        .withAnonymousDataCollection(workspace.anonymousDataCollection)
        .withSecurityUpdates(workspace.securityUpdates)
        .withNews(workspace.news)
        .withInitialSetupComplete(workspace.initialSetupComplete)
        .withDisplaySetupWizard(workspace.displaySetupWizard)
        .withTombstone(false)
        .withNotifications(workspace.notifications)
        .withNotificationSettings(workspace.notificationSettings)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .withOrganizationId(ORGANIZATION_ID)

    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false))
      .thenReturn(workspace)
      .thenReturn(expectedWorkspace)

    val actualWorkspaceRead = getWorkspacesHandler(AirbyteEdition.COMMUNITY).updateWorkspaceName(workspaceUpdate)

    val expectedWorkspaceRead =
      WorkspaceRead()
        .workspaceId(workspace.workspaceId)
        .customerId(workspace.customerId)
        .email(TEST_EMAIL)
        .name("New Workspace Name")
        .slug("new-workspace-name")
        .initialSetupComplete(workspace.initialSetupComplete)
        .displaySetupWizard(workspace.displaySetupWizard)
        .news(workspace.news)
        .anonymousDataCollection(workspace.anonymousDataCollection)
        .securityUpdates(workspace.securityUpdates)
        .notifications(listOf<@Valid io.airbyte.api.model.generated.Notification?>(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .organizationId(ORGANIZATION_ID)
        .tombstone(false)

    Mockito.verify(workspaceService).writeStandardWorkspaceNoSecrets(expectedWorkspace)

    assertEquals(expectedWorkspaceRead, actualWorkspaceRead)
  }

  @Test
  @DisplayName("Update organization in workspace")
  fun testWorkspaceUpdateOrganization() {
    val newOrgId = UUID.randomUUID()
    val workspaceUpdateOrganization =
      WorkspaceUpdateOrganization()
        .workspaceId(workspace.workspaceId)
        .organizationId(newOrgId)
    val expectedWorkspace = clone(workspace).withOrganizationId(newOrgId)
    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false))
      .thenReturn(expectedWorkspace)
    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false))
      .thenReturn(expectedWorkspace)
    // The same as the original workspace, with only the organization ID updated.
    val expectedWorkspaceRead =
      WorkspaceRead()
        .workspaceId(workspace.workspaceId)
        .customerId(workspace.customerId)
        .email(workspace.email)
        .name(workspace.name)
        .slug(workspace.slug)
        .initialSetupComplete(workspace.initialSetupComplete)
        .displaySetupWizard(workspace.displaySetupWizard)
        .news(workspace.news)
        .anonymousDataCollection(false)
        .securityUpdates(workspace.securityUpdates)
        .notifications(toApiList(workspace.notifications))
        .notificationSettings(toApi(workspace.notificationSettings))
        .dataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .organizationId(newOrgId)
        .tombstone(false)

    val actualWorkspaceRead =
      getWorkspacesHandler(AirbyteEdition.COMMUNITY).updateWorkspaceOrganization(workspaceUpdateOrganization)
    Mockito.verify(workspaceService).writeStandardWorkspaceNoSecrets(expectedWorkspace)
    assertEquals(expectedWorkspaceRead, actualWorkspaceRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @DisplayName("Partial patch update should preserve unchanged fields")
  fun testWorkspacePatchUpdate(airbyteEdition: AirbyteEdition) {
    val expectedNewEmail = "expected-new-email@example.com"
    val workspaceUpdate =
      WorkspaceUpdate()
        .workspaceId(workspace.workspaceId)
        .anonymousDataCollection(true) // originally was DATAPLANE_GROUP_ID_1
        .dataplaneGroupId(DATAPLANE_GROUP_ID_2)
        .email(expectedNewEmail)

    // Mock dataplane group validation for the update
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_2).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    Mockito.`when`(dataplaneGroupService.listDefaultDataplaneGroups()).thenReturn(defaultDataplaneGroups)

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
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false))
      .thenReturn(workspace)
      .thenReturn(expectedWorkspace)

    // The same as the original workspace, with only the email and data collection flags changed.
    val expectedWorkspaceRead =
      WorkspaceRead()
        .workspaceId(workspace.workspaceId)
        .customerId(workspace.customerId)
        .email(expectedNewEmail)
        .name(workspace.name)
        .slug(workspace.slug)
        .initialSetupComplete(workspace.initialSetupComplete)
        .displaySetupWizard(workspace.displaySetupWizard)
        .news(workspace.news)
        .anonymousDataCollection(true)
        .securityUpdates(workspace.securityUpdates)
        .notifications(toApiList(workspace.notifications))
        .notificationSettings(toApi(workspace.notificationSettings))
        .dataplaneGroupId(DATAPLANE_GROUP_ID_2)
        .organizationId(ORGANIZATION_ID)
        .webhookConfigs(mutableListOf<@Valid WebhookConfigRead?>())
        .tombstone(false)

    val actualWorkspaceRead = getWorkspacesHandler(airbyteEdition).updateWorkspace(workspaceUpdate)
    Mockito.verify(workspaceService).writeStandardWorkspaceNoSecrets(expectedWorkspace)
    assertEquals(expectedWorkspaceRead, actualWorkspaceRead)
  }

  @Test
  fun testWorkspacePatchUpdateWithPublicNotificationConfig() {
    // This is the workspace that exists before the update. It has a customerio notification for both
    // success and failure, and a webhook ("slack") notification for failure.
    //
    // This test shows that the notifications can be patched, and leave the existing settings in place.

    val existingWorkspace = clone(workspace)
    existingWorkspace
      .notificationSettings
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
        .workspaceId(existingWorkspace.workspaceId)
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
      .notificationSettings
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
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false))
      .thenReturn(existingWorkspace)
      .thenReturn(expectedWorkspace)

    getWorkspacesHandler(AirbyteEdition.COMMUNITY).updateWorkspace(workspaceUpdate)
    Mockito.verify(workspaceService).writeStandardWorkspaceNoSecrets(expectedWorkspace)
  }

  @Test
  fun testSetFeedbackDone() {
    val workspaceGiveFeedback =
      WorkspaceGiveFeedback()
        .workspaceId(UUID.randomUUID())

    getWorkspacesHandler(AirbyteEdition.COMMUNITY).setFeedbackDone(workspaceGiveFeedback)

    Mockito.verify(workspaceService).setFeedback(workspaceGiveFeedback.workspaceId)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  fun testWorkspaceIsWrittenThroughSecretsWriter(airbyteEdition: AirbyteEdition) {
    val workspacesHandler =
      WorkspacesHandler(
        workspacePersistence,
        organizationService,
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
        roleResolver,
      )

    val uuid = UUID.randomUUID()
    Mockito.`when`(uuidSupplier.get()).thenReturn(uuid)

    // Mock dataplane group validation for creation
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    Mockito.`when`(dataplaneGroupService.listDefaultDataplaneGroups()).thenReturn(defaultDataplaneGroups)

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

    assertEquals(expectedRead, actualRead)
    Mockito.verify(workspaceService, Mockito.times(1)).writeWorkspaceWithSecrets(anyOrNull())
  }

  @Test
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
    assertEquals(2, result.getWorkspaces().size)
  }

  @Test
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
    assertEquals(2, result.getWorkspaces().size)
  }

  @Test
  fun testListWorkspacesInOrgForUserNoKeyword() {
    val userId = UUID.randomUUID()
    val request =
      ListWorkspacesInOrganizationRequestBody().organizationId(ORGANIZATION_ID).pagination(Pagination().pageSize(100).rowOffset(0))
    val expectedWorkspaces = listOf(generateWorkspace(), generateWorkspace())
    Mockito
      .`when`(
        workspacePersistence.listWorkspacesInOrganizationByUserIdPaginated(
          ResourcesByOrganizationQueryPaginated(ORGANIZATION_ID, false, 100, 0),
          userId,
          Optional.empty<String>(),
        ),
      ).thenReturn(expectedWorkspaces)
    val result = getWorkspacesHandler(AirbyteEdition.COMMUNITY).listWorkspacesInOrganizationForUser(userId, request)
    assertEquals(2, result.getWorkspaces().size)
  }

  @Test
  fun testListWorkspacesInOrgForUserWithKeyword() {
    val userId = UUID.randomUUID()
    val request =
      ListWorkspacesInOrganizationRequestBody()
        .organizationId(ORGANIZATION_ID)
        .nameContains("test")
        .pagination(Pagination().pageSize(100).rowOffset(0))
    val expectedWorkspaces = listOf(generateWorkspace())
    Mockito
      .`when`(
        workspacePersistence.listWorkspacesInOrganizationByUserIdPaginated(
          ResourcesByOrganizationQueryPaginated(ORGANIZATION_ID, false, 100, 0),
          userId,
          Optional.of<String>("test"),
        ),
      ).thenReturn(expectedWorkspaces)
    val result = getWorkspacesHandler(AirbyteEdition.COMMUNITY).listWorkspacesInOrganizationForUser(userId, request)
    assertEquals(1, result.getWorkspaces().size)
  }

  @Test
  fun testListWorkspacesInOrgForUserNoPagination() {
    val userId = UUID.randomUUID()
    val request = ListWorkspacesInOrganizationRequestBody().organizationId(ORGANIZATION_ID)
    val expectedWorkspaces = listOf(generateWorkspace(), generateWorkspace())
    Mockito
      .`when`(
        workspacePersistence.listWorkspacesInOrganizationByUserId(
          ORGANIZATION_ID,
          userId,
          Optional.empty<String>(),
        ),
      ).thenReturn(expectedWorkspaces)
    val result = getWorkspacesHandler(AirbyteEdition.COMMUNITY).listWorkspacesInOrganizationForUser(userId, request)
    assertEquals(2, result.getWorkspaces().size)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @DisplayName("Creating workspace with explicit dataplane group requires org editor permission")
  fun testCreateWorkspaceWithDataplaneGroupRequiresOrgEditor(airbyteEdition: AirbyteEdition) {
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

    // Mock that DATAPLANE_GROUP_ID_2 is not a default group
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    Mockito.`when`(dataplaneGroupService.listDefaultDataplaneGroups()).thenReturn(defaultDataplaneGroups)

    // Mock the dataplane group that belongs to the same organization
    val dataplaneGroup = DataplaneGroup().withId(DATAPLANE_GROUP_ID_2).withOrganizationId(ORGANIZATION_ID)
    Mockito.`when`(dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID_2)).thenReturn(dataplaneGroup)

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
        .dataplaneGroupId(DATAPLANE_GROUP_ID_2) // Explicit dataplane group
        .webhookConfigs(listOf<@Valid WebhookConfigWrite?>(WebhookConfigWrite().name(TEST_NAME).authToken(TEST_AUTH_TOKEN)))
        .organizationId(ORGANIZATION_ID)

    Mockito.`when`(secretPersistence.read(anyOrNull())).thenReturn("")

    getWorkspacesHandler(airbyteEdition).createWorkspace(workspaceCreate)

    // Verify that role check was performed
    Mockito.verify(roleResolver).newRequest()
    Mockito.verify(roleRequest).withCurrentUser()
    Mockito.verify(roleRequest).withOrg(ORGANIZATION_ID)
    Mockito.verify(roleRequest).requireRole("ORGANIZATION_EDITOR")
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @DisplayName("Updating workspace dataplane group requires org editor permission")
  fun testUpdateWorkspaceDataplaneGroupRequiresOrgEditor(airbyteEdition: AirbyteEdition) {
    // Mock that DATAPLANE_GROUP_ID_2 is not a default group
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    Mockito.`when`(dataplaneGroupService.listDefaultDataplaneGroups()).thenReturn(defaultDataplaneGroups)

    // Mock the dataplane group that belongs to the same organization
    val dataplaneGroup = DataplaneGroup().withId(DATAPLANE_GROUP_ID_2).withOrganizationId(ORGANIZATION_ID)
    Mockito.`when`(dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID_2)).thenReturn(dataplaneGroup)

    val workspaceUpdate =
      WorkspaceUpdate()
        .workspaceId(workspace.workspaceId)
        .dataplaneGroupId(DATAPLANE_GROUP_ID_2) // Changing dataplane group

    val expectedWorkspace =
      clone(workspace)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_2)
        .withWebhookOperationConfigs(
          jsonNode<WebhookOperationConfigs?>(
            WebhookOperationConfigs()
              .withWebhookConfigs(mutableListOf<WebhookConfig?>()),
          ),
        )

    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false))
      .thenReturn(workspace)
      .thenReturn(expectedWorkspace)

    getWorkspacesHandler(airbyteEdition).updateWorkspace(workspaceUpdate)

    // Verify that role check was performed
    Mockito.verify(roleResolver).newRequest()
    Mockito.verify(roleRequest).withCurrentUser()
    Mockito.verify(roleRequest).withOrg(ORGANIZATION_ID)
    Mockito.verify(roleRequest).requireRole("ORGANIZATION_EDITOR")
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @DisplayName("Creating workspace with default dataplane group should not require org editor permission")
  fun testCreateWorkspaceWithDefaultDataplaneGroupDoesNotRequireOrgEditor(airbyteEdition: AirbyteEdition) {
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

    // Mock default dataplane group
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_2).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    Mockito.`when`(dataplaneGroupService.listDefaultDataplaneGroups()).thenReturn(defaultDataplaneGroups)

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val workspaceCreate =
      WorkspaceCreate()
        .name(NEW_WORKSPACE)
        .email(TEST_EMAIL)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(listOf(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettings())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_2) // This is a default group
        .webhookConfigs(listOf(WebhookConfigWrite().name(TEST_NAME).authToken(TEST_AUTH_TOKEN)))
        .organizationId(ORGANIZATION_ID)

    Mockito.`when`(secretPersistence.read(anyOrNull())).thenReturn("")

    getWorkspacesHandler(airbyteEdition).createWorkspace(workspaceCreate)

    // Verify that role check was still performed (but won't throw since it's a default group)
    Mockito.verify(roleResolver).newRequest()
    Mockito.verify(roleRequest).withCurrentUser()
    Mockito.verify(roleRequest).withOrg(ORGANIZATION_ID)
    Mockito.verify(roleRequest).requireRole("ORGANIZATION_EDITOR")
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @DisplayName("Creating workspace with non-default dataplane group from wrong org should throw forbidden")
  fun testCreateWorkspaceWithWrongOrgDataplaneGroupThrowsForbidden(airbyteEdition: AirbyteEdition) {
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

    // Mock that DATAPLANE_GROUP_ID_2 is not a default group
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    Mockito.`when`(dataplaneGroupService.listDefaultDataplaneGroups()).thenReturn(defaultDataplaneGroups)

    // Mock that the dataplane group belongs to a different organization
    val wrongOrgId = UUID.randomUUID()
    val dataplaneGroupFromWrongOrg = DataplaneGroup().withId(DATAPLANE_GROUP_ID_2).withOrganizationId(wrongOrgId)
    Mockito.`when`(dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID_2)).thenReturn(dataplaneGroupFromWrongOrg)

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val workspaceCreate =
      WorkspaceCreate()
        .name(NEW_WORKSPACE)
        .email(TEST_EMAIL)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(listOf(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettings())
        .dataplaneGroupId(DATAPLANE_GROUP_ID_2) // This belongs to wrong org
        .webhookConfigs(listOf(WebhookConfigWrite().name(TEST_NAME).authToken(TEST_AUTH_TOKEN)))
        .organizationId(ORGANIZATION_ID)

    Mockito.`when`(secretPersistence.read(anyOrNull())).thenReturn("")

    // Should throw ForbiddenProblem
    assertThrows<ForbiddenProblem> {
      getWorkspacesHandler(airbyteEdition).createWorkspace(workspaceCreate)
    }
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @DisplayName("Updating workspace with default dataplane group should not require additional validation")
  fun testUpdateWorkspaceWithDefaultDataplaneGroupSucceeds(airbyteEdition: AirbyteEdition) {
    // Mock that DATAPLANE_GROUP_ID_2 is a default group
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_2).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    Mockito.`when`(dataplaneGroupService.listDefaultDataplaneGroups()).thenReturn(defaultDataplaneGroups)

    val workspaceUpdate =
      WorkspaceUpdate()
        .workspaceId(workspace.workspaceId)
        .dataplaneGroupId(DATAPLANE_GROUP_ID_2) // This is a default group

    val expectedWorkspace =
      clone(workspace)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_2)
        .withWebhookOperationConfigs(
          jsonNode<WebhookOperationConfigs?>(
            WebhookOperationConfigs()
              .withWebhookConfigs(mutableListOf<WebhookConfig?>()),
          ),
        )

    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false))
      .thenReturn(workspace)
      .thenReturn(expectedWorkspace)

    getWorkspacesHandler(airbyteEdition).updateWorkspace(workspaceUpdate)

    // Verify that role check was still performed (but passed because it's a default group)
    Mockito.verify(roleResolver).newRequest()
    Mockito.verify(roleRequest).withCurrentUser()
    Mockito.verify(roleRequest).withOrg(ORGANIZATION_ID)
    Mockito.verify(roleRequest).requireRole("ORGANIZATION_EDITOR")
    Mockito.verify(workspaceService).writeStandardWorkspaceNoSecrets(expectedWorkspace)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @DisplayName("Updating workspace with non-default dataplane group from wrong org should throw forbidden")
  fun testUpdateWorkspaceWithWrongOrgDataplaneGroupThrowsForbidden(airbyteEdition: AirbyteEdition) {
    // Mock that DATAPLANE_GROUP_ID_2 is not a default group
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    Mockito.`when`(dataplaneGroupService.listDefaultDataplaneGroups()).thenReturn(defaultDataplaneGroups)

    // Mock that the dataplane group belongs to a different organization
    val wrongOrgId = UUID.randomUUID()
    val dataplaneGroupFromWrongOrg = DataplaneGroup().withId(DATAPLANE_GROUP_ID_2).withOrganizationId(wrongOrgId)
    Mockito.`when`(dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID_2)).thenReturn(dataplaneGroupFromWrongOrg)

    val workspaceUpdate =
      WorkspaceUpdate()
        .workspaceId(workspace.workspaceId)
        .dataplaneGroupId(DATAPLANE_GROUP_ID_2) // This belongs to wrong org

    Mockito
      .`when`(workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false))
      .thenReturn(workspace)

    // Should throw ForbiddenProblem
    assertThrows<ForbiddenProblem> {
      getWorkspacesHandler(airbyteEdition).updateWorkspace(workspaceUpdate)
    }
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
