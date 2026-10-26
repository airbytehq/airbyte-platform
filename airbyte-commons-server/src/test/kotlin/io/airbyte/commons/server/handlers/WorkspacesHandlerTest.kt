/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.analytics.TrackingClient
import io.airbyte.api.problems.model.generated.ProblemWorkspaceLimitData
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.api.problems.throwable.generated.WorkspaceLimitForOrganizationReachedProblem
import io.airbyte.api.server.generated.models.ActorListCursorPaginatedRequestBody
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionRead
import io.airbyte.api.server.generated.models.ConnectionReadList
import io.airbyte.api.server.generated.models.DestinationRead
import io.airbyte.api.server.generated.models.DestinationReadList
import io.airbyte.api.server.generated.models.ListWorkspacesInOrganizationRequestBody
import io.airbyte.api.server.generated.models.NotificationConfig
import io.airbyte.api.server.generated.models.NotificationType
import io.airbyte.api.server.generated.models.NotificationsConfig
import io.airbyte.api.server.generated.models.Pagination
import io.airbyte.api.server.generated.models.SlugRequestBody
import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.api.server.generated.models.SourceReadList
import io.airbyte.api.server.generated.models.WebhookConfigRead
import io.airbyte.api.server.generated.models.WebhookConfigWrite
import io.airbyte.api.server.generated.models.WebhookNotificationConfig
import io.airbyte.api.server.generated.models.WorkspaceCreate
import io.airbyte.api.server.generated.models.WorkspaceCreateWithId
import io.airbyte.api.server.generated.models.WorkspaceGiveFeedback
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.api.server.generated.models.WorkspaceRead
import io.airbyte.api.server.generated.models.WorkspaceUpdate
import io.airbyte.api.server.generated.models.WorkspaceUpdateName
import io.airbyte.api.server.generated.models.WorkspaceUpdateOrganization
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.MaximumWorkspacesEntitlement
import io.airbyte.commons.entitlements.models.NumericEntitlementResult
import io.airbyte.commons.json.Jsons
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
import io.airbyte.config.persistence.WorkspaceTombstoneResult
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ResourcesByOrganizationQueryPaginated
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.scim.ScimConfigurationRead
import io.airbyte.domain.models.scim.ScimConfigurationStatus
import io.airbyte.domain.services.scim.ScimAccessGate
import io.airbyte.domain.services.scim.ScimConfigurationService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.mockk.Called
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
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
  lateinit var workspaceHelper: WorkspaceHelper
  lateinit var dataplaneGroupService: DataplaneGroupService
  lateinit var entitlementService: EntitlementService
  lateinit var organizationService: OrganizationService
  lateinit var trackingClient: TrackingClient
  lateinit var limitsProvider: ProductLimitsProvider
  lateinit var consumptionService: ConsumptionService
  lateinit var ffClient: FeatureFlagClient
  lateinit var roleResolver: RoleResolver
  lateinit var roleRequest: RoleResolver.Request
  lateinit var scimConfigurationService: ScimConfigurationService
  lateinit var scimAccessGate: ScimAccessGate

  @BeforeEach
  fun setUp() {
    workspacePersistence = mockk<WorkspacePersistence>(relaxUnitFun = true)
    organizationService = mockk<OrganizationService>(relaxUnitFun = true)
    secretPersistence = mockk<SecretPersistence>(relaxUnitFun = true)
    permissionHandler = mockk<PermissionHandler>(relaxUnitFun = true)
    connectionsHandler = mockk<ConnectionsHandler>(relaxUnitFun = true)
    destinationHandler = mockk<DestinationHandler>(relaxUnitFun = true)
    sourceHandler = mockk<SourceHandler>(relaxUnitFun = true)
    uuidSupplier = mockk<Supplier<UUID>>()
    workspaceService = mockk<WorkspaceService>(relaxUnitFun = true)
    workspaceHelper = mockk<WorkspaceHelper>(relaxUnitFun = true)
    dataplaneGroupService = mockk<DataplaneGroupService>(relaxUnitFun = true)
    entitlementService = mockk<EntitlementService>(relaxUnitFun = true)
    trackingClient = mockk<TrackingClient>(relaxUnitFun = true)
    limitsProvider = mockk<ProductLimitsProvider>(relaxUnitFun = true)
    consumptionService = mockk<ConsumptionService>(relaxUnitFun = true)
    ffClient = mockk<TestClient>(relaxUnitFun = true)
    roleResolver = mockk<RoleResolver>(relaxUnitFun = true)
    roleRequest = mockk<RoleResolver.Request>(relaxUnitFun = true)
    scimConfigurationService = mockk<ScimConfigurationService>(relaxUnitFun = true)
    scimAccessGate = mockk<ScimAccessGate>(relaxUnitFun = true)

    // Setup roleResolver mock chain
    every { roleResolver.newRequest() } returns roleRequest
    every { roleRequest.withCurrentUser() } returns roleRequest
    every { roleRequest.withOrg(any()) } returns roleRequest
    every { roleRequest.requireRole(any()) } just Runs

    every { scimConfigurationService.getConfiguration(any()) } returns ScimConfigurationRead(status = ScimConfigurationStatus.NOT_CONFIGURED)
    every { scimAccessGate.isAllowedForOrganizationInfo(any()) } returns true

    // By default, no maximum-workspaces entitlement is granted and the organization has no workspaces.
    every { entitlementService.getNumericEntitlement(any(), any()) } returns
      NumericEntitlementResult(
        featureId = MaximumWorkspacesEntitlement.featureId,
        hasAccess = false,
        value = null,
      )
    every { workspacePersistence.workspaceExists(any()) } returns false
    every { workspacePersistence.countWorkspacesByOrganizationId(any()) } returns 0L
    every { workspaceService.getWorkspaceBySlugOptional(any(), any()) } returns Optional.empty()
    every { entitlementService.getCurrentPlanId(any()) } returns null
    every { ffClient.boolVariation(any(), any()) } returns false

    workspace = generateWorkspace()
    every { organizationService.getActiveOrganization(ORGANIZATION_ID) } returns Optional.of(generateOrganization(null))
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
      workspaceHelper,
      dataplaneGroupService,
      entitlementService,
      trackingClient,
      limitsProvider,
      consumptionService,
      ffClient,
      airbyteEdition,
      roleResolver,
      scimConfigurationService,
      scimAccessGate,
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

  private fun generateApiNotification(webhook: String = FAILURE_NOTIFICATION_WEBHOOK): io.airbyte.api.server.generated.models.Notification =
    io.airbyte.api.server.generated.models.Notification(
      notificationType = NotificationType.SLACK,
      slackConfiguration =
        io.airbyte.api.server.generated.models
          .SlackNotificationConfiguration(webhook = webhook),
    )

  private fun generateApiNotificationSettings(): io.airbyte.api.server.generated.models.NotificationSettings? =
    io.airbyte.api.server.generated.models.NotificationSettings(
      sendOnFailure =
        io.airbyte.api.server.generated.models.NotificationItem(
          notificationType = listOf(NotificationType.SLACK),
          slackConfiguration =
            io.airbyte.api.server.generated.models.SlackNotificationConfiguration(
              webhook = FAILURE_NOTIFICATION_WEBHOOK,
            ),
        ),
    )

  private fun generateApiNotificationSettingsWithDefaultValue(): io.airbyte.api.server.generated.models.NotificationSettings? =
    io.airbyte.api.server.generated.models.NotificationSettings(
      sendOnFailure =
        io.airbyte.api.server.generated.models.NotificationItem(
          notificationType = listOf(NotificationType.SLACK),
          slackConfiguration =
            io.airbyte.api.server.generated.models.SlackNotificationConfiguration(
              webhook = FAILURE_NOTIFICATION_WEBHOOK,
            ),
        ),
      sendOnSuccess =
        io.airbyte.api.server.generated.models
          .NotificationItem(notificationType = emptyList()),
      sendOnConnectionUpdate = customerioNotificationItem(),
      sendOnConnectionUpdateActionRequired = customerioNotificationItem(),
      sendOnSyncDisabled = customerioNotificationItem(),
      sendOnSyncDisabledWarning = customerioNotificationItem(),
      sendOnBreakingChangeWarning = customerioNotificationItem(),
      sendOnBreakingChangeSyncsDisabled = customerioNotificationItem(),
      sendOnConnectionSyncQueued =
        io.airbyte.api.server.generated.models
          .NotificationItem(notificationType = emptyList()),
    )

  private fun generateDefaultApiNotificationSettings(): io.airbyte.api.server.generated.models.NotificationSettings? =
    io.airbyte.api.server.generated.models.NotificationSettings(
      sendOnSuccess =
        io.airbyte.api.server.generated.models
          .NotificationItem(notificationType = emptyList()),
      sendOnFailure =
        io.airbyte.api.server.generated.models
          .NotificationItem(notificationType = listOf(NotificationType.CUSTOMERIO)),
      sendOnConnectionUpdate = customerioNotificationItem(),
      sendOnConnectionUpdateActionRequired = customerioNotificationItem(),
      sendOnSyncDisabled = customerioNotificationItem(),
      sendOnSyncDisabledWarning = customerioNotificationItem(),
      sendOnBreakingChangeWarning = customerioNotificationItem(),
      sendOnBreakingChangeSyncsDisabled = customerioNotificationItem(),
      sendOnConnectionSyncQueued =
        io.airbyte.api.server.generated.models
          .NotificationItem(notificationType = emptyList()),
    )

  private fun customerioNotificationItem() =
    io.airbyte.api.server.generated.models
      .NotificationItem(notificationType = listOf(NotificationType.CUSTOMERIO))

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  fun testCreateWorkspace(airbyteEdition: AirbyteEdition) {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
    every {
      workspaceService.getStandardWorkspaceNoSecrets(
        any(),
        false,
      )
    } returns workspace

    val uuid = UUID.randomUUID()
    every { uuidSupplier.get() } returns uuid

    // Mock dataplane group service for DATAPLANE_GROUP_ID_1
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    every { dataplaneGroupService.listDefaultDataplaneGroups() } returns defaultDataplaneGroups

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val workspaceCreate =
      WorkspaceCreate(
        name = NEW_WORKSPACE,
        email = TEST_EMAIL,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettings(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        webhookConfigs = listOf<WebhookConfigWrite>(WebhookConfigWrite(name = TEST_NAME, authToken = TEST_AUTH_TOKEN)),
        organizationId = ORGANIZATION_ID,
      )

    every { secretPersistence.read(any()) } returns ""

    val actualRead = getWorkspacesHandler(airbyteEdition).createWorkspace(workspaceCreate)
    val expectedRead =
      WorkspaceRead(
        workspaceId = uuid,
        customerId = uuid,
        email = TEST_EMAIL,
        name = NEW_WORKSPACE,
        slug = NEW_WORKSPACE_SLUG,
        initialSetupComplete = false,
        displaySetupWizard = false,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettingsWithDefaultValue(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        webhookConfigs = listOf<WebhookConfigRead>(WebhookConfigRead(id = uuid, name = TEST_NAME)),
        organizationId = ORGANIZATION_ID,
        tombstone = false,
      )

    assertEquals(expectedRead, actualRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  fun testCreateWorkspaceIfNotExist(airbyteEdition: AirbyteEdition) {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
    every {
      workspaceService.getStandardWorkspaceNoSecrets(
        any(),
        false,
      )
    } returns workspace

    val uuid = UUID.randomUUID()
    every { uuidSupplier.get() } returns uuid

    // Mock dataplane group service for DATAPLANE_GROUP_ID_1
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    every { dataplaneGroupService.listDefaultDataplaneGroups() } returns defaultDataplaneGroups

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val notSuppliedUUID = UUID.randomUUID()

    val workspaceCreateWithId =
      WorkspaceCreateWithId(
        id = notSuppliedUUID,
        name = NEW_WORKSPACE,
        email = TEST_EMAIL,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettings(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        webhookConfigs = listOf<WebhookConfigWrite>(WebhookConfigWrite(name = TEST_NAME, authToken = TEST_AUTH_TOKEN)),
        organizationId = ORGANIZATION_ID,
      )

    every { secretPersistence.read(any()) } returns ""

    val actualRead = getWorkspacesHandler(airbyteEdition).createWorkspaceIfNotExist(workspaceCreateWithId)
    val secondActualRead = getWorkspacesHandler(airbyteEdition).createWorkspaceIfNotExist(workspaceCreateWithId)
    val expectedRead =
      WorkspaceRead(
        workspaceId = notSuppliedUUID,
        customerId = uuid,
        email = TEST_EMAIL,
        name = NEW_WORKSPACE,
        slug = NEW_WORKSPACE_SLUG,
        initialSetupComplete = false,
        displaySetupWizard = false,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettingsWithDefaultValue(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        webhookConfigs = listOf<WebhookConfigRead>(WebhookConfigRead(id = uuid, name = TEST_NAME)),
        organizationId = ORGANIZATION_ID,
        tombstone = false,
      )

    assertEquals(expectedRead, actualRead)
    assertEquals(expectedRead, secondActualRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  fun testCreateWorkspaceIfNotExistWithBlankEmail(airbyteEdition: AirbyteEdition) {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
    every {
      workspaceService.getStandardWorkspaceNoSecrets(
        any(),
        false,
      )
    } returns workspace

    val uuid = UUID.randomUUID()
    every { uuidSupplier.get() } returns uuid

    // Mock dataplane group service for DATAPLANE_GROUP_ID_1
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    every { dataplaneGroupService.listDefaultDataplaneGroups() } returns defaultDataplaneGroups

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val notSuppliedUUID = UUID.randomUUID()

    val workspaceCreateWithId =
      WorkspaceCreateWithId(
        id = notSuppliedUUID,
        name = NEW_WORKSPACE,
        email = "",
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettings(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        webhookConfigs = listOf<WebhookConfigWrite>(WebhookConfigWrite(name = TEST_NAME, authToken = TEST_AUTH_TOKEN)),
        organizationId = ORGANIZATION_ID,
      )

    every { secretPersistence.read(any()) } returns ""

    val actualRead = getWorkspacesHandler(airbyteEdition).createWorkspaceIfNotExist(workspaceCreateWithId)
    val secondActualRead = getWorkspacesHandler(airbyteEdition).createWorkspaceIfNotExist(workspaceCreateWithId)
    val expectedRead =
      WorkspaceRead(
        workspaceId = notSuppliedUUID,
        customerId = uuid,
        email = null,
        name = NEW_WORKSPACE,
        slug = NEW_WORKSPACE_SLUG,
        initialSetupComplete = false,
        displaySetupWizard = false,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettingsWithDefaultValue(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        webhookConfigs = listOf<WebhookConfigRead>(WebhookConfigRead(id = uuid, name = TEST_NAME)),
        organizationId = ORGANIZATION_ID,
        tombstone = false,
      )

    assertEquals(expectedRead, actualRead)
    assertEquals(expectedRead, secondActualRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  fun testCreateWorkspaceIfNotExistWithNullEmail(airbyteEdition: AirbyteEdition) {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
    every {
      workspaceService.getStandardWorkspaceNoSecrets(
        any(),
        false,
      )
    } returns workspace

    val uuid = UUID.randomUUID()
    every { uuidSupplier.get() } returns uuid

    // Mock dataplane group service for DATAPLANE_GROUP_ID_1
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    every { dataplaneGroupService.listDefaultDataplaneGroups() } returns defaultDataplaneGroups

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val notSuppliedUUID = UUID.randomUUID()

    val workspaceCreateWithId =
      WorkspaceCreateWithId(
        id = notSuppliedUUID,
        name = NEW_WORKSPACE,
        email = null,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettings(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        webhookConfigs = listOf<WebhookConfigWrite>(WebhookConfigWrite(name = TEST_NAME, authToken = TEST_AUTH_TOKEN)),
        organizationId = ORGANIZATION_ID,
      )

    every { secretPersistence.read(any()) } returns ""

    val actualRead = getWorkspacesHandler(airbyteEdition).createWorkspaceIfNotExist(workspaceCreateWithId)
    val secondActualRead = getWorkspacesHandler(airbyteEdition).createWorkspaceIfNotExist(workspaceCreateWithId)
    val expectedRead =
      WorkspaceRead(
        workspaceId = notSuppliedUUID,
        customerId = uuid,
        email = null,
        name = NEW_WORKSPACE,
        slug = NEW_WORKSPACE_SLUG,
        initialSetupComplete = false,
        displaySetupWizard = false,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettingsWithDefaultValue(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        webhookConfigs = listOf<WebhookConfigRead>(WebhookConfigRead(id = uuid, name = TEST_NAME)),
        organizationId = ORGANIZATION_ID,
        tombstone = false,
      )

    assertEquals(expectedRead, actualRead)
    assertEquals(expectedRead, secondActualRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  fun testCreateWorkspaceWithMinimumInput(airbyteEdition: AirbyteEdition) {
    every {
      workspaceService.getStandardWorkspaceNoSecrets(
        any(),
        false,
      )
    } returns workspace
    every { dataplaneGroupService.getDefaultDataplaneGroup() } returns DataplaneGroup().withId(DATAPLANE_GROUP_ID_1)

    val uuid = UUID.randomUUID()
    every { uuidSupplier.get() } returns uuid

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val workspaceCreate =
      WorkspaceCreate(name = NEW_WORKSPACE, email = TEST_EMAIL, organizationId = ORGANIZATION_ID)

    val actualRead = getWorkspacesHandler(airbyteEdition).createWorkspace(workspaceCreate)

    val expectedRead =
      WorkspaceRead(
        workspaceId = actualRead.workspaceId,
        customerId = actualRead.customerId,
        email = TEST_EMAIL,
        name = NEW_WORKSPACE,
        slug = actualRead.slug,
        initialSetupComplete = false,
        displaySetupWizard = false,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = mutableListOf<io.airbyte.api.server.generated.models.Notification>(),
        notificationSettings = generateDefaultApiNotificationSettings(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        webhookConfigs = mutableListOf<WebhookConfigRead>(),
        tombstone = false,
        organizationId = ORGANIZATION_ID,
      )

    assertEquals(expectedRead, actualRead)
  }

  private fun stubSuccessfulWorkspaceCreate() {
    every {
      workspaceService.getStandardWorkspaceNoSecrets(
        any(),
        false,
      )
    } returns workspace
    every { dataplaneGroupService.getDefaultDataplaneGroup() } returns DataplaneGroup().withId(DATAPLANE_GROUP_ID_1)
    every { uuidSupplier.get() } returns UUID.randomUUID()
  }

  private fun generateWorkspaceCreateWithId(): WorkspaceCreateWithId =
    WorkspaceCreateWithId(
      id = UUID.randomUUID(),
      name = NEW_WORKSPACE,
      organizationId = ORGANIZATION_ID,
    )

  @Test
  fun testCreateWorkspaceWhenAtMaximumWorkspacesLimitIsRejected() {
    every { entitlementService.getNumericEntitlement(any(), any()) } returns
      NumericEntitlementResult(
        featureId = MaximumWorkspacesEntitlement.featureId,
        hasAccess = true,
        value = 2L,
      )
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 2L

    val exception =
      assertThrows<WorkspaceLimitForOrganizationReachedProblem> {
        getWorkspacesHandler(AirbyteEdition.CLOUD).createWorkspaceIfNotExist(generateWorkspaceCreateWithId())
      }

    assertEquals(2L, (exception.problem.getData() as ProblemWorkspaceLimitData).limit)
    verify(exactly = 0) { workspaceService.writeWorkspaceWithSecrets(any()) }
  }

  @Test
  fun testCreateWorkspaceBelowMaximumWorkspacesLimitIsAllowed() {
    every { entitlementService.getNumericEntitlement(any(), any()) } returns
      NumericEntitlementResult(
        featureId = MaximumWorkspacesEntitlement.featureId,
        hasAccess = true,
        value = 3L,
      )
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 2L
    stubSuccessfulWorkspaceCreate()

    val actualRead = getWorkspacesHandler(AirbyteEdition.CLOUD).createWorkspaceIfNotExist(generateWorkspaceCreateWithId())

    assertEquals(ORGANIZATION_ID, actualRead.organizationId)
    verify(exactly = 1) { workspaceService.writeWorkspaceWithSecrets(any()) }
  }

  @Test
  fun testCreateWorkspaceWithUnlimitedWorkspacesEntitlementIsAllowed() {
    // Stigg models an unlimited numeric entitlement with isUnlimited=true and a meaningless value.
    every { entitlementService.getNumericEntitlement(any(), any()) } returns
      NumericEntitlementResult(
        featureId = MaximumWorkspacesEntitlement.featureId,
        hasAccess = true,
        value = 0L,
        isUnlimited = true,
      )
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 100L
    stubSuccessfulWorkspaceCreate()

    val actualRead = getWorkspacesHandler(AirbyteEdition.CLOUD).createWorkspaceIfNotExist(generateWorkspaceCreateWithId())

    assertEquals(ORGANIZATION_ID, actualRead.organizationId)
    verify(exactly = 1) { workspaceService.writeWorkspaceWithSecrets(any()) }
  }

  @Test
  fun testCreateFirstWorkspaceWithoutMaximumWorkspacesEntitlementIsAllowed() {
    every { entitlementService.getNumericEntitlement(any(), any()) } returns
      NumericEntitlementResult(
        featureId = MaximumWorkspacesEntitlement.featureId,
        hasAccess = false,
        value = null,
      )
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 0L
    stubSuccessfulWorkspaceCreate()

    val actualRead = getWorkspacesHandler(AirbyteEdition.CLOUD).createWorkspaceIfNotExist(generateWorkspaceCreateWithId())

    assertEquals(ORGANIZATION_ID, actualRead.organizationId)
    verify(exactly = 1) { workspaceService.writeWorkspaceWithSecrets(any()) }
  }

  @Test
  fun testCreateSecondWorkspaceWithoutMaximumWorkspacesEntitlementIsRejectedOnCloudPlan() {
    every { entitlementService.getNumericEntitlement(any(), any()) } returns
      NumericEntitlementResult(
        featureId = MaximumWorkspacesEntitlement.featureId,
        hasAccess = false,
        value = null,
      )
    every { entitlementService.getCurrentPlanId(OrganizationId(ORGANIZATION_ID)) } returns "plan-airbyte-standard"
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 1L

    val exception =
      assertThrows<WorkspaceLimitForOrganizationReachedProblem> {
        getWorkspacesHandler(AirbyteEdition.CLOUD).createWorkspaceIfNotExist(generateWorkspaceCreateWithId())
      }

    assertEquals(1L, (exception.problem.getData() as ProblemWorkspaceLimitData).limit)
    verify(exactly = 0) { workspaceService.writeWorkspaceWithSecrets(any()) }
  }

  @Test
  fun testCreateSecondWorkspaceWithoutMaximumWorkspacesEntitlementOrPlanIsAllowedOnCloud() {
    every { entitlementService.getNumericEntitlement(any(), any()) } returns
      NumericEntitlementResult(
        featureId = MaximumWorkspacesEntitlement.featureId,
        hasAccess = false,
        value = null,
      )
    every { entitlementService.getCurrentPlanId(OrganizationId(ORGANIZATION_ID)) } returns null
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 100L
    stubSuccessfulWorkspaceCreate()

    val actualRead = getWorkspacesHandler(AirbyteEdition.CLOUD).createWorkspaceIfNotExist(generateWorkspaceCreateWithId())

    assertEquals(ORGANIZATION_ID, actualRead.organizationId)
    verify(exactly = 0) { workspacePersistence.countWorkspacesByOrganizationId(any()) }
    verify(exactly = 1) { workspaceService.writeWorkspaceWithSecrets(any()) }
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class, names = ["COMMUNITY"])
  fun testCreateSecondWorkspaceWithoutMaximumWorkspacesEntitlementIsAllowedOutsideCloud(airbyteEdition: AirbyteEdition) {
    every { entitlementService.getNumericEntitlement(any(), any()) } returns
      NumericEntitlementResult(
        featureId = MaximumWorkspacesEntitlement.featureId,
        hasAccess = false,
        value = null,
      )
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 100L
    stubSuccessfulWorkspaceCreate()

    val actualRead = getWorkspacesHandler(airbyteEdition).createWorkspaceIfNotExist(generateWorkspaceCreateWithId())

    assertEquals(ORGANIZATION_ID, actualRead.organizationId)
    verify(exactly = 0) { entitlementService.getCurrentPlanId(any()) }
    verify(exactly = 0) { workspacePersistence.countWorkspacesByOrganizationId(any()) }
    verify(exactly = 1) { workspaceService.writeWorkspaceWithSecrets(any()) }
  }

  @Test
  fun testCreateWorkspaceAtLimitIsAllowedForInstanceAdmin() {
    every { entitlementService.getNumericEntitlement(any(), any()) } returns
      NumericEntitlementResult(
        featureId = MaximumWorkspacesEntitlement.featureId,
        hasAccess = true,
        value = 1L,
      )
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 1L
    every { roleRequest.roles() } returns setOf(AuthRoleConstants.ADMIN)
    stubSuccessfulWorkspaceCreate()

    val actualRead = getWorkspacesHandler(AirbyteEdition.CLOUD).createWorkspaceIfNotExist(generateWorkspaceCreateWithId())

    assertEquals(ORGANIZATION_ID, actualRead.organizationId)
    verify(exactly = 1) { workspaceService.writeWorkspaceWithSecrets(any()) }
  }

  @Test
  fun testReCreatingExistingWorkspaceAtLimitIsAllowed() {
    every { entitlementService.getNumericEntitlement(any(), any()) } returns
      NumericEntitlementResult(
        featureId = MaximumWorkspacesEntitlement.featureId,
        hasAccess = true,
        value = 1L,
      )
    every { workspacePersistence.workspaceExists(any()) } returns true
    every { roleRequest.roles() } returns emptySet()
    stubSuccessfulWorkspaceCreate()

    val actualRead = getWorkspacesHandler(AirbyteEdition.CLOUD).createWorkspaceIfNotExist(generateWorkspaceCreateWithId())

    assertEquals(ORGANIZATION_ID, actualRead.organizationId)
    verify(exactly = 0) { workspacePersistence.countWorkspacesByOrganizationId(any()) }
    verify(exactly = 1) { workspaceService.writeWorkspaceWithSecrets(any()) }
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  fun testCreateWorkspaceDuplicateSlug(airbyteEdition: AirbyteEdition) {
    every {
      workspaceService.getWorkspaceBySlugOptional(
        any(),
        true,
      )
    } returnsMany
      listOf(Optional.of<StandardWorkspace>(workspace), Optional.of<StandardWorkspace>(workspace), Optional.empty<StandardWorkspace>())
    every {
      workspaceService.getStandardWorkspaceNoSecrets(
        any(),
        false,
      )
    } returns workspace

    val dataplaneGroupId = UUID.randomUUID()
    every { dataplaneGroupService.getDefaultDataplaneGroup() } returns DataplaneGroup().withId(dataplaneGroupId)

    val uuid = UUID.randomUUID()
    every { uuidSupplier.get() } returns uuid

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val workspaceCreate =
      WorkspaceCreate(
        name = workspace.name,
        email = TEST_EMAIL,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = mutableListOf<io.airbyte.api.server.generated.models.Notification>(),
        organizationId = ORGANIZATION_ID,
      )

    val actualRead = getWorkspacesHandler(airbyteEdition).createWorkspace(workspaceCreate)
    val expectedRead =
      WorkspaceRead(
        workspaceId = uuid,
        customerId = uuid,
        email = TEST_EMAIL,
        name = workspace.name,
        slug = workspace.slug,
        initialSetupComplete = false,
        displaySetupWizard = false,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = mutableListOf<io.airbyte.api.server.generated.models.Notification>(),
        notificationSettings = generateDefaultApiNotificationSettings(),
        dataplaneGroupId = dataplaneGroupId,
        webhookConfigs = mutableListOf<WebhookConfigRead>(),
        tombstone = false,
        organizationId = ORGANIZATION_ID,
      )

    assertTrue(actualRead.slug.startsWith(workspace.slug))
    assertNotEquals(workspace.slug, actualRead.slug)
    assertEquals(expectedRead.copy(slug = ""), actualRead.copy(slug = ""))
    val slugs = mutableListOf<String>()
    verify(exactly = 3) { workspaceService.getWorkspaceBySlugOptional(capture(slugs), true) }
    assertEquals(3, slugs.size)
    assertEquals(workspace.slug, slugs[0])
    assertTrue(slugs[1].startsWith(workspace.slug))
    assertTrue(slugs[2].startsWith(workspace.slug))
  }

  @Test
  fun testDeleteWorkspace() {
    val workspaceIdRequestBody = WorkspaceIdRequestBody(workspaceId = workspace.workspaceId)

    val connection =
      ConnectionRead(
        connectionId = UUID.randomUUID(),
        name = "connection",
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        syncCatalog =
          io.airbyte.api.server.generated.models
            .AirbyteCatalog(streams = emptyList()),
        status = io.airbyte.api.server.generated.models.ConnectionStatus.ACTIVE,
        breakingChange = false,
      )
    val destination =
      DestinationRead(
        destinationDefinitionId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        workspaceId = workspace.workspaceId,
        connectionConfiguration = Jsons.emptyObject(),
        name = "destination",
        destinationName = "destination-definition",
        createdAt = 1L,
      )
    val source =
      SourceRead(
        sourceDefinitionId = UUID.randomUUID(),
        sourceId = UUID.randomUUID(),
        workspaceId = workspace.workspaceId,
        connectionConfiguration = Jsons.emptyObject(),
        name = "source",
        sourceName = "source-definition",
        createdAt = 1L,
      )

    every { workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false) } returns workspace

    every { workspaceService.listStandardWorkspaces(false) } returns
      mutableListOf(
        workspace,
      )

    every {
      connectionsHandler.listConnectionsForWorkspace(
        io.airbyte.api.server.generated.models
          .WorkspaceIdRequestBody(workspaceId = workspaceIdRequestBody.workspaceId),
      )
    } returns ConnectionReadList(connections = listOf(connection))

    every {
      destinationHandler
        .listDestinationsForWorkspace(ActorListCursorPaginatedRequestBody(workspaceId = workspaceIdRequestBody.workspaceId))
    } returns
      DestinationReadList(destinations = listOf(destination))

    every {
      sourceHandler.listSourcesForWorkspace(
        ActorListCursorPaginatedRequestBody(workspaceId = workspaceIdRequestBody.workspaceId),
      )
    } returns SourceReadList(sources = listOf(source))
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 2L
    stubTombstone(WorkspaceTombstoneResult.TOMBSTONED, allowLastWorkspace = false)

    getWorkspacesHandler(AirbyteEdition.COMMUNITY).deleteWorkspace(workspaceIdRequestBody)

    verify { workspacePersistence.tombstoneWorkspace(workspace.workspaceId, ORGANIZATION_ID, false) }
    verify { connectionsHandler.deleteConnection(connection.connectionId) }
    verify { destinationHandler.deleteDestination(destination) }
    verify { sourceHandler.deleteSource(source) }
  }

  @Test
  fun `deleteWorkspace rejects the last workspace before deleting anything`() {
    val workspaceIdRequestBody = stubEmptyWorkspaceForDeletion()
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 1L

    val problem =
      assertThrows<StateConflictProblem> {
        getWorkspacesHandler(AirbyteEdition.CLOUD).deleteWorkspace(workspaceIdRequestBody)
      }

    assertEquals(LAST_WORKSPACE_DELETE_MESSAGE, problem.problem.getDetail())
    verify { connectionsHandler wasNot Called }
    verify { destinationHandler wasNot Called }
    verify { sourceHandler wasNot Called }
    verify(exactly = 0) { workspacePersistence.tombstoneWorkspace(any(), any(), any()) }
  }

  @Test
  fun `deleteWorkspace returns 409 when a concurrent delete took the second-to-last workspace`() {
    val workspaceIdRequestBody = stubEmptyWorkspaceForDeletion()
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 2L
    stubTombstone(WorkspaceTombstoneResult.LAST_WORKSPACE, allowLastWorkspace = false)

    val problem =
      assertThrows<StateConflictProblem> {
        getWorkspacesHandler(AirbyteEdition.CLOUD).deleteWorkspace(workspaceIdRequestBody)
      }

    assertEquals(LAST_WORKSPACE_DELETE_MESSAGE, problem.problem.getDetail())
  }

  @Test
  fun `deleteWorkspace returns 409 when the workspace changed during deletion`() {
    val workspaceIdRequestBody = stubEmptyWorkspaceForDeletion()
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 2L
    stubTombstone(WorkspaceTombstoneResult.NOT_FOUND, allowLastWorkspace = false)

    val problem =
      assertThrows<StateConflictProblem> {
        getWorkspacesHandler(AirbyteEdition.CLOUD).deleteWorkspace(workspaceIdRequestBody)
      }

    assertEquals(WORKSPACE_DELETE_CONFLICT_MESSAGE, problem.problem.getDetail())
  }

  @Test
  fun `deleteWorkspace allows an instance admin to delete the last workspace`() {
    val workspaceIdRequestBody = stubEmptyWorkspaceForDeletion()
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 1L
    every { roleRequest.roles() } returns setOf(AuthRoleConstants.ADMIN)
    stubTombstone(WorkspaceTombstoneResult.TOMBSTONED, allowLastWorkspace = true)

    getWorkspacesHandler(AirbyteEdition.CLOUD).deleteWorkspace(workspaceIdRequestBody)

    verify { workspacePersistence.tombstoneWorkspace(workspace.workspaceId, ORGANIZATION_ID, true) }
  }

  private fun stubTombstone(
    result: WorkspaceTombstoneResult,
    allowLastWorkspace: Boolean,
  ) {
    every { workspacePersistence.tombstoneWorkspace(workspace.workspaceId, ORGANIZATION_ID, allowLastWorkspace) } returns result
  }

  private fun stubEmptyWorkspaceForDeletion(): WorkspaceIdRequestBody {
    val workspaceIdRequestBody = WorkspaceIdRequestBody(workspaceId = workspace.workspaceId)
    every { workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false) } returns workspace
    every {
      connectionsHandler.listConnectionsForWorkspace(
        io.airbyte.api.server.generated.models
          .WorkspaceIdRequestBody(workspaceId = workspaceIdRequestBody.workspaceId),
      )
    } returns ConnectionReadList(connections = mutableListOf())
    every { destinationHandler.listDestinationsForWorkspace(any()) } returns DestinationReadList(destinations = emptyList())
    every { sourceHandler.listSourcesForWorkspace(any()) } returns SourceReadList(sources = emptyList())
    return workspaceIdRequestBody
  }

  @Test
  fun testListWorkspaces() {
    val workspace2 = generateWorkspace()

    every { workspaceService.listStandardWorkspaces(false) } returns
      listOf(
        workspace,
        workspace2,
      )

    val expectedWorkspaceRead1 =
      WorkspaceRead(
        workspaceId = workspace.workspaceId,
        customerId = workspace.customerId,
        email = workspace.email,
        name = workspace.name,
        slug = workspace.slug,
        initialSetupComplete = workspace.initialSetupComplete,
        displaySetupWizard = workspace.displaySetupWizard,
        news = workspace.news,
        anonymousDataCollection = workspace.anonymousDataCollection,
        securityUpdates = workspace.securityUpdates,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettingsWithDefaultValue(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        organizationId = ORGANIZATION_ID,
        tombstone = false,
      )

    val expectedWorkspaceRead2 =
      WorkspaceRead(
        workspaceId = workspace2.workspaceId,
        customerId = workspace2.customerId,
        email = workspace2.email,
        name = workspace2.name,
        slug = workspace2.slug,
        initialSetupComplete = workspace2.initialSetupComplete,
        displaySetupWizard = workspace2.displaySetupWizard,
        news = workspace2.news,
        anonymousDataCollection = workspace2.anonymousDataCollection,
        securityUpdates = workspace2.securityUpdates,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettingsWithDefaultValue(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        organizationId = ORGANIZATION_ID,
        tombstone = false,
      )

    val actualWorkspaceReadList = getWorkspacesHandler(AirbyteEdition.COMMUNITY).listWorkspaces()

    assertEquals(
      listOf<WorkspaceRead?>(expectedWorkspaceRead1, expectedWorkspaceRead2),
      actualWorkspaceReadList.workspaces,
    )
  }

  @Test
  fun testGetWorkspace() {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
    every { workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false) } returns workspace

    val workspaceIdRequestBody = WorkspaceIdRequestBody(workspaceId = workspace.workspaceId)

    val workspaceRead =
      WorkspaceRead(
        workspaceId = workspace.workspaceId,
        customerId = workspace.customerId,
        email = TEST_EMAIL,
        name = TEST_WORKSPACE_NAME,
        slug = TEST_WORKSPACE_SLUG,
        initialSetupComplete = false,
        displaySetupWizard = true,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettingsWithDefaultValue(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        webhookConfigs = listOf<WebhookConfigRead>(WebhookConfigRead(id = WEBHOOK_CONFIG_ID, name = TEST_NAME)),
        organizationId = ORGANIZATION_ID,
        tombstone = false,
      )

    assertEquals(workspaceRead, getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspace(workspaceIdRequestBody))
  }

  @Test
  fun testGetWorkspaceBySlug() {
    every { workspaceService.getWorkspaceBySlug("default", false) } returns workspace

    val slugRequestBody = SlugRequestBody(slug = "default")
    val workspaceRead = getWorkspaceReadPerWorkspace(workspace)

    assertEquals(workspaceRead, getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceBySlug(slugRequestBody))
  }

  @Test
  fun testGetWorkspaceByConnectionId() {
    val connectionId = UUID.randomUUID()
    every { workspaceService.getStandardWorkspaceFromConnection(connectionId, false) } returns workspace
    val connectionIdRequestBody = ConnectionIdRequestBody(connectionId = connectionId)
    val workspaceRead = getWorkspaceReadPerWorkspace(workspace)

    assertEquals(
      workspaceRead,
      getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceByConnectionId(connectionIdRequestBody, false),
    )
  }

  private fun getWorkspaceReadPerWorkspace(workspace: StandardWorkspace): WorkspaceRead =
    WorkspaceRead(
      workspaceId = workspace.workspaceId,
      customerId = workspace.customerId,
      email = TEST_EMAIL,
      name = workspace.name,
      slug = workspace.slug,
      initialSetupComplete = workspace.initialSetupComplete,
      displaySetupWizard = workspace.displaySetupWizard,
      news = workspace.news,
      anonymousDataCollection = workspace.anonymousDataCollection,
      securityUpdates = workspace.securityUpdates,
      notifications = toApiList(workspace.notifications),
      notificationSettings = toApi(workspace.notificationSettings),
      dataplaneGroupId = DATAPLANE_GROUP_ID_1,
      organizationId = ORGANIZATION_ID,
      tombstone = workspace.tombstone,
    )

  @Test
  fun testGetWorkspaceByConnectionIdOnConfigNotFound() {
    val connectionId = UUID.randomUUID()
    every { workspaceService.getStandardWorkspaceFromConnection(connectionId, false) } throws
      ConfigNotFoundException("something", connectionId.toString())
    val connectionIdRequestBody = ConnectionIdRequestBody(connectionId = connectionId)
    assertThrows(
      ConfigNotFoundException::class.java,
    ) { getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceByConnectionId(connectionIdRequestBody, false) }
  }

  @ParameterizedTest
  @CsvSource(
    "true, ENABLED, true",
    "false, ENABLED, true",
    "true, DISABLED, false",
    "false, DISABLED, false",
    "true, NOT_CONFIGURED, false",
    "false, NOT_CONFIGURED, false",
  )
  fun testGetWorkspaceOrganizationInfo(
    isSso: Boolean,
    scimStatus: ScimConfigurationStatus,
    expectedScim: Boolean,
  ) {
    val organization = generateOrganization(if (isSso) "test-realm" else null)
    val workspaceIdRequestBody = WorkspaceIdRequestBody(workspaceId = workspace.workspaceId)

    every { organizationService.getOrganizationForWorkspaceId(workspace.workspaceId) } returns Optional.of<Organization>(organization)
    every { scimConfigurationService.getConfiguration(OrganizationId(organization.organizationId)) } returns
      ScimConfigurationRead(status = scimStatus)

    val orgInfo =
      getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceOrganizationInfo(workspaceIdRequestBody)

    assertEquals(organization.organizationId, orgInfo.organizationId)
    assertEquals(organization.name, orgInfo.organizationName)
    assertEquals(isSso, orgInfo.sso) // sso is true if ssoRealm is set
    assertEquals(expectedScim, orgInfo.scim)
    // Pins the org id passed to the SCIM lookup: without this, the false-expecting rows would also
    // pass against the setUp catch-all stub if the handler looked up the wrong organization.
    verify { scimConfigurationService.getConfiguration(OrganizationId(organization.organizationId)) }
  }

  @Test
  fun testGetWorkspaceOrganizationInfoReportsScimDisabledWhenAccessRevoked() {
    val organization = generateOrganization("test-realm")
    val workspaceIdRequestBody = WorkspaceIdRequestBody(workspaceId = workspace.workspaceId)

    every { organizationService.getOrganizationForWorkspaceId(workspace.workspaceId) } returns Optional.of<Organization>(organization)
    every { scimConfigurationService.getConfiguration(OrganizationId(organization.organizationId)) } returns
      ScimConfigurationRead(status = ScimConfigurationStatus.ENABLED)
    every { scimAccessGate.isAllowedForOrganizationInfo(OrganizationId(organization.organizationId)) } returns false

    val orgInfo =
      getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceOrganizationInfo(workspaceIdRequestBody)

    assertEquals(false, orgInfo.scim)
  }

  @Test
  fun testGetWorkspaceOrganizationInfoReportsScimDisabledWhenLookupFails() {
    val organization = generateOrganization(null)
    val workspaceIdRequestBody = WorkspaceIdRequestBody(workspaceId = workspace.workspaceId)

    every { organizationService.getOrganizationForWorkspaceId(workspace.workspaceId) } returns Optional.of<Organization>(organization)
    every { scimConfigurationService.getConfiguration(OrganizationId(organization.organizationId)) } throws
      IllegalStateException("Unsupported SCIM IdP provider: jumpcloud")

    val orgInfo =
      getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceOrganizationInfo(workspaceIdRequestBody)

    assertEquals(organization.organizationId, orgInfo.organizationId)
    assertEquals(false, orgInfo.scim)
  }

  @Test
  fun testGerWorkspaceOrganizationInfoConfigNotFound() {
    val workspaceIdRequestBody = WorkspaceIdRequestBody(workspaceId = workspace.workspaceId)

    every { organizationService.getOrganizationForWorkspaceId(workspace.workspaceId) } returns Optional.empty<Organization>()

    assertThrows(
      ConfigNotFoundException::class.java,
    ) { getWorkspacesHandler(AirbyteEdition.COMMUNITY).getWorkspaceOrganizationInfo(workspaceIdRequestBody) }
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  fun testUpdateWorkspace(airbyteEdition: AirbyteEdition) {
    val apiNotification = generateApiNotification(UPDATED)
    val workspaceUpdate =
      WorkspaceUpdate(
        workspaceId = workspace.workspaceId,
        anonymousDataCollection = true,
        securityUpdates = false,
        news = false,
        name = NEW_WORKSPACE,
        initialSetupComplete = true,
        displaySetupWizard = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(apiNotification),
        notificationSettings = generateApiNotificationSettings(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        webhookConfigs = listOf<WebhookConfigWrite>(WebhookConfigWrite(name = TEST_NAME, authToken = "test-auth-token")),
      )

    val expectedNotification = generateNotification()
    expectedNotification.slackConfiguration.withWebhook(UPDATED)

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

    every { uuidSupplier.get() } returns WEBHOOK_CONFIG_ID

    // Mock dataplane group validation for the update
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    every { dataplaneGroupService.listDefaultDataplaneGroups() } returns defaultDataplaneGroups

    every { workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false) } returnsMany listOf(workspace, expectedWorkspace)

    every { secretPersistence.read(any()) } returns ""

    val actualWorkspaceRead = getWorkspacesHandler(airbyteEdition).updateWorkspace(workspaceUpdate)

    val expectedNotificationRead = generateApiNotification(UPDATED)

    val expectedWorkspaceRead =
      WorkspaceRead(
        workspaceId = workspace.workspaceId,
        customerId = workspace.customerId,
        email = TEST_EMAIL,
        name = NEW_WORKSPACE,
        slug = NEW_WORKSPACE_SLUG,
        initialSetupComplete = true,
        displaySetupWizard = false,
        news = false,
        anonymousDataCollection = true,
        securityUpdates = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(expectedNotificationRead),
        notificationSettings = generateApiNotificationSettingsWithDefaultValue(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        webhookConfigs = listOf<WebhookConfigRead>(WebhookConfigRead(name = TEST_NAME, id = WEBHOOK_CONFIG_ID)),
        organizationId = ORGANIZATION_ID,
        tombstone = false,
      )

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

    verify { workspaceService.writeWorkspaceWithSecrets(expectedWorkspaceWithSecrets) }

    assertEquals(expectedWorkspaceRead, actualWorkspaceRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  fun testUpdateWorkspaceWithoutWebhookConfigs(airbyteEdition: AirbyteEdition) {
    val apiNotification = generateApiNotification(UPDATED)
    val workspaceUpdate =
      WorkspaceUpdate(workspaceId = workspace.workspaceId, anonymousDataCollection = false)

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
        .withOrganizationId(ORGANIZATION_ID)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_1)
        .withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)

    every { uuidSupplier.get() } returns WEBHOOK_CONFIG_ID

    every { workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false) } returnsMany
      listOf(expectedWorkspace, expectedWorkspace.withAnonymousDataCollection(false))

    getWorkspacesHandler(airbyteEdition).updateWorkspace(workspaceUpdate)

    verify { workspaceService.writeStandardWorkspaceNoSecrets(expectedWorkspace) }
  }

  @Test
  @DisplayName("Updating workspace name should update name and slug")
  fun testUpdateWorkspaceNoNameUpdate() {
    val workspaceUpdate =
      WorkspaceUpdateName(workspaceId = workspace.workspaceId, name = "New Workspace Name")

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

    every { workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false) } returnsMany listOf(workspace, expectedWorkspace)

    val actualWorkspaceRead = getWorkspacesHandler(AirbyteEdition.COMMUNITY).updateWorkspaceName(workspaceUpdate)

    val expectedWorkspaceRead =
      WorkspaceRead(
        workspaceId = workspace.workspaceId,
        customerId = workspace.customerId,
        email = TEST_EMAIL,
        name = "New Workspace Name",
        slug = "new-workspace-name",
        initialSetupComplete = workspace.initialSetupComplete,
        displaySetupWizard = workspace.displaySetupWizard,
        news = workspace.news,
        anonymousDataCollection = workspace.anonymousDataCollection,
        securityUpdates = workspace.securityUpdates,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettingsWithDefaultValue(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        organizationId = ORGANIZATION_ID,
        tombstone = false,
      )

    verify { workspaceService.writeStandardWorkspaceNoSecrets(expectedWorkspace) }

    assertEquals(expectedWorkspaceRead, actualWorkspaceRead)
  }

  @Test
  @DisplayName("Update organization in workspace")
  fun testWorkspaceUpdateOrganization() {
    val newOrgId = UUID.randomUUID()
    val workspaceUpdateOrganization =
      WorkspaceUpdateOrganization(workspaceId = workspace.workspaceId, organizationId = newOrgId)
    val expectedWorkspace = clone(workspace).withOrganizationId(newOrgId)
    every { workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false) } returns expectedWorkspace
    every { workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false) } returns expectedWorkspace
    // The same as the original workspace, with only the organization ID updated.
    val expectedWorkspaceRead =
      WorkspaceRead(
        workspaceId = workspace.workspaceId,
        customerId = workspace.customerId,
        email = workspace.email,
        name = workspace.name,
        slug = workspace.slug,
        initialSetupComplete = workspace.initialSetupComplete,
        displaySetupWizard = workspace.displaySetupWizard,
        news = workspace.news,
        anonymousDataCollection = false,
        securityUpdates = workspace.securityUpdates,
        notifications = toApiList(workspace.notifications),
        notificationSettings = toApi(workspace.notificationSettings),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        organizationId = newOrgId,
        tombstone = false,
      )

    val actualWorkspaceRead =
      getWorkspacesHandler(AirbyteEdition.COMMUNITY).updateWorkspaceOrganization(workspaceUpdateOrganization)
    verify { workspaceService.writeStandardWorkspaceNoSecrets(expectedWorkspace) }
    verify { workspaceHelper.invalidateWorkspaceOrganization(workspace.workspaceId) }
    assertEquals(expectedWorkspaceRead, actualWorkspaceRead)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @DisplayName("Partial patch update should preserve unchanged fields")
  fun testWorkspacePatchUpdate(airbyteEdition: AirbyteEdition) {
    val expectedNewEmail = "expected-new-email@example.com"
    val workspaceUpdate =
      WorkspaceUpdate(
        workspaceId = workspace.workspaceId,
        anonymousDataCollection = true,
        dataplaneGroupId = DATAPLANE_GROUP_ID_2,
        email = expectedNewEmail,
      )

    // Mock dataplane group validation for the update
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_2).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    every { dataplaneGroupService.listDefaultDataplaneGroups() } returns defaultDataplaneGroups

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

    every { workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false) } returnsMany listOf(workspace, expectedWorkspace)

    // The same as the original workspace, with only the email and data collection flags changed.
    val expectedWorkspaceRead =
      WorkspaceRead(
        workspaceId = workspace.workspaceId,
        customerId = workspace.customerId,
        email = expectedNewEmail,
        name = workspace.name,
        slug = workspace.slug,
        initialSetupComplete = workspace.initialSetupComplete,
        displaySetupWizard = workspace.displaySetupWizard,
        news = workspace.news,
        anonymousDataCollection = true,
        securityUpdates = workspace.securityUpdates,
        notifications = toApiList(workspace.notifications),
        notificationSettings = toApi(workspace.notificationSettings),
        dataplaneGroupId = DATAPLANE_GROUP_ID_2,
        organizationId = ORGANIZATION_ID,
        webhookConfigs = mutableListOf<WebhookConfigRead>(),
        tombstone = false,
      )

    val actualWorkspaceRead = getWorkspacesHandler(airbyteEdition).updateWorkspace(workspaceUpdate)
    verify { workspaceService.writeStandardWorkspaceNoSecrets(expectedWorkspace) }
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
      WorkspaceUpdate(
        workspaceId = existingWorkspace.workspaceId,
        notificationsConfig =
          NotificationsConfig(
            success =
              NotificationConfig(
                webhook = WebhookNotificationConfig(enabled = true, url = "http://foo.bar/success"),
              ),
            failure = NotificationConfig(webhook = WebhookNotificationConfig(enabled = false)),
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

    every { workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false) } returnsMany listOf(existingWorkspace, expectedWorkspace)

    getWorkspacesHandler(AirbyteEdition.COMMUNITY).updateWorkspace(workspaceUpdate)
    verify { workspaceService.writeStandardWorkspaceNoSecrets(expectedWorkspace) }
  }

  @Test
  fun testSetFeedbackDone() {
    val workspaceGiveFeedback =
      WorkspaceGiveFeedback(workspaceId = UUID.randomUUID())

    getWorkspacesHandler(AirbyteEdition.COMMUNITY).setFeedbackDone(workspaceGiveFeedback)

    verify { workspaceService.setFeedback(workspaceGiveFeedback.workspaceId) }
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
        workspaceHelper,
        dataplaneGroupService,
        entitlementService,
        trackingClient,
        limitsProvider,
        consumptionService,
        ffClient,
        airbyteEdition,
        roleResolver,
        scimConfigurationService,
        scimAccessGate,
      )

    val uuid = UUID.randomUUID()
    every { uuidSupplier.get() } returns uuid

    // Mock dataplane group validation for creation
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    every { dataplaneGroupService.listDefaultDataplaneGroups() } returns defaultDataplaneGroups

    val workspaceCreate =
      WorkspaceCreate(
        name = NEW_WORKSPACE,
        email = TEST_EMAIL,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettings(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        organizationId = ORGANIZATION_ID,
      )

    val actualRead = workspacesHandler.createWorkspace(workspaceCreate)
    val expectedRead =
      WorkspaceRead(
        workspaceId = uuid,
        customerId = uuid,
        email = TEST_EMAIL,
        name = NEW_WORKSPACE,
        slug = NEW_WORKSPACE_SLUG,
        initialSetupComplete = false,
        displaySetupWizard = false,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettingsWithDefaultValue(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_1,
        webhookConfigs = mutableListOf<WebhookConfigRead>(),
        tombstone = false,
        organizationId = ORGANIZATION_ID,
      )

    assertEquals(expectedRead, actualRead)
    verify(exactly = 1) { workspaceService.writeWorkspaceWithSecrets(any()) }
  }

  @Test
  fun testListWorkspacesInOrgNoKeyword() {
    val request =
      ListWorkspacesInOrganizationRequestBody(
        organizationId = ORGANIZATION_ID,
        pagination = Pagination(pageSize = 100, rowOffset = 0),
      )
    val expectedWorkspaces = listOf(generateWorkspace(), generateWorkspace())

    every {
      workspacePersistence.listWorkspacesByOrganizationIdPaginated(
        ResourcesByOrganizationQueryPaginated(ORGANIZATION_ID, false, 100, 0),
        Optional.empty<String>(),
      )
    } returns expectedWorkspaces
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 2L
    val result = getWorkspacesHandler(AirbyteEdition.COMMUNITY).listWorkspacesInOrganization(request)
    assertEquals(2, result.workspaces.size)
    assertEquals(2L, result.totalWorkspaceCount)
  }

  @Test
  fun testListWorkspacesInOrgWithKeyword() {
    val request =
      ListWorkspacesInOrganizationRequestBody(
        organizationId = ORGANIZATION_ID,
        nameContains = "nameContains",
        pagination = Pagination(pageSize = 100, rowOffset = 0),
      )
    val expectedWorkspaces = listOf(generateWorkspace(), generateWorkspace())

    every {
      workspacePersistence.listWorkspacesByOrganizationIdPaginated(
        ResourcesByOrganizationQueryPaginated(ORGANIZATION_ID, false, 100, 0),
        Optional.of<String>("nameContains"),
      )
    } returns expectedWorkspaces
    // The total count ignores the keyword filter and counts all workspaces in the organization.
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 5L
    val result = getWorkspacesHandler(AirbyteEdition.COMMUNITY).listWorkspacesInOrganization(request)
    assertEquals(2, result.workspaces.size)
    assertEquals(5L, result.totalWorkspaceCount)
  }

  @Test
  fun testListWorkspacesInOrgForUserNoKeyword() {
    val userId = UUID.randomUUID()
    val request =
      ListWorkspacesInOrganizationRequestBody(
        organizationId = ORGANIZATION_ID,
        pagination = Pagination(pageSize = 100, rowOffset = 0),
      )
    val expectedWorkspaces = listOf(generateWorkspace(), generateWorkspace())
    every {
      workspacePersistence.listWorkspacesInOrganizationByUserIdPaginated(
        ResourcesByOrganizationQueryPaginated(ORGANIZATION_ID, false, 100, 0),
        userId,
        Optional.empty<String>(),
      )
    } returns expectedWorkspaces
    // The user can only see 2 of the organization's 3 workspaces, but the total counts all of them.
    every { workspacePersistence.countWorkspacesByOrganizationId(ORGANIZATION_ID) } returns 3L
    val result = getWorkspacesHandler(AirbyteEdition.COMMUNITY).listWorkspacesInOrganizationForUser(userId, request)
    assertEquals(2, result.workspaces.size)
    assertEquals(3L, result.totalWorkspaceCount)
  }

  @Test
  fun testListWorkspacesInOrgForUserWithKeyword() {
    val userId = UUID.randomUUID()
    val request =
      ListWorkspacesInOrganizationRequestBody(
        organizationId = ORGANIZATION_ID,
        nameContains = "test",
        pagination = Pagination(pageSize = 100, rowOffset = 0),
      )
    val expectedWorkspaces = listOf(generateWorkspace())
    every {
      workspacePersistence.listWorkspacesInOrganizationByUserIdPaginated(
        ResourcesByOrganizationQueryPaginated(ORGANIZATION_ID, false, 100, 0),
        userId,
        Optional.of<String>("test"),
      )
    } returns expectedWorkspaces
    val result = getWorkspacesHandler(AirbyteEdition.COMMUNITY).listWorkspacesInOrganizationForUser(userId, request)
    assertEquals(1, result.workspaces.size)
  }

  @Test
  fun testListWorkspacesInOrgForUserNoPagination() {
    val userId = UUID.randomUUID()
    val request = ListWorkspacesInOrganizationRequestBody(organizationId = ORGANIZATION_ID)
    val expectedWorkspaces = listOf(generateWorkspace(), generateWorkspace())
    every {
      workspacePersistence.listWorkspacesInOrganizationByUserId(
        ORGANIZATION_ID,
        userId,
        Optional.empty<String>(),
      )
    } returns expectedWorkspaces
    val result = getWorkspacesHandler(AirbyteEdition.COMMUNITY).listWorkspacesInOrganizationForUser(userId, request)
    assertEquals(2, result.workspaces.size)
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @DisplayName("Creating workspace with explicit dataplane group requires org editor permission")
  fun testCreateWorkspaceWithDataplaneGroupRequiresOrgEditor(airbyteEdition: AirbyteEdition) {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
    every {
      workspaceService.getStandardWorkspaceNoSecrets(
        any(),
        false,
      )
    } returns workspace

    val uuid = UUID.randomUUID()
    every { uuidSupplier.get() } returns uuid

    // Mock that DATAPLANE_GROUP_ID_2 is not a default group
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    every { dataplaneGroupService.listDefaultDataplaneGroups() } returns defaultDataplaneGroups

    // Mock the dataplane group that belongs to the same organization
    val dataplaneGroup = DataplaneGroup().withId(DATAPLANE_GROUP_ID_2).withOrganizationId(ORGANIZATION_ID)
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID_2) } returns dataplaneGroup

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val workspaceCreate =
      WorkspaceCreate(
        name = NEW_WORKSPACE,
        email = TEST_EMAIL,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf<io.airbyte.api.server.generated.models.Notification>(generateApiNotification()),
        notificationSettings = generateApiNotificationSettings(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_2,
        webhookConfigs = listOf<WebhookConfigWrite>(WebhookConfigWrite(name = TEST_NAME, authToken = TEST_AUTH_TOKEN)),
        organizationId = ORGANIZATION_ID,
      )

    every { secretPersistence.read(any()) } returns ""

    getWorkspacesHandler(airbyteEdition).createWorkspace(workspaceCreate)

    // Verify that role check was performed
    verify { roleResolver.newRequest() }
    verify { roleRequest.withCurrentUser() }
    verify { roleRequest.withOrg(ORGANIZATION_ID) }
    verify { roleRequest.requireRole("ORGANIZATION_EDITOR") }
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
    every { dataplaneGroupService.listDefaultDataplaneGroups() } returns defaultDataplaneGroups

    // Mock the dataplane group that belongs to the same organization
    val dataplaneGroup = DataplaneGroup().withId(DATAPLANE_GROUP_ID_2).withOrganizationId(ORGANIZATION_ID)
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID_2) } returns dataplaneGroup

    val workspaceUpdate =
      WorkspaceUpdate(workspaceId = workspace.workspaceId, dataplaneGroupId = DATAPLANE_GROUP_ID_2) // Changing dataplane group

    val expectedWorkspace =
      clone(workspace)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_2)
        .withWebhookOperationConfigs(
          jsonNode<WebhookOperationConfigs?>(
            WebhookOperationConfigs()
              .withWebhookConfigs(mutableListOf<WebhookConfig?>()),
          ),
        )

    every { workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false) } returnsMany listOf(workspace, expectedWorkspace)

    getWorkspacesHandler(airbyteEdition).updateWorkspace(workspaceUpdate)

    // Verify that role check was performed
    verify { roleResolver.newRequest() }
    verify { roleRequest.withCurrentUser() }
    verify { roleRequest.withOrg(ORGANIZATION_ID) }
    verify { roleRequest.requireRole("ORGANIZATION_EDITOR") }
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @DisplayName("Creating workspace with default dataplane group should not require org editor permission")
  fun testCreateWorkspaceWithDefaultDataplaneGroupDoesNotRequireOrgEditor(airbyteEdition: AirbyteEdition) {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
    every {
      workspaceService.getStandardWorkspaceNoSecrets(
        any(),
        false,
      )
    } returns workspace

    val uuid = UUID.randomUUID()
    every { uuidSupplier.get() } returns uuid

    // Mock default dataplane group
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_2).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    every { dataplaneGroupService.listDefaultDataplaneGroups() } returns defaultDataplaneGroups

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val workspaceCreate =
      WorkspaceCreate(
        name = NEW_WORKSPACE,
        email = TEST_EMAIL,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf(generateApiNotification()),
        notificationSettings = generateApiNotificationSettings(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_2,
        webhookConfigs = listOf(WebhookConfigWrite(name = TEST_NAME, authToken = TEST_AUTH_TOKEN)),
        organizationId = ORGANIZATION_ID,
      )

    every { secretPersistence.read(any()) } returns ""

    getWorkspacesHandler(airbyteEdition).createWorkspace(workspaceCreate)

    // Verify that role check was still performed (but won't throw since it's a default group)
    verify { roleResolver.newRequest() }
    verify { roleRequest.withCurrentUser() }
    verify { roleRequest.withOrg(ORGANIZATION_ID) }
    verify { roleRequest.requireRole("ORGANIZATION_EDITOR") }
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  @DisplayName("Creating workspace with non-default dataplane group from wrong org should throw forbidden")
  fun testCreateWorkspaceWithWrongOrgDataplaneGroupThrowsForbidden(airbyteEdition: AirbyteEdition) {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
    every {
      workspaceService.getStandardWorkspaceNoSecrets(
        any(),
        false,
      )
    } returns workspace

    val uuid = UUID.randomUUID()
    every { uuidSupplier.get() } returns uuid

    // Mock that DATAPLANE_GROUP_ID_2 is not a default group
    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup().withId(DATAPLANE_GROUP_ID_1).withOrganizationId(io.airbyte.commons.DEFAULT_ORGANIZATION_ID),
      )
    every { dataplaneGroupService.listDefaultDataplaneGroups() } returns defaultDataplaneGroups

    // Mock that the dataplane group belongs to a different organization
    val wrongOrgId = UUID.randomUUID()
    val dataplaneGroupFromWrongOrg = DataplaneGroup().withId(DATAPLANE_GROUP_ID_2).withOrganizationId(wrongOrgId)
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID_2) } returns dataplaneGroupFromWrongOrg

    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val workspaceCreate =
      WorkspaceCreate(
        name = NEW_WORKSPACE,
        email = TEST_EMAIL,
        news = false,
        anonymousDataCollection = false,
        securityUpdates = false,
        notifications = listOf(generateApiNotification()),
        notificationSettings = generateApiNotificationSettings(),
        dataplaneGroupId = DATAPLANE_GROUP_ID_2,
        webhookConfigs = listOf(WebhookConfigWrite(name = TEST_NAME, authToken = TEST_AUTH_TOKEN)),
        organizationId = ORGANIZATION_ID,
      )

    every { secretPersistence.read(any()) } returns ""

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
    every { dataplaneGroupService.listDefaultDataplaneGroups() } returns defaultDataplaneGroups

    val workspaceUpdate =
      WorkspaceUpdate(workspaceId = workspace.workspaceId, dataplaneGroupId = DATAPLANE_GROUP_ID_2) // This is a default group

    val expectedWorkspace =
      clone(workspace)
        .withDataplaneGroupId(DATAPLANE_GROUP_ID_2)
        .withWebhookOperationConfigs(
          jsonNode<WebhookOperationConfigs?>(
            WebhookOperationConfigs()
              .withWebhookConfigs(mutableListOf<WebhookConfig?>()),
          ),
        )

    every { workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false) } returnsMany listOf(workspace, expectedWorkspace)

    getWorkspacesHandler(airbyteEdition).updateWorkspace(workspaceUpdate)

    // Verify that role check was still performed (but passed because it's a default group)
    verify { roleResolver.newRequest() }
    verify { roleRequest.withCurrentUser() }
    verify { roleRequest.withOrg(ORGANIZATION_ID) }
    verify { roleRequest.requireRole("ORGANIZATION_EDITOR") }
    verify { workspaceService.writeStandardWorkspaceNoSecrets(expectedWorkspace) }
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
    every { dataplaneGroupService.listDefaultDataplaneGroups() } returns defaultDataplaneGroups

    // Mock that the dataplane group belongs to a different organization
    val wrongOrgId = UUID.randomUUID()
    val dataplaneGroupFromWrongOrg = DataplaneGroup().withId(DATAPLANE_GROUP_ID_2).withOrganizationId(wrongOrgId)
    every { dataplaneGroupService.getDataplaneGroup(DATAPLANE_GROUP_ID_2) } returns dataplaneGroupFromWrongOrg

    val workspaceUpdate =
      WorkspaceUpdate(workspaceId = workspace.workspaceId, dataplaneGroupId = DATAPLANE_GROUP_ID_2) // This belongs to wrong org

    every { workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false) } returns workspace

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
