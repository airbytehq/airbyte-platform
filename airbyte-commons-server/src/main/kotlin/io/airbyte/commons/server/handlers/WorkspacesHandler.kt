/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Preconditions
import com.google.common.base.Strings
import io.airbyte.analytics.TrackingClient
import io.airbyte.api.model.generated.ActorListCursorPaginatedRequestBody
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.model.generated.ListWorkspacesByUserRequestBody
import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody
import io.airbyte.api.model.generated.NotificationConfig
import io.airbyte.api.model.generated.NotificationsConfig
import io.airbyte.api.model.generated.SlugRequestBody
import io.airbyte.api.model.generated.WorkspaceCreate
import io.airbyte.api.model.generated.WorkspaceCreateWithId
import io.airbyte.api.model.generated.WorkspaceGiveFeedback
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.model.generated.WorkspaceOrganizationInfoRead
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.api.model.generated.WorkspaceReadList
import io.airbyte.api.model.generated.WorkspaceUpdate
import io.airbyte.api.model.generated.WorkspaceUpdateName
import io.airbyte.api.model.generated.WorkspaceUpdateOrganization
import io.airbyte.commons.random.randomAlpha
import io.airbyte.commons.server.converters.NotificationConverter.toConfigList
import io.airbyte.commons.server.converters.NotificationSettingsConverter.toConfig
import io.airbyte.commons.server.converters.WorkspaceConverter.domainToApiModel
import io.airbyte.commons.server.converters.WorkspaceWebhookConfigsConverter.toPersistenceWrite
import io.airbyte.commons.server.errors.BadObjectSchemaKnownException
import io.airbyte.commons.server.errors.InternalServerKnownException
import io.airbyte.commons.server.errors.ValueConflictKnownException
import io.airbyte.commons.server.handlers.helpers.validateWorkspace
import io.airbyte.commons.server.limits.ConsumptionService
import io.airbyte.commons.server.limits.ProductLimitsProvider
import io.airbyte.commons.server.slug.slugify
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.Notification
import io.airbyte.config.NotificationItem
import io.airbyte.config.NotificationSettings
import io.airbyte.config.Organization
import io.airbyte.config.ScopeType
import io.airbyte.config.SlackNotificationConfiguration
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.helpers.patchNotificationSettingsWithDefaultValue
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ResourcesByOrganizationQueryPaginated
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated
import io.airbyte.data.services.shared.ResourcesQueryPaginated
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.HydrateLimits
import io.airbyte.featureflag.Workspace
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.util.CollectionUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.tools.StringUtils
import java.io.IOException
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier
import java.util.stream.Collectors

/**
 * WorkspacesHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
class WorkspacesHandler
  @VisibleForTesting
  constructor(
    private val workspacePersistence: WorkspacePersistence,
    private val organizationPersistence: OrganizationPersistence,
    private val permissionHandler: PermissionHandler,
    private val connectionsHandler: ConnectionsHandler,
    private val destinationHandler: DestinationHandler,
    private val sourceHandler: SourceHandler,
    @param:Named("uuidGenerator") private val uuidSupplier: Supplier<UUID>,
    private val workspaceService: WorkspaceService,
    private val dataplaneGroupService: DataplaneGroupService,
    private val trackingClient: TrackingClient,
    private val limitsProvider: ProductLimitsProvider,
    private val consumptionService: ConsumptionService,
    private val ffClient: FeatureFlagClient,
    private val airbyteEdition: AirbyteEdition,
  ) {
    @Throws(JsonValidationException::class, IOException::class, ValueConflictKnownException::class, ConfigNotFoundException::class)
    fun createWorkspace(workspaceCreate: WorkspaceCreate): WorkspaceRead {
      val workspaceCreateWithId =
        WorkspaceCreateWithId()
          .id(uuidSupplier.get())
          .organizationId(workspaceCreate.organizationId)
          .dataplaneGroupId(workspaceCreate.dataplaneGroupId)
          .displaySetupWizard(workspaceCreate.displaySetupWizard)
          .name(workspaceCreate.name)
          .notifications(workspaceCreate.notifications)
          .webhookConfigs(workspaceCreate.webhookConfigs)
          .anonymousDataCollection(workspaceCreate.anonymousDataCollection)
          .email(workspaceCreate.email)
          .news(workspaceCreate.news)
          .notificationSettings(workspaceCreate.notificationSettings)
          .securityUpdates(workspaceCreate.securityUpdates)

      return createWorkspaceIfNotExist(workspaceCreateWithId)
    }

    @Throws(JsonValidationException::class, IOException::class, ValueConflictKnownException::class, ConfigNotFoundException::class)
    fun createWorkspaceIfNotExist(workspaceCreateWithId: WorkspaceCreateWithId): WorkspaceRead {
      // We expect that the caller is specifying the workspace ID.
      // Since this code is currently only called by OSS, it's enforced in the public API and the UI
      // currently.

      if (workspaceCreateWithId.organizationId == null) {
        throw BadObjectSchemaKnownException("Workspace missing org ID.")
      }

      val email = workspaceCreateWithId.email
      val anonymousDataCollection = workspaceCreateWithId.anonymousDataCollection
      val news = workspaceCreateWithId.news
      val securityUpdates = workspaceCreateWithId.securityUpdates
      val displaySetupWizard = workspaceCreateWithId.displaySetupWizard

      // if not set on the workspaceCreate, set the default dataplane group ID
      val dataplaneGroupId: UUID
      if (workspaceCreateWithId.dataplaneGroupId == null) {
        val defaultDataplaneGroup = dataplaneGroupService.getDefaultDataplaneGroupForAirbyteEdition(airbyteEdition)
        dataplaneGroupId = defaultDataplaneGroup.id
      } else {
        dataplaneGroupId = workspaceCreateWithId.dataplaneGroupId
      }

      // NotificationSettings from input will be patched with default values.
      val notificationSettings =
        patchNotificationSettingsWithDefaultValue(toConfig(workspaceCreateWithId.notificationSettings))

      val workspace =
        StandardWorkspace()
          .withWorkspaceId(workspaceCreateWithId.id)
          .withCustomerId(uuidSupplier.get()) // "customer_id" should be deprecated
          .withName(workspaceCreateWithId.name)
          .withSlug(generateUniqueSlug(workspaceCreateWithId.name))
          .withInitialSetupComplete(false)
          .withAnonymousDataCollection(anonymousDataCollection ?: false)
          .withNews(news ?: false)
          .withSecurityUpdates(securityUpdates ?: false)
          .withDisplaySetupWizard(displaySetupWizard ?: false)
          .withTombstone(false)
          .withNotifications(toConfigList(workspaceCreateWithId.notifications))
          .withNotificationSettings(notificationSettings)
          .withDataplaneGroupId(dataplaneGroupId)
          .withWebhookOperationConfigs(toPersistenceWrite(workspaceCreateWithId.webhookConfigs, uuidSupplier))
          .withOrganizationId(workspaceCreateWithId.organizationId)

      if (!Strings.isNullOrEmpty(email)) {
        workspace.withEmail(email)
      }

      validateWorkspace(workspace, airbyteEdition)

      return persistStandardWorkspace(workspace)
    }

    @Throws(
      JsonValidationException::class,
      IOException::class,
      ConfigNotFoundException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun deleteWorkspace(workspaceIdRequestBody: WorkspaceIdRequestBody) {
      // get existing implementation
      val persistedWorkspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceIdRequestBody.workspaceId, false)

      // disable all connections associated with this workspace
      for (connectionRead in connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody).connections) {
        connectionsHandler.deleteConnection(connectionRead.connectionId)
      }

      // disable all destinations associated with this workspace
      for (destinationRead in destinationHandler
        .listDestinationsForWorkspace(
          ActorListCursorPaginatedRequestBody().workspaceId(workspaceIdRequestBody.workspaceId),
        ).destinations) {
        destinationHandler.deleteDestination(destinationRead)
      }

      // disable all sources associated with this workspace
      for (sourceRead in sourceHandler
        .listSourcesForWorkspace(
          ActorListCursorPaginatedRequestBody().workspaceId(workspaceIdRequestBody.workspaceId),
        ).sources) {
        sourceHandler.deleteSource(sourceRead)
      }
      persistedWorkspace.withTombstone(true)
      persistStandardWorkspace(persistedWorkspace)
    }

    @Throws(IOException::class)
    fun listWorkspaces(): WorkspaceReadList {
      val reads =
        workspaceService
          .listStandardWorkspaces(false)
          .stream()
          .map<WorkspaceRead> { obj: StandardWorkspace -> domainToApiModel(obj) }
          .collect(Collectors.toList<WorkspaceRead>())
      return WorkspaceReadList().workspaces(reads)
    }

    @Throws(IOException::class)
    fun listWorkspacesPaginated(listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody): WorkspaceReadList {
      val standardWorkspaces =
        workspaceService.listStandardWorkspacesPaginated(
          ResourcesQueryPaginated(
            listResourcesForWorkspacesRequestBody.workspaceIds,
            listResourcesForWorkspacesRequestBody.includeDeleted,
            listResourcesForWorkspacesRequestBody.pagination.pageSize,
            listResourcesForWorkspacesRequestBody.pagination.rowOffset,
            null,
          ),
        )

      val reads =
        standardWorkspaces
          .stream()
          .map<WorkspaceRead> { obj: StandardWorkspace -> domainToApiModel(obj) }
          .collect(Collectors.toList<WorkspaceRead>())
      return WorkspaceReadList().workspaces(reads)
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun getWorkspace(workspaceIdRequestBody: WorkspaceIdRequestBody): WorkspaceRead {
      val workspaceId = workspaceIdRequestBody.workspaceId
      val includeTombstone = if (workspaceIdRequestBody.includeTombstone != null) workspaceIdRequestBody.includeTombstone else false
      val workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, includeTombstone)
      val result = domainToApiModel(workspace)
      if (ffClient.boolVariation(HydrateLimits, Workspace(workspaceId))) {
        val limits = limitsProvider.getLimitForWorkspace(workspaceId)
        val consumption = consumptionService.getForWorkspace(workspaceId)
        result.workspaceLimits(domainToApiModel(limits, consumption))
      }
      return result
    }

    @Throws(IOException::class, ConfigNotFoundException::class)
    fun getWorkspaceOrganizationInfo(workspaceIdRequestBody: WorkspaceIdRequestBody): WorkspaceOrganizationInfoRead {
      val workspaceId = workspaceIdRequestBody.workspaceId
      val organization = organizationPersistence.getOrganizationByWorkspaceId(workspaceId)
      if (organization.isEmpty) {
        throw ConfigNotFoundException(io.airbyte.config.persistence.ConfigNotFoundException.NO_ORGANIZATION_FOR_WORKSPACE, workspaceId.toString())
      }
      return buildWorkspaceOrganizationInfoRead(organization.get())
    }

    @Suppress("unused")
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun getWorkspaceBySlug(slugRequestBody: SlugRequestBody): WorkspaceRead {
      // for now we assume there is one workspace and it has a default uuid.
      val workspace = workspaceService.getWorkspaceBySlug(slugRequestBody.slug, false)
      return domainToApiModel(workspace)
    }

    @Throws(ConfigNotFoundException::class)
    fun getWorkspaceByConnectionId(
      connectionIdRequestBody: ConnectionIdRequestBody,
      includeTombstone: Boolean,
    ): WorkspaceRead {
      val workspace =
        workspaceService.getStandardWorkspaceFromConnection(connectionIdRequestBody.connectionId, includeTombstone)
      return domainToApiModel(workspace)
    }

    @Throws(IOException::class)
    fun listWorkspacesInOrganization(request: ListWorkspacesInOrganizationRequestBody): WorkspaceReadList {
      val nameContains = if (StringUtils.isBlank(request.nameContains)) Optional.empty() else Optional.of(request.nameContains)
      val standardWorkspaces =
        if (request.pagination != null) {
          workspacePersistence
            .listWorkspacesByOrganizationIdPaginated(
              ResourcesByOrganizationQueryPaginated(
                request.organizationId,
                false,
                request.pagination.pageSize,
                request.pagination.rowOffset,
              ),
              nameContains,
            ).stream()
            .map<WorkspaceRead> { obj: StandardWorkspace -> domainToApiModel(obj) }
            .collect(Collectors.toList<WorkspaceRead>())
        } else {
          workspacePersistence
            .listWorkspacesByOrganizationId(request.organizationId, false, nameContains)
            .stream()
            .map<WorkspaceRead> { obj: StandardWorkspace -> domainToApiModel(obj) }
            .collect(Collectors.toList<WorkspaceRead>())
        }
      return WorkspaceReadList().workspaces(standardWorkspaces)
    }

    @Throws(IOException::class)
    private fun listWorkspacesByInstanceAdminUser(request: ListWorkspacesByUserRequestBody): WorkspaceReadList {
      val nameContains = if (StringUtils.isBlank(request.nameContains)) Optional.empty() else Optional.of(request.nameContains)
      val standardWorkspaces =
        if (request.pagination != null) {
          workspacePersistence
            .listWorkspacesByInstanceAdminUserPaginated(
              false,
              request.pagination.pageSize,
              request.pagination.rowOffset,
              nameContains,
            ).stream()
            .map<WorkspaceRead> { obj: StandardWorkspace -> domainToApiModel(obj) }
            .collect(Collectors.toList<WorkspaceRead>())
        } else {
          workspacePersistence
            .listWorkspacesByInstanceAdminUser(false, nameContains)
            .stream()
            .map<WorkspaceRead> { obj: StandardWorkspace -> domainToApiModel(obj) }
            .collect(Collectors.toList<WorkspaceRead>())
        }
      return WorkspaceReadList().workspaces(standardWorkspaces)
    }

    @Throws(IOException::class)
    fun listWorkspacesByUser(request: ListWorkspacesByUserRequestBody): WorkspaceReadList {
      // If user has instance_admin permission, list all workspaces.
      if (permissionHandler.isUserInstanceAdmin(request.userId)) {
        return listWorkspacesByInstanceAdminUser(request)
      }
      // User has no instance_admin permission.
      val nameContains = if (StringUtils.isBlank(request.nameContains)) Optional.empty() else Optional.of(request.nameContains)
      val standardWorkspaces =
        if (request.pagination != null) {
          workspacePersistence
            .listWorkspacesByUserIdPaginated(
              ResourcesByUserQueryPaginated(
                request.userId,
                false,
                request.pagination.pageSize,
                request.pagination.rowOffset,
              ),
              nameContains,
            ).stream()
            .map<WorkspaceRead> { obj: StandardWorkspace -> domainToApiModel(obj) }
            .collect(Collectors.toList<WorkspaceRead>())
        } else {
          workspacePersistence
            .listActiveWorkspacesByUserId(request.userId, nameContains)
            .stream()
            .map<WorkspaceRead> { obj: StandardWorkspace -> domainToApiModel(obj) }
            .collect(Collectors.toList<WorkspaceRead>())
        }
      return WorkspaceReadList().workspaces(standardWorkspaces)
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun updateWorkspace(workspacePatch: WorkspaceUpdate): WorkspaceRead {
      val workspaceId = workspacePatch.workspaceId

      log.debug { "Starting updateWorkspace for workspaceId $workspaceId..." }
      log.debug { "Incoming workspacePatch: $workspacePatch" }

      val workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)
      log.debug { "Initial workspace: $workspace" }
      log.debug { "Initial WorkspaceRead: ${domainToApiModel(workspace)}" }

      applyPatchToStandardWorkspace(workspace, workspacePatch)
      workspace.notificationSettings = patchNotificationSettingsWithDefaultValue(workspace.notificationSettings)
      validateWorkspacePatch(workspace, workspacePatch)
      validateWorkspace(workspace, airbyteEdition)

      log.debug { "Patched Workspace before persisting: $workspace" }

      if (CollectionUtils.isEmpty(workspacePatch.webhookConfigs)) {
        // We aren't persisting any secrets. It's safe (and necessary) to use the NoSecrets variant because
        // we never hydrated them in the first place.
        workspaceService.writeStandardWorkspaceNoSecrets(workspace)
      } else {
        // We're saving new webhook configs, so we need to persist the secrets.
        persistStandardWorkspace(workspace)
      }

      // after updating email or tracking info, we need to re-identify the instance.
      trackingClient.identify(workspaceId, ScopeType.WORKSPACE)

      return buildWorkspaceReadFromId(workspaceId)
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun updateWorkspaceName(workspaceUpdateName: WorkspaceUpdateName): WorkspaceRead {
      val workspaceId = workspaceUpdateName.workspaceId

      val persistedWorkspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)

      persistedWorkspace
        .withName(workspaceUpdateName.name)
        .withSlug(generateUniqueSlug(workspaceUpdateName.name))

      // NOTE: it's safe (and necessary) to use the NoSecrets variant because we never hydrated them in
      // the first place.
      workspaceService.writeStandardWorkspaceNoSecrets(persistedWorkspace)

      return buildWorkspaceReadFromId(workspaceId)
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun updateWorkspaceOrganization(workspaceUpdateOrganization: WorkspaceUpdateOrganization): WorkspaceRead {
      val workspaceId = workspaceUpdateOrganization.workspaceId

      val persistedWorkspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)
      persistedWorkspace
        .withOrganizationId(workspaceUpdateOrganization.organizationId)
      workspaceService.writeStandardWorkspaceNoSecrets(persistedWorkspace)
      return buildWorkspaceReadFromId(workspaceId)
    }

    @Throws(IOException::class, ConfigNotFoundException::class)
    fun setFeedbackDone(workspaceGiveFeedback: WorkspaceGiveFeedback) {
      workspaceService.setFeedback(workspaceGiveFeedback.workspaceId)
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    private fun buildWorkspaceReadFromId(workspaceId: UUID): WorkspaceRead {
      val workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)
      return domainToApiModel(workspace)
    }

    private fun buildWorkspaceOrganizationInfoRead(organization: Organization): WorkspaceOrganizationInfoRead =
      WorkspaceOrganizationInfoRead()
        .organizationId(organization.organizationId)
        .organizationName(organization.name)
        .sso(organization.ssoRealm != null && !organization.ssoRealm.isEmpty())

    @Throws(IOException::class)
    private fun generateUniqueSlug(workspaceName: String): String {
      val proposedSlug = slugify(workspaceName)

      // todo (cgardens) - this is going to be too expensive once there are too many workspaces. needs to
      // be replaced with an actual sql query. e.g. SELECT COUNT(*) WHERE slug=%s;
      var isSlugUsed = workspaceService.getWorkspaceBySlugOptional(proposedSlug, true).isPresent
      var resolvedSlug = proposedSlug
      var count = 0
      while (isSlugUsed) {
        // todo (cgardens) - this is still susceptible to a race condition where we randomly generate the
        // same slug in two different threads. this should be very unlikely. we can fix this by exposing
        // database transaction, but that is not something we can do quickly.
        resolvedSlug = proposedSlug + "-" + randomAlpha(8)
        isSlugUsed = workspaceService.getWorkspaceBySlugOptional(resolvedSlug, true).isPresent
        count++
        if (count > MAX_SLUG_GENERATION_ATTEMPTS) {
          throw InternalServerKnownException(String.format("could not generate a valid slug after %s tries.", MAX_SLUG_GENERATION_ATTEMPTS))
        }
      }

      return resolvedSlug
    }

    private fun validateWorkspacePatch(
      persistedWorkspace: StandardWorkspace,
      workspacePatch: WorkspaceUpdate,
    ) {
      Preconditions.checkArgument(persistedWorkspace.workspaceId == workspacePatch.workspaceId)
    }

    @Throws(IOException::class)
    private fun applyPatchToStandardWorkspace(
      workspace: StandardWorkspace,
      workspacePatch: WorkspaceUpdate,
    ) {
      if (workspacePatch.anonymousDataCollection != null) {
        workspace.anonymousDataCollection = workspacePatch.anonymousDataCollection
      }
      if (workspacePatch.news != null) {
        workspace.news = workspacePatch.news
      }
      if (workspacePatch.displaySetupWizard != null) {
        workspace.displaySetupWizard = workspacePatch.displaySetupWizard
      }
      if (workspacePatch.securityUpdates != null) {
        workspace.securityUpdates = workspacePatch.securityUpdates
      }
      if (!Strings.isNullOrEmpty(workspacePatch.email)) {
        workspace.email = workspacePatch.email
      }
      if (workspacePatch.initialSetupComplete != null) {
        workspace.initialSetupComplete = workspacePatch.initialSetupComplete
      }
      if (CollectionUtils.isNotEmpty(workspacePatch.notifications)) {
        workspace.notifications = toConfigList(workspacePatch.notifications)
      }

      // The updateWorkspace function that calls this code is overloaded with two different behaviors:
      // it's mostly used for a PUT (complete overwrite) in the internal APIs, but also for PATCH in the
      // public API.
      //
      // The internal PUT APIs use notificationSettings, so if that field is set, then replace those
      // settings.
      // The public PATCH APIs use notificationsConfig, so if that field is set, then merge that config
      // into
      // the notification settings.
      if (workspacePatch.notificationSettings != null) {
        workspace.notificationSettings = toConfig(workspacePatch.notificationSettings)
      } else if (workspacePatch.notificationsConfig != null) {
        patchNotifications(workspace, workspacePatch.notificationsConfig)
      }

      if (workspacePatch.dataplaneGroupId != null) {
        workspace.dataplaneGroupId = workspacePatch.dataplaneGroupId
      }
      if (workspacePatch.name != null) {
        workspace.name = workspacePatch.name
        workspace.slug = generateUniqueSlug(workspacePatch.name)
      }
      // Empty List is a valid value for webhookConfigs
      if (workspacePatch.webhookConfigs != null) {
        workspace.webhookOperationConfigs = toPersistenceWrite(workspacePatch.webhookConfigs, uuidSupplier)
      }
      if (workspacePatch.name != null) {
        workspace.name = workspacePatch.name
      }
    }

    // Apply a patch to the internal notification config model (NotificationSettings).
    private fun patchNotifications(
      workspace: StandardWorkspace,
      config: NotificationsConfig,
    ) {
      var settings = workspace.notificationSettings
      if (settings == null) {
        settings = NotificationSettings()
        workspace.notificationSettings = settings
      }

      settings.sendOnSuccess = patchNotificationItem(settings.sendOnSuccess, config.success)
      settings.sendOnFailure = patchNotificationItem(settings.sendOnFailure, config.failure)
      settings.sendOnConnectionUpdate = patchNotificationItem(settings.sendOnConnectionUpdate, config.connectionUpdate)
      settings.sendOnConnectionUpdateActionRequired =
        patchNotificationItem(settings.sendOnConnectionUpdateActionRequired, config.connectionUpdateActionRequired)
      settings.sendOnSyncDisabled = patchNotificationItem(settings.sendOnSyncDisabled, config.syncDisabled)
      settings.sendOnSyncDisabledWarning = patchNotificationItem(settings.sendOnSyncDisabledWarning, config.syncDisabledWarning)
    }

    // Apply a patch to a specific webhook from the public API to the internal notification config model
    // (NotificationItem).
    private fun patchNotificationItem(
      item: NotificationItem?,
      config: NotificationConfig?,
    ): NotificationItem? {
      var item = item
      if (config == null) {
        return item
      }

      val webhook = config.webhook
      val email = config.email

      if (webhook == null && email == null) {
        return item
      }
      if (item == null) {
        item = NotificationItem()
      }

      if (email != null && email.enabled != null) {
        if (email.enabled) {
          addNotificationType(item, Notification.NotificationType.CUSTOMERIO)
        } else {
          removeNotificationType(item, Notification.NotificationType.CUSTOMERIO)
        }
      }

      if (webhook != null) {
        if (webhook.enabled != null) {
          if (webhook.enabled) {
            addNotificationType(item, Notification.NotificationType.SLACK)
          } else {
            removeNotificationType(item, Notification.NotificationType.SLACK)
          }
        }

        if (webhook.url != null) {
          if (item.slackConfiguration != null) {
            item.slackConfiguration.webhook = webhook.url
          } else {
            item.slackConfiguration = SlackNotificationConfiguration().withWebhook(webhook.url)
          }
        }
      }

      return item
    }

    private fun addNotificationType(
      item: NotificationItem,
      type: Notification.NotificationType,
    ) {
      if (item.notificationType == null) {
        item.notificationType = ArrayList()
      }
      if (item.notificationType.contains(type)) {
        return
      }
      item.notificationType.add(type)
    }

    private fun removeNotificationType(
      item: NotificationItem,
      type: Notification.NotificationType,
    ) {
      if (item.notificationType == null) {
        return
      }
      item.notificationType.remove(type)
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    private fun persistStandardWorkspace(workspace: StandardWorkspace): WorkspaceRead {
      workspaceService.writeWorkspaceWithSecrets(workspace)
      return domainToApiModel(workspace)
    }

    companion object {
      const val MAX_SLUG_GENERATION_ATTEMPTS: Int = 10
      private val log = KotlinLogging.logger {}
    }
  }
