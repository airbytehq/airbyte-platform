/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.model.generated.NotificationItem
import io.airbyte.api.model.generated.NotificationSettings
import io.airbyte.api.model.generated.NotificationType
import io.airbyte.api.model.generated.Pagination
import io.airbyte.api.model.generated.SlackNotificationConfiguration
import io.airbyte.api.model.generated.WorkspaceCreate
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.model.generated.WorkspaceUpdate
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.publicApi.server.generated.models.EmailNotificationConfig
import io.airbyte.publicApi.server.generated.models.NotificationConfig
import io.airbyte.publicApi.server.generated.models.NotificationsConfig
import io.airbyte.publicApi.server.generated.models.WebhookNotificationConfig
import io.airbyte.publicApi.server.generated.models.WorkspaceCreateRequest
import io.airbyte.publicApi.server.generated.models.WorkspaceOAuthCredentialsRequest
import io.airbyte.publicApi.server.generated.models.WorkspaceResponse
import io.airbyte.publicApi.server.generated.models.WorkspaceUpdateRequest
import io.airbyte.publicApi.server.generated.models.WorkspacesResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.constants.PATCH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.constants.WORKSPACES_PATH
import io.airbyte.server.apis.publicapi.constants.WORKSPACES_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.mappers.WorkspaceResponseMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import jakarta.ws.rs.core.Response
import java.util.UUID

interface WorkspaceService {
  fun createWorkspace(workspaceCreateRequest: WorkspaceCreateRequest): WorkspaceResponse

  fun controllerCreateWorkspace(workspaceCreateRequest: WorkspaceCreateRequest): Response

  fun updateWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
  ): WorkspaceResponse

  fun controllerUpdateWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
  ): Response

  fun getWorkspace(workspaceId: UUID): WorkspaceResponse

  fun controllerGetWorkspace(workspaceId: UUID): Response

  fun deleteWorkspace(workspaceId: UUID)

  fun controllerDeleteWorkspace(workspaceId: UUID): Response

  fun listWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean = false,
    limit: Int = 20,
    offset: Int = 0,
  ): WorkspacesResponse

  fun controllerListWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean = false,
    limit: Int = 20,
    offset: Int = 0,
  ): Response

  fun controllerSetWorkspaceOverrideOAuthParams(
    workspaceId: UUID?,
    workspaceOAuthCredentialsRequest: WorkspaceOAuthCredentialsRequest?,
  ): Response
}

private val log = KotlinLogging.logger {}

@Singleton
@Secondary
open class WorkspaceServiceImpl(
  private val userService: UserService,
  private val trackingHelper: TrackingHelper,
  private val workspacesHandler: WorkspacesHandler,
  @Value("\${airbyte.api.host}") open val publicApiHost: String,
  private val currentUserService: CurrentUserService,
  private val dataplaneGroupService: DataplaneGroupService,
) : WorkspaceService {
  /**
   * Creates a workspace.
   */
  override fun createWorkspace(workspaceCreateRequest: WorkspaceCreateRequest): WorkspaceResponse {
    // For now this should always be true in OSS.
    val organizationId = DEFAULT_ORGANIZATION_ID

    val workspaceCreate =
      WorkspaceCreate()
        .name(workspaceCreateRequest.name)
        .email(currentUserService.getCurrentUser().email)
        .organizationId(organizationId)
        .notificationSettings(workspaceCreateRequest.notifications?.toNotificationSettings())
    if (workspaceCreateRequest.regionId != null) {
      workspaceCreate.dataplaneGroupId = dataplaneGroupService.getDataplaneGroup(workspaceCreateRequest.regionId!!).id
    }

    val result =
      kotlin
        .runCatching { workspacesHandler.createWorkspace(workspaceCreate) }
        .onFailure {
          log.error(it) { "Error for createWorkspace" }
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug { HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result }
    return WorkspaceResponseMapper.from(
      result.getOrNull()!!,
      dataplaneGroupService.getDataplaneGroup(result.getOrNull()!!.dataplaneGroupId).name,
    )
  }

  override fun controllerCreateWorkspace(workspaceCreateRequest: WorkspaceCreateRequest): Response {
    val userId: UUID = currentUserService.getCurrentUser().userId

    val workspaceResponse: WorkspaceResponse =
      trackingHelper.callWithTracker(
        { createWorkspace(workspaceCreateRequest) },
        WORKSPACES_PATH,
        POST,
        userId,
      ) as WorkspaceResponse
    trackingHelper.trackSuccess(
      WORKSPACES_PATH,
      POST,
      userId,
      UUID.fromString(workspaceResponse.workspaceId),
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(workspaceResponse)
      .build()
  }

  /**
   * Updates a workspace name in OSS.
   */
  override fun updateWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
  ): WorkspaceResponse {
    val workspaceUpdate =
      WorkspaceUpdate().apply {
        this.name = workspaceUpdateRequest.name
        this.workspaceId = workspaceId
        this.notificationsConfig = workspaceUpdateRequest.notifications.toInternalNotificationConfig()
      }
    if (workspaceUpdateRequest.regionId != null) {
      workspaceUpdate.apply {
        this.dataplaneGroupId = dataplaneGroupService.getDataplaneGroup(workspaceUpdateRequest.regionId!!).id
      }
    }

    val result =
      kotlin
        .runCatching { workspacesHandler.updateWorkspace(workspaceUpdate) }
        .onFailure {
          log.error(it) { "Error for updateWorkspace" }
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug { HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result }
    return WorkspaceResponseMapper.from(
      result.getOrNull()!!,
      dataplaneGroupService.getDataplaneGroup(result.getOrNull()!!.dataplaneGroupId).name,
    )
  }

  override fun controllerUpdateWorkspace(
    workspaceId: UUID,
    workspaceUpdateRequest: WorkspaceUpdateRequest,
  ): Response {
    val userId: UUID = currentUserService.getCurrentUser().userId

    val workspaceResponse: Any =
      trackingHelper.callWithTracker(
        { updateWorkspace(workspaceId, workspaceUpdateRequest) },
        WORKSPACES_WITH_ID_PATH,
        PATCH,
        userId,
      )
    trackingHelper.trackSuccess(
      WORKSPACES_WITH_ID_PATH,
      PATCH,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(workspaceResponse)
      .build()
  }

  /**
   * Fetches a workspace by ID.
   */
  override fun getWorkspace(workspaceId: UUID): WorkspaceResponse {
    val workspaceIdRequestBody = WorkspaceIdRequestBody()
    workspaceIdRequestBody.workspaceId = workspaceId
    val result =
      kotlin
        .runCatching { workspacesHandler.getWorkspace(workspaceIdRequestBody) }
        .onFailure {
          log.error(it) { "Error for getWorkspace" }
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug { HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result }
    return WorkspaceResponseMapper.from(result.getOrNull()!!, dataplaneGroupService.getDataplaneGroup(result.getOrNull()!!.dataplaneGroupId).name)
  }

  override fun controllerGetWorkspace(workspaceId: UUID): Response {
    val userId: UUID = currentUserService.getCurrentUser().userId

    val workspaceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          getWorkspace(
            workspaceId,
          )
        },
        WORKSPACES_WITH_ID_PATH,
        GET,
        userId,
      )
    trackingHelper.trackSuccess(
      WORKSPACES_WITH_ID_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(workspaceResponse)
      .build()
  }

  /**
   * Deletes a workspace by ID.
   */
  override fun deleteWorkspace(workspaceId: UUID) {
    val workspaceIdRequestBody = WorkspaceIdRequestBody()
    workspaceIdRequestBody.workspaceId = workspaceId
    val result =
      kotlin
        .runCatching { workspacesHandler.deleteWorkspace(workspaceIdRequestBody) }
        .onFailure {
          log.error(it) { "Error for deleteWorkspace" }
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug { HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result }
  }

  override fun controllerDeleteWorkspace(workspaceId: UUID): Response {
    val userId: UUID = currentUserService.getCurrentUser().userId

    val workspaceResponse: Any? =
      trackingHelper.callWithTracker(
        {
          deleteWorkspace(
            workspaceId,
          )
        },
        WORKSPACES_WITH_ID_PATH,
        DELETE,
        userId,
      )
    return Response
      .status(Response.Status.NO_CONTENT.statusCode)
      .entity(workspaceResponse)
      .build()
  }

  /**
   * Lists a workspace by a set of IDs or all workspaces if no IDs are provided.
   */
  override fun listWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
  ): WorkspacesResponse {
    val pagination: Pagination = Pagination().pageSize(limit).rowOffset(offset)

    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(currentUserService.getCurrentUser().userId) }
    log.debug { "Workspaces to query: $workspaceIdsToQuery" }
    val listResourcesForWorkspacesRequestBody = ListResourcesForWorkspacesRequestBody()
    listResourcesForWorkspacesRequestBody.includeDeleted = includeDeleted
    listResourcesForWorkspacesRequestBody.pagination = pagination
    listResourcesForWorkspacesRequestBody.workspaceIds = workspaceIdsToQuery
    val result =
      kotlin
        .runCatching { workspacesHandler.listWorkspacesPaginated(listResourcesForWorkspacesRequestBody) }
        .onFailure {
          log.error(it) { "Error for listWorkspaces" }
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug { HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result }
    return io.airbyte.server.apis.publicapi.mappers.WorkspacesResponseMapper.from(
      result.getOrNull()!!,
      workspaceIds,
      result.getOrNull()!!.workspaces.map {
        dataplaneGroupService.getDataplaneGroup(it.dataplaneGroupId).name
      },
      includeDeleted,
      limit,
      offset,
      publicApiHost,
    )
  }

  override fun controllerListWorkspaces(
    workspaceIds: List<UUID>,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
  ): Response {
    val userId: UUID = currentUserService.getCurrentUser().userId

    val workspaces: Any? =
      trackingHelper.callWithTracker(
        {
          listWorkspaces(
            workspaceIds,
            includeDeleted,
            limit,
            offset,
          )
        },
        WORKSPACES_PATH,
        GET,
        userId,
      )
    trackingHelper.trackSuccess(
      WORKSPACES_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(workspaces)
      .build()
  }

  override fun controllerSetWorkspaceOverrideOAuthParams(
    workspaceId: UUID?,
    workspaceOAuthCredentialsRequest: WorkspaceOAuthCredentialsRequest?,
  ): Response = Response.status(Response.Status.NOT_IMPLEMENTED).build()
}

fun NotificationsConfig.toNotificationSettings() =
  NotificationSettings()
    .sendOnFailure(failure?.toNotificationItem(enableEmailByDefault = true))
    .sendOnSuccess(success?.toNotificationItem(enableEmailByDefault = false))
    .sendOnConnectionUpdate(connectionUpdate?.toNotificationItem(enableEmailByDefault = true))
    .sendOnConnectionUpdateActionRequired(connectionUpdateActionRequired?.toNotificationItem(enableEmailByDefault = true))
    .sendOnSyncDisabled(syncDisabled?.toNotificationItem(enableEmailByDefault = true))
    .sendOnSyncDisabledWarning(syncDisabledWarning?.toNotificationItem(enableEmailByDefault = true))

private fun NotificationsConfig?.toInternalNotificationConfig() =
  this?.let {
    io.airbyte.api.model.generated
      .NotificationsConfig()
      .success(it.success?.toInternalNotificationConfig())
      .failure(it.failure?.toInternalNotificationConfig())
      .connectionUpdate(it.connectionUpdate?.toInternalNotificationConfig())
      .connectionUpdateActionRequired(it.connectionUpdateActionRequired?.toInternalNotificationConfig())
      .syncDisabled(it.syncDisabled?.toInternalNotificationConfig())
      .syncDisabledWarning(it.syncDisabledWarning?.toInternalNotificationConfig())
  }

private fun NotificationConfig?.toInternalNotificationConfig() =
  this?.let {
    io.airbyte.api.model.generated
      .NotificationConfig()
      .email(it.email?.toInternalEmailNotificationConfig())
      .webhook(it.webhook?.toInternalWebhookNotificationConfig())
  }

private fun EmailNotificationConfig?.toInternalEmailNotificationConfig() =
  this?.let {
    io.airbyte.api.model.generated
      .EmailNotificationConfig()
      .enabled(it.enabled)
  }

private fun WebhookNotificationConfig?.toInternalWebhookNotificationConfig() =
  this?.let {
    io.airbyte.api.model.generated
      .WebhookNotificationConfig()
      .enabled(it.enabled)
      .url(it.url)
  }

private fun NotificationConfig.toNotificationItem(enableEmailByDefault: Boolean): NotificationItem {
  val item = NotificationItem()
  if (this.webhook?.enabled == true) {
    item.addNotificationTypeItem(NotificationType.SLACK)
    item.slackConfiguration(SlackNotificationConfiguration().webhook(this.webhook?.url))
  }

  if (this.email?.enabled == true || (enableEmailByDefault && this.email == null)) {
    item.addNotificationTypeItem(NotificationType.CUSTOMERIO)
  }
  return item
}
