/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.DataplaneCreateResponse
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Permission
import io.airbyte.publicApi.server.generated.models.DataplaneCreateRequest
import io.airbyte.publicApi.server.generated.models.DataplanePatchRequest
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.DATAPLANES_PATH
import io.airbyte.server.apis.publicapi.constants.DATAPLANES_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.PATCH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.mappers.DataplaneCreateResponseMapper
import io.airbyte.server.apis.publicapi.mappers.DataplaneResponseMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import jakarta.ws.rs.core.Response
import java.util.UUID

interface DataplaneService {
  fun controllerListDataplanes(regionIds: List<UUID>?): Response

  fun controllerCreateDataplane(dataplaneCreateRequest: DataplaneCreateRequest): Response

  fun controllerGetDataplane(dataplaneId: UUID): Response

  fun controllerUpdateDataplane(
    dataplaneId: UUID,
    dataplanePatchRequest: DataplanePatchRequest,
  ): Response

  fun controllerDeleteDataplane(dataplaneId: UUID): Response

  fun getRegionIdFromDataplane(dataplaneId: UUID): UUID
}

private val log = KotlinLogging.logger {}

@Singleton
class DataplaneServiceImpl(
  private val dataplaneDataService: io.airbyte.data.services.DataplaneService,
  private val dataplaneService: io.airbyte.server.services.DataplaneService,
  private val trackingHelper: TrackingHelper,
  private val currentUserService: CurrentUserService,
  private val permissionHandler: PermissionHandler,
) : DataplaneService {
  override fun controllerListDataplanes(regionIds: List<UUID>?): Response {
    val userId = currentUserService.getCurrentUser().userId
    val result =
      trackingHelper.callWithTracker(
        {
          runCatching {
            if (!regionIds.isNullOrEmpty()) {
              dataplaneDataService.listDataplanes(regionIds, withTombstone = false)
            } else if (permissionHandler.isUserInstanceAdmin(userId)) {
              // If the user is an instance admin, get all dataplane
              dataplaneDataService.listDataplanes(withTombstone = false)
            } else {
              // If no regionIds are provided, get dataplanes for all organizations in which the
              // user has access
              dataplaneDataService.listDataplanesForOrganizations(
                organizationIds = getOrgIdsWithDataplaneAccessForUser(userId),
                withTombstone = false,
              )
            }
          }.onFailure {
            log.error(it) { "Error listing dataplanes" }
            ConfigClientErrorHandler.handleError(it)
          }.getOrNull()
        },
        DATAPLANES_PATH,
        GET,
        userId,
      )

    return Response.ok().entity(result?.map(DataplaneResponseMapper::from)).build()
  }

  private fun getOrgIdsWithDataplaneAccessForUser(userId: UUID): List<UUID> {
    val userPermissions = permissionHandler.listPermissionsForUser(userId)
    // Require that a user is an org admin to get dataplane access for that org
    return userPermissions
      .filter { it.permissionType == Permission.PermissionType.ORGANIZATION_ADMIN }
      .map { it.organizationId }
  }

  override fun controllerCreateDataplane(dataplaneCreateRequest: DataplaneCreateRequest): Response {
    val userId = currentUserService.getCurrentUser().userId

    val newDataplane =
      io.airbyte.config.Dataplane().apply {
        id = UUID.randomUUID()
        dataplaneGroupId = dataplaneCreateRequest.regionId
        name = dataplaneCreateRequest.name
        enabled = dataplaneCreateRequest.enabled ?: true
      }

    val result =
      trackingHelper.callWithTracker(
        {
          runCatching {
            val dataplaneWithServiceAccount = dataplaneDataService.createDataplaneAndServiceAccount(newDataplane)
            DataplaneCreateResponse()
              .regionId(dataplaneWithServiceAccount.dataplane.dataplaneGroupId)
              .dataplaneId(dataplaneWithServiceAccount.dataplane.id)
              .clientId(dataplaneWithServiceAccount.serviceAccount.id.toString())
              .clientSecret(dataplaneWithServiceAccount.serviceAccount.secret)
          }.onFailure {
            log.error(it) { "Error creating dataplane" }
            ConfigClientErrorHandler.handleError(it)
          }.getOrNull()
        },
        DATAPLANES_PATH,
        POST,
        userId,
      )

    return Response.ok().entity(DataplaneCreateResponseMapper.from(result)).build()
  }

  override fun controllerGetDataplane(dataplaneId: UUID): Response {
    val userId = currentUserService.getCurrentUser().userId

    val result =
      trackingHelper.callWithTracker(
        {
          runCatching { dataplaneDataService.getDataplane(dataplaneId) }
            .onFailure {
              log.error(it) { "Error getting dataplane" }
              ConfigClientErrorHandler.handleError(it)
            }.getOrNull()
        },
        DATAPLANES_WITH_ID_PATH,
        GET,
        userId,
      )

    return Response.ok().entity(DataplaneResponseMapper.from(result)).build()
  }

  override fun controllerUpdateDataplane(
    dataplaneId: UUID,
    dataplanePatchRequest: DataplanePatchRequest,
  ): Response {
    val userId = currentUserService.getCurrentUser().userId

    val existing = dataplaneDataService.getDataplane(dataplaneId)
    val updated =
      existing.apply {
        dataplanePatchRequest.name?.let { name = it }
        dataplanePatchRequest.enabled?.let { enabled = it }
      }

    val result =
      trackingHelper.callWithTracker(
        {
          runCatching { dataplaneDataService.updateDataplane(updated) }
            .onFailure {
              log.error(it) { "Error updating dataplane" }
              ConfigClientErrorHandler.handleError(it)
            }.getOrNull()
        },
        DATAPLANES_WITH_ID_PATH,
        PATCH,
        userId,
      )

    return Response.ok().entity(DataplaneResponseMapper.from(result)).build()
  }

  override fun controllerDeleteDataplane(dataplaneId: UUID): Response {
    val userId = currentUserService.getCurrentUser().userId

    val result =
      trackingHelper.callWithTracker(
        {
          runCatching {
            dataplaneService.deleteDataplane(dataplaneId)
          }.onFailure {
            log.error(it) { "Error deleting dataplane" }
            ConfigClientErrorHandler.handleError(it)
          }.getOrNull()
        },
        DATAPLANES_WITH_ID_PATH,
        DELETE,
        userId,
      )

    return Response.ok().entity(DataplaneResponseMapper.from(result)).build()
  }

  override fun getRegionIdFromDataplane(dataplaneId: UUID): UUID = dataplaneDataService.getDataplane(dataplaneId).dataplaneGroupId
}
