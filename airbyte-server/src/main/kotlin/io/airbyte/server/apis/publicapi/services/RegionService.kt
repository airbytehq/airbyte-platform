/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.publicApi.server.generated.models.RegionCreateRequest
import io.airbyte.publicApi.server.generated.models.RegionPatchRequest
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.PATCH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.constants.REGIONS_PATH
import io.airbyte.server.apis.publicapi.constants.REGIONS_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.mappers.RegionResponseMapper
import io.airbyte.server.services.DataplaneService
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import jakarta.ws.rs.core.Response
import java.util.UUID

interface RegionService {
  fun controllerListRegions(organizationId: UUID): Response

  fun controllerCreateRegion(regionCreateRequest: RegionCreateRequest): Response

  fun controllerGetRegion(regionId: UUID): Response

  fun controllerUpdateRegion(
    regionId: UUID,
    regionPatchRequest: RegionPatchRequest,
  ): Response

  fun controllerDeleteRegion(regionId: UUID): Response
}

private val log = KotlinLogging.logger {}

@Singleton
class RegionServiceImpl(
  private val dataplaneGroupService: DataplaneGroupService,
  private val dataplaneService: DataplaneService,
  private val trackingHelper: TrackingHelper,
  private val currentUserService: CurrentUserService,
) : RegionService {
  override fun controllerListRegions(organizationId: UUID): Response {
    val userId = currentUserService.getCurrentUser().userId
    val regions =
      trackingHelper.callWithTracker(
        {
          kotlin
            .runCatching { dataplaneGroupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID, organizationId), false) }
            .onFailure {
              log.error { "Error listing regions" }
              ConfigClientErrorHandler.handleError(it)
            }.getOrNull()
        },
        REGIONS_PATH,
        GET,
        userId,
      )

    return Response.ok().entity(regions?.mapNotNull { RegionResponseMapper.from(it) }).build()
  }

  override fun controllerCreateRegion(regionCreateRequest: RegionCreateRequest): Response {
    val userId = currentUserService.getCurrentUser().userId

    val regionCreate =
      DataplaneGroup()
        .apply {
          name = regionCreateRequest.name
          organizationId = regionCreateRequest.organizationId
          enabled = regionCreateRequest.enabled ?: true
        }

    val created =
      trackingHelper.callWithTracker(
        {
          kotlin
            .runCatching { dataplaneGroupService.writeDataplaneGroup(regionCreate) }
            .onFailure {
              log.error { "Error creating region" }
              ConfigClientErrorHandler.handleError(it)
            }.getOrNull()
        },
        REGIONS_PATH,
        POST,
        userId,
      )

    return Response.status(Response.Status.OK).entity(RegionResponseMapper.from(created)).build()
  }

  override fun controllerGetRegion(regionId: UUID): Response {
    val userId = currentUserService.getCurrentUser().userId

    val region =
      trackingHelper.callWithTracker(
        {
          kotlin
            .runCatching { dataplaneGroupService.getDataplaneGroup(regionId) }
            .onFailure {
              log.error { "Error fetching region" }
              ConfigClientErrorHandler.handleError(it)
            }.getOrNull()
        },
        REGIONS_WITH_ID_PATH,
        GET,
        userId,
      )

    return Response.ok().entity(RegionResponseMapper.from(region)).build()
  }

  override fun controllerUpdateRegion(
    regionId: UUID,
    regionPatchRequest: RegionPatchRequest,
  ): Response {
    val userId = currentUserService.getCurrentUser().userId

    val existing = dataplaneGroupService.getDataplaneGroup(regionId)

    val updated =
      existing.apply {
        regionPatchRequest.name?.let { name = it }
        regionPatchRequest.enabled?.let { enabled = it }
      }

    val result =
      trackingHelper.callWithTracker(
        {
          runCatching { dataplaneGroupService.writeDataplaneGroup(updated) }
            .onFailure {
              log.error { "Error updating region" }
              ConfigClientErrorHandler.handleError(it)
            }.getOrNull()
        },
        REGIONS_WITH_ID_PATH,
        PATCH,
        userId,
      )

    return Response.ok().entity(RegionResponseMapper.from(result)).build()
  }

  override fun controllerDeleteRegion(regionId: UUID): Response {
    val userId = currentUserService.getCurrentUser().userId

    val delete =
      trackingHelper.callWithTracker(
        {
          kotlin
            .runCatching {
              val deletedDataplaneGroup = dataplaneGroupService.getDataplaneGroup(regionId)

              val tombstonedGroup =
                deletedDataplaneGroup.apply {
                  tombstone = true
                }
              dataplaneService.listDataplanes(regionId).forEach {
                dataplaneService.deleteDataplane(it.id)
              }
              dataplaneGroupService.writeDataplaneGroup(tombstonedGroup)

              tombstonedGroup
            }.onFailure {
              log.error { "Error deleting region" }
              ConfigClientErrorHandler.handleError(it)
            }.getOrNull()
        },
        REGIONS_WITH_ID_PATH,
        DELETE,
        userId,
      )

    return Response.ok().entity(RegionResponseMapper.from(delete)).build()
  }
}
