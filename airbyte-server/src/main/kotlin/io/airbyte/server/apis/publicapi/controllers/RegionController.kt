/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.publicApi.server.generated.apis.PublicRegionsApi
import io.airbyte.publicApi.server.generated.models.RegionCreateRequest
import io.airbyte.publicApi.server.generated.models.RegionPatchRequest
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.REGIONS_PATH
import io.airbyte.server.apis.publicapi.services.RegionService
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import jakarta.ws.rs.Path
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(AuthRoleConstants.ADMIN)
open class RegionController(
  private val regionService: RegionService,
) : PublicRegionsApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListRegions(organizationId: UUID): Response = regionService.controllerListRegions(organizationId)

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateRegion(regionCreateRequest: RegionCreateRequest): Response = regionService.controllerCreateRegion(regionCreateRequest)

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$REGIONS_PATH/{regionId}")
  override fun publicGetRegion(regionId: UUID): Response = regionService.controllerGetRegion(regionId)

  @Patch
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$REGIONS_PATH/{regionId}")
  override fun publicUpdateRegion(
    regionId: UUID,
    regionPatchRequest: RegionPatchRequest,
  ): Response = regionService.controllerUpdateRegion(regionId, regionPatchRequest)

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$REGIONS_PATH/{regionId}")
  override fun publicDeleteRegion(regionId: UUID): Response = regionService.controllerDeleteRegion(regionId)
}
