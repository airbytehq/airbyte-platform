/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.apis.PublicRegionsApi
import io.airbyte.publicApi.server.generated.models.RegionCreateRequest
import io.airbyte.publicApi.server.generated.models.RegionPatchRequest
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.REGIONS_PATH
import io.airbyte.server.apis.publicapi.services.RegionService
import io.airbyte.server.auth.PublicOrganizationAdminRule
import io.airbyte.server.auth.PublicOrganizationBodyAdminRule
import io.airbyte.server.auth.PublicRegionAdminRule
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.Path
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class RegionController(
  private val regionService: RegionService,
  private val entitlementService: EntitlementService,
) : PublicRegionsApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicOrganizationAdminRule::class,
  )
  override fun publicListRegions(organizationId: UUID): Response {
    ensureSelfManagedRegionsEntitlement(OrganizationId(organizationId))

    return regionService.controllerListRegions(organizationId)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicOrganizationBodyAdminRule::class,
  )
  override fun publicCreateRegion(regionCreateRequest: RegionCreateRequest): Response {
    ensureSelfManagedRegionsEntitlement(OrganizationId(regionCreateRequest.organizationId))

    return regionService.controllerCreateRegion(regionCreateRequest)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$REGIONS_PATH/{regionId}")
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicRegionAdminRule::class,
  )
  override fun publicGetRegion(regionId: UUID): Response {
    val orgId = regionService.getOrganizationIdFromRegion(regionId)
    ensureSelfManagedRegionsEntitlement(OrganizationId(orgId))

    return regionService.controllerGetRegion(regionId)
  }

  @Patch
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$REGIONS_PATH/{regionId}")
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicRegionAdminRule::class,
  )
  override fun publicUpdateRegion(
    regionId: UUID,
    regionPatchRequest: RegionPatchRequest,
  ): Response {
    val orgId = regionService.getOrganizationIdFromRegion(regionId)
    ensureSelfManagedRegionsEntitlement(OrganizationId(orgId))

    return regionService.controllerUpdateRegion(regionId, regionPatchRequest)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$REGIONS_PATH/{regionId}")
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = PublicRegionAdminRule::class,
  )
  override fun publicDeleteRegion(regionId: UUID): Response {
    val orgId = regionService.getOrganizationIdFromRegion(regionId)
    ensureSelfManagedRegionsEntitlement(OrganizationId(orgId))

    return regionService.controllerDeleteRegion(regionId)
  }

  private fun ensureSelfManagedRegionsEntitlement(orgId: OrganizationId) {
    entitlementService.ensureEntitled(orgId, SelfManagedRegionsEntitlement)
  }
}
