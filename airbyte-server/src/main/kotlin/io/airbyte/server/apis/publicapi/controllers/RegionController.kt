/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.domain.models.OrganizationId
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
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.Path
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class RegionController(
  private val regionService: RegionService,
  private val roleResolver: RoleResolver,
  private val entitlementService: EntitlementService,
) : PublicRegionsApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListRegions(organizationId: UUID): Response {
    ensureSelfManagedRegionsEntitlement(OrganizationId(organizationId))
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, organizationId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    return regionService.controllerListRegions(organizationId)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateRegion(regionCreateRequest: RegionCreateRequest): Response {
    ensureSelfManagedRegionsEntitlement(OrganizationId(regionCreateRequest.organizationId))
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, regionCreateRequest.organizationId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    return regionService.controllerCreateRegion(regionCreateRequest)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$REGIONS_PATH/{regionId}")
  override fun publicGetRegion(regionId: UUID): Response {
    val orgId = regionService.getOrganizationIdFromRegion(regionId)
    ensureSelfManagedRegionsEntitlement(OrganizationId(orgId))
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, orgId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    return regionService.controllerGetRegion(regionId)
  }

  @Patch
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$REGIONS_PATH/{regionId}")
  override fun publicUpdateRegion(
    regionId: UUID,
    regionPatchRequest: RegionPatchRequest,
  ): Response {
    val orgId = regionService.getOrganizationIdFromRegion(regionId)
    ensureSelfManagedRegionsEntitlement(OrganizationId(orgId))
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, orgId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    return regionService.controllerUpdateRegion(regionId, regionPatchRequest)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$REGIONS_PATH/{regionId}")
  override fun publicDeleteRegion(regionId: UUID): Response {
    val orgId = regionService.getOrganizationIdFromRegion(regionId)
    ensureSelfManagedRegionsEntitlement(OrganizationId(orgId))
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, orgId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    return regionService.controllerDeleteRegion(regionId)
  }

  private fun ensureSelfManagedRegionsEntitlement(orgId: OrganizationId) {
    entitlementService.ensureEntitled(orgId, SelfManagedRegionsEntitlement)
  }
}
