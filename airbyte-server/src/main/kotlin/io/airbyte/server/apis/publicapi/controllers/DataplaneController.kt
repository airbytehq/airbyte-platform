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
import io.airbyte.publicApi.server.generated.apis.PublicDataplanesApi
import io.airbyte.publicApi.server.generated.models.DataplaneCreateRequest
import io.airbyte.publicApi.server.generated.models.DataplanePatchRequest
import io.airbyte.server.apis.publicapi.constants.DATAPLANES_PATH
import io.airbyte.server.apis.publicapi.services.DataplaneService
import io.airbyte.server.apis.publicapi.services.RegionService
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Patch
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.Path
import jakarta.ws.rs.core.Response
import java.util.UUID

@Controller(DATAPLANES_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DataplaneController(
  private val dataplaneService: DataplaneService,
  private val regionService: RegionService,
  private val roleResolver: RoleResolver,
  private val entitlementService: EntitlementService,
) : PublicDataplanesApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicListDataplanes(regionIds: List<UUID>?): Response {
    if (!regionIds.isNullOrEmpty()) {
      roleResolver
        .newRequest()
        .withCurrentUser()
        .apply {
          regionIds.forEach { withOrg(regionService.getOrganizationIdFromRegion(it)) }
        }.requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)
    }
    return dataplaneService.controllerListDataplanes(regionIds)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateDataplane(dataplaneCreateRequest: DataplaneCreateRequest): Response {
    val orgId = regionService.getOrganizationIdFromRegion(dataplaneCreateRequest.regionId)
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, orgId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)
    ensureSelfManagedRegionsEntitlement(OrganizationId(orgId))

    return dataplaneService.controllerCreateDataplane(dataplaneCreateRequest)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$DATAPLANES_PATH/{dataplaneId}")
  override fun publicGetDataplane(dataplaneId: UUID): Response {
    val regionId = dataplaneService.getRegionIdFromDataplane(dataplaneId)
    val orgId = regionService.getOrganizationIdFromRegion(regionId)
    ensureSelfManagedRegionsEntitlement(OrganizationId(orgId))
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, orgId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    return dataplaneService.controllerGetDataplane(dataplaneId)
  }

  @Patch
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$DATAPLANES_PATH/{dataplaneId}")
  override fun publicUpdateDataplane(
    dataplaneId: UUID,
    dataplanePatchRequest: DataplanePatchRequest,
  ): Response {
    val regionId = dataplaneService.getRegionIdFromDataplane(dataplaneId)
    val orgId = regionService.getOrganizationIdFromRegion(regionId)
    ensureSelfManagedRegionsEntitlement(OrganizationId(orgId))
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, orgId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    return dataplaneService.controllerUpdateDataplane(dataplaneId, dataplanePatchRequest)
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  @Path("$DATAPLANES_PATH/{dataplaneId}")
  override fun publicDeleteDataplane(dataplaneId: UUID): Response {
    val regionId = dataplaneService.getRegionIdFromDataplane(dataplaneId)
    val orgId = regionService.getOrganizationIdFromRegion(regionId)
    ensureSelfManagedRegionsEntitlement(OrganizationId(orgId))
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.ORGANIZATION_ID, orgId)
      .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)

    return dataplaneService.controllerDeleteDataplane(dataplaneId)
  }

  private fun ensureSelfManagedRegionsEntitlement(orgId: OrganizationId) {
    entitlementService.ensureEntitled(orgId, SelfManagedRegionsEntitlement)
  }
}
