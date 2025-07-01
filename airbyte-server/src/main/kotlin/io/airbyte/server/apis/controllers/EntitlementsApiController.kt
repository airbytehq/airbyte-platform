/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.EntitlementsApi
import io.airbyte.api.model.generated.GetEntitlementsByOrganizationIdRequestBody
import io.airbyte.api.model.generated.GetEntitlementsByOrganizationIdResponse
import io.airbyte.api.model.generated.OrganizationIsEntitledRequestBody
import io.airbyte.api.model.generated.OrganizationIsEntitledResponse
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.EntitlementHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/entitlements")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class EntitlementsApiController(
  private val entitlementHandler: EntitlementHandler,
) : EntitlementsApi {
  @Post("/is_entitled")
  @Secured(AuthRoleConstants.ORGANIZATION_MEMBER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun checkEntitlement(
    @Body isEntitledRequestBody: OrganizationIsEntitledRequestBody,
  ): OrganizationIsEntitledResponse? =
    execute {
      val entitlementResult =
        entitlementHandler.isEntitled(isEntitledRequestBody.organizationId, isEntitledRequestBody.featureId)
      val response = OrganizationIsEntitledResponse()
      response.featureId(entitlementResult.featureId).isEntitled(entitlementResult.isEntitled).accessDeniedReason(entitlementResult.reason)
      response
    }

  @Post("/get_entitlements")
  @Secured(AuthRoleConstants.ORGANIZATION_MEMBER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getEntitlements(
    @Body getEntitlementsByOrganizationIdRequestBody: GetEntitlementsByOrganizationIdRequestBody,
  ): GetEntitlementsByOrganizationIdResponse? =
    execute {
      val result =
        entitlementHandler.getEntitlements(getEntitlementsByOrganizationIdRequestBody.organizationId)
      val response = GetEntitlementsByOrganizationIdResponse()
      response.entitlements(
        result.map { OrganizationIsEntitledResponse().featureId(it.featureId).isEntitled(it.isEntitled).accessDeniedReason(it.reason) },
      )
      response
    }
}
