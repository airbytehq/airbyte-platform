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
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.domain.models.OrganizationId
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
  private val entitlementService: EntitlementService,
) : EntitlementsApi {
  @Post("/is_entitled")
  @Secured(AuthRoleConstants.ORGANIZATION_MEMBER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun checkEntitlement(
    @Body isEntitledRequestBody: OrganizationIsEntitledRequestBody,
  ): OrganizationIsEntitledResponse? =
    execute {
      val orgId = OrganizationId(isEntitledRequestBody.organizationId)
      val entitlementId = isEntitledRequestBody.featureId
      val entitlement =
        Entitlements.fromId(entitlementId)
          ?: throw IllegalArgumentException("Unknown entitlementId: $entitlementId")
      val entitlementResult = entitlementService.checkEntitlement(orgId, entitlement)

      OrganizationIsEntitledResponse()
        .featureId(entitlementResult.featureId)
        .isEntitled(entitlementResult.isEntitled)
        .accessDeniedReason(entitlementResult.reason)
    }

  @Post("/get_entitlements")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getEntitlements(
    @Body getEntitlementsByOrganizationIdRequestBody: GetEntitlementsByOrganizationIdRequestBody,
  ): GetEntitlementsByOrganizationIdResponse? =
    execute {
      val orgId = OrganizationId(getEntitlementsByOrganizationIdRequestBody.organizationId)
      val result = entitlementService.getEntitlements(orgId)

      GetEntitlementsByOrganizationIdResponse()
        .entitlements(
          result.map {
            OrganizationIsEntitledResponse()
              .featureId(it.featureId)
              .isEntitled(it.isEntitled)
              .accessDeniedReason(it.reason)
          },
        )
    }
}
