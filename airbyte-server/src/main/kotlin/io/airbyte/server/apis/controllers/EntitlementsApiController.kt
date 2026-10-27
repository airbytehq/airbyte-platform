/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.EntitlementsApi
import io.airbyte.api.server.generated.models.GetEntitlementsByOrganizationIdRequestBody
import io.airbyte.api.server.generated.models.GetEntitlementsByOrganizationIdResponse
import io.airbyte.api.server.generated.models.ListAllEntitlementPlansResponse
import io.airbyte.api.server.generated.models.OrganizationIsEntitledRequestBody
import io.airbyte.api.server.generated.models.OrganizationIsEntitledResponse
import io.airbyte.api.server.generated.models.UpdateOrganizationEntitlementPlanRequestBody
import io.airbyte.api.server.generated.models.UpdateOrganizationEntitlementPlanResponse
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.domain.models.OrganizationId
import io.airbyte.server.apis.execute
import io.airbyte.server.apis.requireBody
import io.airbyte.server.auth.GlobalAdminRule
import io.airbyte.server.auth.OrganizationEntitlementsRule
import io.airbyte.server.auth.OrganizationMemberRule
import io.airbyte.server.auth.OssInstanceAdminUnsupportedRule
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
open class EntitlementsApiController(
  private val entitlementService: EntitlementService,
) : EntitlementsApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OrganizationMemberRule::class,
  )
  override fun checkEntitlement(
    @Body isEntitledRequestBody: OrganizationIsEntitledRequestBody?,
  ): OrganizationIsEntitledResponse =
    execute {
      val body = isEntitledRequestBody.requireBody()
      val orgId = OrganizationId(body.organizationId)
      val entitlementId = body.featureId
      val entitlement =
        Entitlements.fromId(entitlementId)
          ?: throw IllegalArgumentException("Unknown entitlementId: $entitlementId")
      val entitlementResult = entitlementService.checkEntitlement(orgId, entitlement)

      OrganizationIsEntitledResponse(
        featureId = entitlementResult.featureId,
        isEntitled = entitlementResult.isEntitled,
        accessDeniedReason = entitlementResult.reason,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OrganizationEntitlementsRule::class,
  )
  override fun getEntitlements(
    @Body getEntitlementsByOrganizationIdRequestBody: GetEntitlementsByOrganizationIdRequestBody?,
  ): GetEntitlementsByOrganizationIdResponse =
    execute {
      val orgId = OrganizationId(getEntitlementsByOrganizationIdRequestBody.requireBody().organizationId)

      val result = entitlementService.getEntitlements(orgId)

      GetEntitlementsByOrganizationIdResponse(
        entitlements =
          result.map {
            OrganizationIsEntitledResponse(
              featureId = it.featureId,
              featureName = it.featureName,
              isEntitled = it.isEntitled,
              accessDeniedReason = it.reason,
              value = it.value,
              isUnlimited = it.isUnlimited,
            )
          },
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = GlobalAdminRule::class,
  )
  override fun listAllEntitlementPlans(): ListAllEntitlementPlansResponse = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = OssInstanceAdminUnsupportedRule::class,
  )
  override fun updateOrganizationEntitlementPlan(
    @Body updateOrganizationEntitlementPlanRequestBody: UpdateOrganizationEntitlementPlanRequestBody,
  ): UpdateOrganizationEntitlementPlanResponse = throw ApiNotImplementedInOssProblem()
}
