/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.EntitlementsApi
import io.airbyte.api.model.generated.EntitlementPlanInfo
import io.airbyte.api.model.generated.GetEntitlementsByOrganizationIdRequestBody
import io.airbyte.api.model.generated.GetEntitlementsByOrganizationIdResponse
import io.airbyte.api.model.generated.ListAllEntitlementPlansResponse
import io.airbyte.api.model.generated.OrganizationIsEntitledRequestBody
import io.airbyte.api.model.generated.OrganizationIsEntitledResponse
import io.airbyte.api.model.generated.UpdateOrganizationEntitlementPlanRequestBody
import io.airbyte.api.model.generated.UpdateOrganizationEntitlementPlanResponse
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.server.apis.execute
import io.airbyte.server.helpers.OrganizationAccessAuthorizationHelper
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
  private val organizationAccessAuthorizationHelper: OrganizationAccessAuthorizationHelper,
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
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getEntitlements(
    @Body getEntitlementsByOrganizationIdRequestBody: GetEntitlementsByOrganizationIdRequestBody,
  ): GetEntitlementsByOrganizationIdResponse? =
    execute {
      val orgId = OrganizationId(getEntitlementsByOrganizationIdRequestBody.organizationId)

      try {
        // Validate organization access (organization member OR workspace access)
        organizationAccessAuthorizationHelper.validateOrganizationOrWorkspaceAccess(orgId.value)

        val result = entitlementService.getEntitlements(orgId)

        GetEntitlementsByOrganizationIdResponse()
          .entitlements(
            result.map {
              OrganizationIsEntitledResponse()
                .featureId(it.featureId)
                .featureName(it.featureName)
                .isEntitled(it.isEntitled)
                .accessDeniedReason(it.reason)
            },
          )
      } catch (e: ForbiddenProblem) {
        // User doesn't have access to this organization, return empty entitlements list
        GetEntitlementsByOrganizationIdResponse()
          .entitlements(emptyList())
      }
    }

  @Post("/list_plans")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listAllEntitlementPlans(): ListAllEntitlementPlansResponse? =
    execute {
      val allPlans = EntitlementPlan.entries

      val planInfoList =
        allPlans.map { plan ->
          EntitlementPlanInfo()
            .planId(plan.id)
            .planName(plan.name)
        }
      ListAllEntitlementPlansResponse().plans(planInfoList)
    }

  @Post("/update_plan")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateOrganizationEntitlementPlan(
    @Body updateOrganizationEntitlementPlanRequestBody: UpdateOrganizationEntitlementPlanRequestBody,
  ): UpdateOrganizationEntitlementPlanResponse? =
    execute {
      val orgId = OrganizationId(updateOrganizationEntitlementPlanRequestBody.organizationId)
      val planName = updateOrganizationEntitlementPlanRequestBody.planName.toString()

      val plan = EntitlementPlan.valueOf(planName)
      entitlementService.addOrUpdateOrganization(orgId, plan)

      UpdateOrganizationEntitlementPlanResponse()
        .organizationId(updateOrganizationEntitlementPlanRequestBody.organizationId)
        .planName(planName)
    }
}
