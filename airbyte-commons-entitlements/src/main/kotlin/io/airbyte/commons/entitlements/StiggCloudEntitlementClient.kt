/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import com.apollographql.apollo3.exception.ApolloException
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.data.services.OrganizationService
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * StiggCloudEntitlementClient is the entitlement client, backed by Stigg, used in Airbyte Cloud.
 *
 * The main difference is that it contains an implementation of [addOrUpdateOrganization],
 * while other clients are a no-op.
 */
internal class StiggCloudEntitlementClient(
  private val stigg: StiggWrapper,
  private val organizationService: OrganizationService,
) : EntitlementClient {
  override fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult = stigg.checkEntitlement(organizationId, entitlement)

  override fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult> = stigg.getEntitlements(organizationId)

  override fun getEntitlementsForPlan(plan: EntitlementPlan): List<Entitlement> = stigg.getEntitlementsForPlan(plan)

  override fun getPlans(organizationId: OrganizationId): List<EntitlementPlanResponse> = stigg.getPlans(organizationId)

  override fun addOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    addEntitlementPlan(organizationId, plan)
  }

  override fun updateOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    stigg.updateCustomerPlan(organizationId, plan)
  }

  private fun addEntitlementPlan(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    try {
      stigg.provisionCustomer(organizationId, plan)
      logger.info { "Added organization to plan. organizationId=$organizationId plan=$plan" }
    } catch (e: ApolloException) {
      if (e.message?.contains("Duplicated entity not allowed") == true ||
        e.message?.contains("DuplicatedEntityNotAllowed") == true
      ) {
        logger.info { "Organization already exists in Stigg, treating as successful. organizationId=$organizationId plan=$plan" }
        return
      }
      throw e
    }
  }
}
