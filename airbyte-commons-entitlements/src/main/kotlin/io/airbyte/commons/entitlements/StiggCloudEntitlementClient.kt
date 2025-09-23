/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import com.apollographql.apollo3.exception.ApolloException
import io.airbyte.api.problems.model.generated.ProblemEntitlementServiceData
import io.airbyte.api.problems.throwable.generated.EntitlementServiceUnableToAddOrganizationProblem
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
 * The main difference is that it contains an implementation of [addOrganization],
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

  override fun getPlans(organizationId: OrganizationId): List<EntitlementPlan> = stigg.getPlans(organizationId)

  override fun addOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    validatePlanChange(organizationId, plan)

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

  private fun validatePlanChange(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    logger.debug { "Validating plan change organizationId=$organizationId plan=$plan" }

    val currentPlans = stigg.getPlans(organizationId)

    if (currentPlans.contains(plan)) {
      logger.info { "Organization is already in plan. organizationId=$organizationId plan=$plan" }
      return
    }

    // Check if any current plan has a higher value than the target plan
    // (same value plans are allowed to move between each other)
    val hasHigherPlan = currentPlans.any { it.value > plan.value }

    if (hasHigherPlan) {
      val highestCurrentPlan = currentPlans.maxByOrNull { it.value }
      throw EntitlementServiceUnableToAddOrganizationProblem(
        ProblemEntitlementServiceData()
          .organizationId(organizationId.value)
          .planId(plan.toString())
          .errorMessage(
            "Cannot automatically downgrade from ${highestCurrentPlan?.name} (value: ${highestCurrentPlan?.value}) to ${plan.name} (value: ${plan.value})",
          ),
      )
    }

    logger.debug { "Plan change validation passed organizationId=$organizationId plan=$plan currentPlans=$currentPlans" }
  }
}
