/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.api.problems.model.generated.ProblemEntitlementServiceData
import io.airbyte.api.problems.throwable.generated.EntitlementServiceUnableToAddOrganizationProblem
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.data.services.OrganizationService
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.stigg.api.client.Stigg
import io.stigg.api.operations.GetActiveSubscriptionsListQuery
import io.stigg.api.operations.GetEntitlementQuery
import io.stigg.api.operations.GetEntitlementsQuery
import io.stigg.api.operations.ProvisionCustomerMutation
import io.stigg.api.operations.type.FetchEntitlementQuery
import io.stigg.api.operations.type.FetchEntitlementsQuery
import io.stigg.api.operations.type.GetActiveSubscriptionsInput
import io.stigg.api.operations.type.ProvisionCustomerInput
import io.stigg.api.operations.type.ProvisionCustomerSubscriptionInput
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

interface EntitlementClient {
  fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult

  fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult>

  fun addOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  )
}

@Singleton
class DefaultEntitlementClient : EntitlementClient {
  override fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult =
    EntitlementResult(featureId = entitlement.featureId, isEntitled = false, reason = "DefaultEntitlementClient grants no entitlements")

  override fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult> = emptyList()

  override fun addOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {}
}

@Factory
class StiggClientFactory {
  @Singleton
  fun stiggClient(
    @Value("\${airbyte.entitlement.stigg.api-key}") serverKey: String,
  ): Stigg = Stigg.createClient(serverKey)
}

@Singleton
@Replaces(DefaultEntitlementClient::class)
@Requires(property = "airbyte.entitlement.client", value = "stigg")
class StiggEntitlementClient(
  private val stigg: Stigg,
  private val organizationService: OrganizationService,
) : EntitlementClient {
  override fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult {
    logger.debug { "Checking entitlement organizationId=$organizationId entitlement=$entitlement" }

    val result =
      stigg.query(
        GetEntitlementQuery
          .builder()
          .query(
            FetchEntitlementQuery
              .builder()
              .customerId(organizationId.toString())
              .featureId(entitlement.featureId)
              .build(),
          ).build(),
      )

    logger.debug {
      "Got entitlement organizationId=$organizationId entitlement=$entitlement isGranted=${result.entitlement.entitlementFragment.isGranted} accessDeniedReason=${result.entitlement.entitlementFragment.accessDeniedReason?.rawValue}"
    }

    return EntitlementResult(
      entitlement.featureId,
      result.entitlement.entitlementFragment.isGranted,
      result.entitlement.entitlementFragment.accessDeniedReason
        ?.rawValue,
    )
  }

  override fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult> {
    logger.debug { "Getting entitlements organizationId=$organizationId" }

    val result =
      stigg.query(
        GetEntitlementsQuery
          .builder()
          .query(
            FetchEntitlementsQuery
              .builder()
              .customerId(organizationId.toString())
              .build(),
          ).build(),
      )

    logger.debug {
      "Got entitlements organizationId=$organizationId result=$result"
    }

    return result.entitlements.map {
      EntitlementResult(
        it.entitlementFragment.feature.featureFragment.refId,
        it.entitlementFragment.isGranted,
        it.entitlementFragment.accessDeniedReason ?.rawValue,
      )
    }
  }

  fun getPlans(organizationId: OrganizationId): List<EntitlementPlan> {
    val resp =
      stigg.query(
        GetActiveSubscriptionsListQuery
          .builder()
          .input(
            GetActiveSubscriptionsInput
              .builder()
              .customerId(organizationId.toString())
              .build(),
          ).build(),
      )

    return resp.getActiveSubscriptions.map { EntitlementPlan.valueOf(it.slimSubscriptionFragmentV2.plan.planId) }.toList()
  }

  internal fun validatePlanChange(
    organizationId: OrganizationId,
    toPlan: EntitlementPlan,
  ) {
    logger.debug { "Validating plan change organizationId=$organizationId toPlan=$toPlan" }

    val currentPlans = getPlans(organizationId)

    // Check if any current plan has a higher value than the target plan
    // (same value plans are allowed to move between each other)
    val hasHigherPlan = currentPlans.any { it.value > toPlan.value }

    if (hasHigherPlan) {
      val highestCurrentPlan = currentPlans.maxByOrNull { it.value }
      throw EntitlementServiceUnableToAddOrganizationProblem(
        ProblemEntitlementServiceData()
          .organizationId(organizationId.value)
          .planId(toPlan.toString())
          .errorMessage(
            "Cannot automatically downgrade from ${highestCurrentPlan?.name} (value: ${highestCurrentPlan?.value}) to ${toPlan.name} (value: ${toPlan.value})",
          ),
      )
    }

    logger.debug { "Plan change validation passed organizationId=$organizationId toPlan=$toPlan currentPlans=$currentPlans" }
  }

  override fun addOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    val org =
      organizationService
        .getOrganization(organizationId.value)
        .orElseThrow {
          IllegalStateException("Organization $organizationId not found; could not add to plan $plan")
        } ?: throw IllegalStateException("getOrganization() returned null for $organizationId; could not add to plan $plan")

    val input =
      ProvisionCustomerInput
        .builder()
        .customerId(organizationId.toString())
        .additionalMetaData(mapOf("name" to org.name))

    val plans = getPlans(organizationId)

    if (plans.contains(plan)) {
      logger.info { "Organization is already in plan. organizationId=$organizationId plan=$plan" }
      return
    }

    if (plan != null) {
      validatePlanChange(organizationId, plan)
    }

    plan?.let {
      input.subscriptionParams(
        ProvisionCustomerSubscriptionInput(
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          plan.id,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        ),
      )
    }

    stigg.mutation(ProvisionCustomerMutation(input.build()))

    logger.info { "Added organization to plan. organizationId=$organizationId plan=$plan" }
  }
}
