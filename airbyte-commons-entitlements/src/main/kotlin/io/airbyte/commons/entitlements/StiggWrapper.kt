/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import com.apollographql.apollo3.exception.ApolloException
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.stigg.api.operations.GetActiveSubscriptionsListQuery
import io.stigg.api.operations.ProvisionCustomerMutation
import io.stigg.api.operations.ProvisionSubscriptionMutation
import io.stigg.api.operations.UpdateSubscriptionMutation
import io.stigg.api.operations.type.GetActiveSubscriptionsInput
import io.stigg.api.operations.type.ProvisionCustomerInput
import io.stigg.api.operations.type.ProvisionCustomerSubscriptionInput
import io.stigg.api.operations.type.ProvisionSubscriptionInput
import io.stigg.sidecar.proto.v1.GetBooleanEntitlementRequest
import io.stigg.sidecar.proto.v1.GetEntitlementsRequest
import io.stigg.sidecar.proto.v1.ReloadEntitlementsRequest
import io.stigg.sidecar.proto.v1.ReloadEntitlementsResponse
import io.stigg.sidecar.sdk.Stigg

private val logger = KotlinLogging.logger {}

data class EntitlementPlanResponse(
  val planEnum: EntitlementPlan,
  val planId: String,
  val planName: String,
)

/**
 * [StiggWrapper] a wrapper around the raw Stigg client.
 * This is useful for mocking Stigg in tests, because Stigg has a complex,
 * GraphQL-based API that is tricky to mock.
 *
 * This also provides a place to share code between entitlements clients like
 * [StiggCloudEntitlementClient] and [StiggEnterpriseEntitlementClient].
 *
 * Try to keep this wrapper thin. It's harder to test, so try to keep the logic
 * dead simple.
 */
internal class StiggWrapper(
  private val stigg: Stigg,
  private val metricClient: MetricClient? = null,
) {
  fun getPlans(organizationId: OrganizationId): List<EntitlementPlanResponse> {
    try {
      val resp =
        stigg.api().query(
          GetActiveSubscriptionsListQuery
            .builder()
            .input(
              GetActiveSubscriptionsInput
                .builder()
                .customerId(organizationId.value.toString())
                .build(),
            ).build(),
        )
      return resp.getActiveSubscriptions
        .map {
          EntitlementPlanResponse(
            planEnum = EntitlementPlan.fromId(it.slimSubscriptionFragmentV2.plan.planId),
            planId = it.slimSubscriptionFragmentV2.plan.planId,
            planName = it.slimSubscriptionFragmentV2.plan.displayName,
          )
        }.toList()
    } catch (e: ApolloException) {
      if (e.localizedMessage != null && e.localizedMessage!!.contains("Customer not found")) {
        logger.info { "No active subscriptions; organization not present in Stigg. organizationId=$organizationId" }
        return emptyList()
      }
      throw e
    }
  }

  fun provisionCustomer(
    orgId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    stigg.api().mutation(
      ProvisionCustomerMutation(
        ProvisionCustomerInput
          .builder()
          .customerId(orgId.value.toString())
          .subscriptionParams(
            ProvisionCustomerSubscriptionInput
              .builder()
              .planId(plan.id)
              .build(),
          ).build(),
      ),
    )
  }

  fun updateCustomerPlan(
    orgId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    stigg.api().mutation(
      ProvisionSubscriptionMutation(
        ProvisionSubscriptionInput
          .builder()
          .customerId(orgId.value.toString())
          .planId(plan.id)
          .build(),
      ),
    )
  }

  fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult {
    logger.debug { "Checking entitlement organizationId=$organizationId entitlement=$entitlement" }

    val result =
      stigg.getBooleanEntitlement(
        GetBooleanEntitlementRequest
          .newBuilder()
          .setCustomerId(organizationId.value.toString())
          .setFeatureId(entitlement.featureId)
          .build(),
      )

    logger
      .debug {
        "Got entitlement organizationId=$organizationId entitlement=$entitlement hasAccess=${result.hasAccess} isFallback=${result.isFallback} accessDeniedReason=${result.accessDeniedReason.name}"
      }

    if (result.isFallback) {
      metricClient?.count(
        OssMetricsRegistry.STIGG_FALLBACK,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.ORGANIZATION_ID, organizationId.toString()),
            MetricAttribute(MetricTags.FEATURE_ID, entitlement.featureId),
          ),
      )
    }

    return EntitlementResult(
      entitlement.featureId,
      result.hasAccess,
      result.accessDeniedReason.name,
    )
  }

  fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult> {
    logger.debug { "Getting entitlements organizationId=$organizationId" }

    var result =
      stigg.getEntitlements(
        GetEntitlementsRequest
          .newBuilder()
          .setCustomerId(organizationId.value.toString())
          .build(),
      )

    logger.debug {
      "Got entitlements organizationId=$organizationId result=$result"
    }

    return result.entitlementsList.mapNotNull {
      val featureId =
        when (it.entitlementCase) {
          io.stigg.sidecar.proto.v1.Entitlement.EntitlementCase.BOOLEAN -> it.boolean.feature.id
          io.stigg.sidecar.proto.v1.Entitlement.EntitlementCase.NUMERIC -> it.numeric.feature.id
          io.stigg.sidecar.proto.v1.Entitlement.EntitlementCase.METERED -> it.metered.feature.id
          io.stigg.sidecar.proto.v1.Entitlement.EntitlementCase.ENUM -> it.enum.feature.id
          io.stigg.sidecar.proto.v1.Entitlement.EntitlementCase.ENTITLEMENT_NOT_SET -> {
            // Skip this case because there's no entitlement data.
            // This probably never happens, this is just how protobuf works.
            return@mapNotNull null
          }
        }

      val entitlement = Entitlements.fromId(featureId)

      if (entitlement == null) {
        logger.warn { "Encountered unknown entitlement. featureId=$featureId organizationId=$organizationId" }
      }

      // We query for only the entitlements the customer is granted,
      // so we'd never have any results where isEntitled = false.
      EntitlementResult(
        featureId = featureId,
        isEntitled = true,
        reason = null,
        featureName = entitlement?.name,
      )
    }
  }
}
