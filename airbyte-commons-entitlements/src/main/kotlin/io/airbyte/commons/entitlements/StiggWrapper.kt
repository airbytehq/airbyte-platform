/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import com.apollographql.apollo3.exception.ApolloException
import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.BypassStiggEntitlementChecks
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.stigg.api.operations.GetPaywallQuery
import io.stigg.api.operations.ProvisionCustomerMutation
import io.stigg.api.operations.ProvisionSubscriptionMutation
import io.stigg.api.operations.type.GetPaywallInput
import io.stigg.api.operations.type.ProvisionCustomerInput
import io.stigg.api.operations.type.ProvisionCustomerSubscriptionInput
import io.stigg.api.operations.type.ProvisionSubscriptionInput
import io.stigg.sidecar.proto.v1.GetBooleanEntitlementRequest
import io.stigg.sidecar.proto.v1.GetEntitlementsRequest
import io.stigg.sidecar.proto.v1.GetEnumEntitlementRequest
import io.stigg.sidecar.sdk.Stigg
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.time.Duration
import java.util.concurrent.TimeoutException

private val logger = KotlinLogging.logger {}

private const val STIGG_TIMEOUT_MS = 30_000L
private const val STIGG_RETRY_DELAY_MS = 200L
private const val STIGG_RETRY_MAX_DELAY_MS = 2000L
private const val STIGG_MAX_RETRIES = 3

/**
 * Determines if an exception is a transient gRPC error that should be retried.
 * Only retries on UNAVAILABLE status, which indicates connection issues
 * (e.g., during pod restarts or rolling deployments).
 */
private fun isRetryableException(t: Throwable): Boolean {
  // Direct StatusRuntimeException with UNAVAILABLE status
  if (t is StatusRuntimeException && t.status.code == Status.Code.UNAVAILABLE) {
    return true
  }
  // Wrapped StatusRuntimeException with UNAVAILABLE status
  val cause = t.cause
  if (cause is StatusRuntimeException && cause.status.code == Status.Code.UNAVAILABLE) {
    return true
  }
  return false
}

/**
 * Retry policy for Stigg API calls.
 * Retries only on gRPC UNAVAILABLE errors, which indicate transient connection issues
 * that can occur during pod restarts or rolling deployments.
 */
private val stiggRetryPolicy: RetryPolicy<Any> =
  RetryPolicy
    .builder<Any>()
    .handleIf { throwable -> isRetryableException(throwable) }
    .withBackoff(
      Duration.ofMillis(STIGG_RETRY_DELAY_MS),
      Duration.ofMillis(STIGG_RETRY_MAX_DELAY_MS),
    ).withJitter(0.25)
    .withMaxRetries(STIGG_MAX_RETRIES)
    .onRetry { event ->
      logger.warn {
        "Stigg API call failed with UNAVAILABLE, retrying (attempt ${event.attemptCount}/${STIGG_MAX_RETRIES}): ${event.lastException?.message}"
      }
    }.onRetriesExceeded { event ->
      logger.error(event.exception) {
        "Stigg API call failed after $STIGG_MAX_RETRIES retries"
      }
    }.build()

/**
 * Executes a Stigg API call with retry logic and a 30-second timeout.
 * Retries on transient gRPC connection errors (e.g., during rolling deployments).
 * If the call takes longer than 30 seconds, a TimeoutException is thrown.
 *
 * @param operation A description of the operation being performed (for logging)
 * @param block The Stigg API call to execute
 * @return The result of the API call
 * @throws TimeoutException if the call exceeds 30 seconds
 */
private fun <T> withStiggTimeout(
  operation: String,
  block: () -> T,
): T =
  try {
    Failsafe.with(stiggRetryPolicy).get { _ ->
      runBlocking {
        withTimeout(STIGG_TIMEOUT_MS) {
          block()
        }
      }
    }
  } catch (e: kotlinx.coroutines.TimeoutCancellationException) {
    logger.error { "Stigg API call timed out after ${STIGG_TIMEOUT_MS}ms: $operation" }
    throw TimeoutException("Stigg API call timed out after ${STIGG_TIMEOUT_MS}ms: $operation")
  }

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
  private val featureFlagClient: FeatureFlagClient? = null,
) {
  fun getPlans(organizationId: OrganizationId): List<EntitlementPlanResponse> {
    try {
      val result =
        withStiggTimeout("getPlans(organizationId=$organizationId)") {
          stigg.getEnumEntitlement(
            GetEnumEntitlementRequest
              .newBuilder()
              .setCustomerId(organizationId.value.toString())
              .setFeatureId("feature-plan-name")
              .build(),
          )
        }

      if (result.entitlement.enumValuesList.size > 1) {
        logger.error {
          "More than one entitlement Plan was found. This is unexpected. " +
            "organizationId=$organizationId entitlementPlans=${result.entitlement.enumValuesList}"
        }
      }

      return result.entitlement.enumValuesList
        .map {
          val planEnum = EntitlementPlan.fromId(it)
          EntitlementPlanResponse(
            planEnum = planEnum,
            planId = it,
            planName = planEnum.displayName,
          )
        }.toList()
    } catch (e: ApolloException) {
      if (e.localizedMessage != null && e.localizedMessage!!.contains("Customer not found")) {
        logger.info { "No active subscriptions; organization not present in Stigg. organizationId=$organizationId" }
        return emptyList()
      } else {
        throw e
      }
    }
  }

  fun provisionCustomer(
    orgId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    withStiggTimeout("provisionCustomer(orgId=$orgId, plan=${plan.id})") {
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
  }

  fun updateCustomerPlan(
    orgId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    withStiggTimeout("updateCustomerPlan(orgId=$orgId, plan=${plan.id})") {
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
  }

  fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult {
    logger.debug { "Checking entitlement organizationId=$organizationId entitlement=$entitlement" }

    if (featureFlagClient?.boolVariation(BypassStiggEntitlementChecks, Organization(organizationId.value)) == true) {
      logger.info { "Bypassing Stigg entitlement check for organizationId=$organizationId entitlement=${entitlement.featureId}" }
      return EntitlementResult(
        featureId = entitlement.featureId,
        isEntitled = false,
        reason = "BYPASSED_BY_FEATURE_FLAG",
      )
    }

    val result =
      withStiggTimeout("checkEntitlement(organizationId=$organizationId, entitlement=${entitlement.featureId})") {
        stigg.getBooleanEntitlement(
          GetBooleanEntitlementRequest
            .newBuilder()
            .setCustomerId(organizationId.value.toString())
            .setFeatureId(entitlement.featureId)
            .build(),
        )
      }

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

    if (featureFlagClient?.boolVariation(BypassStiggEntitlementChecks, Organization(organizationId.value)) == true) {
      logger.info { "Bypassing Stigg entitlements retrieval for organizationId=$organizationId" }
      return emptyList()
    }

    val result =
      withStiggTimeout("getEntitlements(organizationId=$organizationId)") {
        stigg.getEntitlements(
          GetEntitlementsRequest
            .newBuilder()
            .setCustomerId(organizationId.value.toString())
            .build(),
        )
      }

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

  fun getEntitlementsForPlan(plan: EntitlementPlan): List<Entitlement> {
    logger.debug { "Getting entitlements for plan=$plan" }

    try {
      val result =
        withStiggTimeout("getEntitlementsForPlan(plan=${plan.id})") {
          val query =
            GetPaywallQuery
              .builder()
              .input(
                GetPaywallInput
                  .builder()
                  .productId("product-airbyte")
                  .fetchAllCountriesPrices(true)
                  .build(),
              ).build()
          stigg.api().query(query)
        }

      logger.debug { "Paywall result for plan=$plan result=$result" }

      val plansList = result.paywall.paywallFragment.plans

      logger.debug { "Found ${plansList.size} plans in paywall" }

      for (planItem in plansList) {
        logger.debug {
          "plan.id=${plan.id} planItem.planFragment.refId=${planItem.planFragment.refId} planItem.planFragment.id=${planItem.planFragment.id}"
        }

        if (planItem == null || planItem.planFragment.refId != plan.id) continue

        return planItem.planFragment.entitlements.mapNotNull { Entitlements.fromId(it.packageEntitlementFragment.featureId) }
      }

      logger.warn { "Plan ${plan.id} not found in paywall data." }
      return emptyList()
    } catch (e: Exception) {
      logger.error(e) { "Error getting entitlements for plan=$plan" }
      return emptyList()
    }
  }
}
