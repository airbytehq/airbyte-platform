/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.MaximumWorkspacesEntitlement
import io.airbyte.commons.entitlements.models.SsoEntitlement
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.PlusTiersEnabled
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

internal const val DOWNGRADE_STEP_SSO = "sso"
internal const val DOWNGRADE_STEP_WORKSPACES = "workspaces"

/** Plans without a workspace entitlement (Standard) allow exactly one workspace. */
internal const val DEFAULT_WORKSPACE_LIMIT = 1L

sealed interface PlanLimitStepResult {
  data object NotNeeded : PlanLimitStepResult

  data class Completed(
    val summary: String,
  ) : PlanLimitStepResult

  data class Failed(
    val step: String,
    val message: String?,
  ) : PlanLimitStepResult
}

data class PlanLimitEnforcementResult(
  val sso: PlanLimitStepResult,
  val workspaces: PlanLimitStepResult,
) {
  companion object {
    val SKIPPED = PlanLimitEnforcementResult(PlanLimitStepResult.NotNeeded, PlanLimitStepResult.NotNeeded)
  }
}

/**
 * Brings an organization's resources back within what its current entitlement plan allows: removes SSO when the
 * plan no longer includes it and merges workspaces down to the plan's workspace limit.
 *
 * Only the Standard plan is enforced. It is the only self-serve plan whose limits sit below what Plus grants, and
 * acting on any other plan (including a Plus tier change, which re-asserts the Plus plan) would tear down support
 * exceptions such as an extra workspace an instance admin created.
 *
 * Runs after every plan change and again whenever the current plan is re-asserted, so every step is idempotent.
 * Steps are driven by the organization's actual entitlements, never by a plan diff: when the entitlement platform
 * cannot be read, nothing destructive runs.
 */
@Singleton
internal class PlanLimitEnforcementService(
  private val entitlementClient: EntitlementClient,
  private val entitlementProvider: EntitlementProvider,
  private val featureFlagClient: FeatureFlagClient,
  private val metricClient: MetricClient,
  private val ssoTeardownStep: SsoTeardownStep,
  private val workspaceConsolidationStep: WorkspaceConsolidationStep,
) {
  fun enforce(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ): PlanLimitEnforcementResult {
    if (plan != EntitlementPlan.STANDARD) {
      logger.debug { "Plan limit enforcement only applies to the Standard plan. organizationId=$organizationId plan=$plan" }
      return PlanLimitEnforcementResult.SKIPPED
    }
    if (!featureFlagClient.boolVariation(PlusTiersEnabled, Organization(organizationId.value))) {
      logger.info { "Plan limit enforcement is disabled by feature flag. organizationId=$organizationId" }
      return PlanLimitEnforcementResult.SKIPPED
    }

    // Throws when the entitlement platform is unavailable. That is deliberate: without a definitive read of the
    // organization's entitlements nothing destructive may run.
    val entitlements = entitlementClient.getEntitlements(organizationId)
    if (entitlements.isEmpty()) {
      logger.warn { "No entitlements returned for organization. Skipping plan limit enforcement. organizationId=$organizationId" }
      return PlanLimitEnforcementResult.SKIPPED
    }

    val sso =
      runStep(organizationId, DOWNGRADE_STEP_SSO) {
        if (isEntitledToSso(organizationId, entitlements)) PlanLimitStepResult.NotNeeded else ssoTeardownStep.run(organizationId)
      }
    val workspaces =
      runStep(organizationId, DOWNGRADE_STEP_WORKSPACES) {
        when (val limit = workspaceLimit(entitlements)) {
          null -> {
            logger.info { "No workspace limit to enforce. organizationId=$organizationId" }
            PlanLimitStepResult.NotNeeded
          }
          else -> workspaceConsolidationStep.run(organizationId, limit)
        }
      }
    logger.info { "Plan limit enforcement finished. organizationId=$organizationId sso=$sso workspaces=$workspaces" }
    return PlanLimitEnforcementResult(sso, workspaces)
  }

  /**
   * Mirrors EntitlementServiceImpl.hasSsoConfigUpdateEntitlement: the feature flag override grants SSO as well.
   * An inconclusive entitlement check counts as entitled so that a platform hiccup never tears SSO down.
   */
  private fun isEntitledToSso(
    organizationId: OrganizationId,
    entitlements: List<EntitlementResult>,
  ): Boolean {
    val sso = entitlements.firstOrNull { it.featureId == SsoEntitlement.featureId }
    if (sso != null && !sso.isEntitlementCheckSuccessful) {
      return true
    }
    return sso?.isEntitled == true || entitlementProvider.hasSsoConfigUpdateEntitlement(organizationId)
  }

  /**
   * The entitlement platform only lists granted entitlements, and Standard does not grant a workspace entitlement
   * at all, so an organization without it is allowed [DEFAULT_WORKSPACE_LIMIT] workspace. A granted entitlement
   * carries its limit, or is unlimited. Returns null when there is no limit to enforce.
   */
  private fun workspaceLimit(entitlements: List<EntitlementResult>): Long? {
    val result =
      entitlements.firstOrNull { it.featureId == MaximumWorkspacesEntitlement.featureId }
        ?: return DEFAULT_WORKSPACE_LIMIT
    return when {
      !result.isEntitlementCheckSuccessful -> null
      !result.isEntitled -> DEFAULT_WORKSPACE_LIMIT
      result.isUnlimited -> null
      else -> result.value
    }
  }

  private fun runStep(
    organizationId: OrganizationId,
    step: String,
    block: () -> PlanLimitStepResult,
  ): PlanLimitStepResult =
    try {
      block().also { result ->
        if (result is PlanLimitStepResult.Completed) {
          logger.info { "Plan limit enforcement step completed. organizationId=$organizationId step=$step ${result.summary}" }
          recordStep(organizationId, step, success = true)
        }
      }
    } catch (e: Exception) {
      logger.error(e) { "Plan limit enforcement step failed. organizationId=$organizationId step=$step" }
      recordStep(organizationId, step, success = false)
      PlanLimitStepResult.Failed(step, e.message)
    }

  private fun recordStep(
    organizationId: OrganizationId,
    step: String,
    success: Boolean,
  ) {
    metricClient.count(
      OssMetricsRegistry.ENTITLEMENT_PLAN_DOWNGRADE_STEP,
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.ORGANIZATION_ID, organizationId.toString()),
          MetricAttribute(MetricTags.DOWNGRADE_STEP, step),
          MetricAttribute(MetricTags.SUCCESS, success.toString()),
        ),
    )
  }
}
