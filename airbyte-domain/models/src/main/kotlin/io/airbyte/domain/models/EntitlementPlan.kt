/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import javax.print.attribute.standard.ColorSupported.SUPPORTED

// Entitlement plans defined in our entitlement service (Stigg).
// Plan IDs map to a plan ID in Stigg.
// Plan values provide a way to compare plans based on functionality;
// 0 is basic functionality, and 1 means the org has access to features that aren't available in other plans.
// Orgs may not be automatically move from a higher-value plan to a lower-value plan at the moment, because we do not have
// code in place for feature degradation.
//
// Note:
//   Stigg gives users access to features that they might need before they run a sync, so we need to add them to an EntitlementPlan on org creation.
//   This is different than what we do in Orb, which is start the free trial once the org has a successful sync.
//   We it this way in Stigg because we aren’t using Stigg’s “free trial”, which would give them e.g. 30 days free once we add them to a plan.
//   Instead, we’ll add them to a plan that we call “free trial” but then when they actually pay they’ll be moved to the real plan.
//   If they don’t pay, all of their syncs will be disabled. At that point we could also remove them from the plan that they’re on.
//   However, we haven't implemented feature degradation so for now need to preserve the highest plan they're on.
//
//   In the future, if we integrate with Stigg billing, we may consolidate the free trials with the associated subscription.
//   If we do that, we'll want to make sure that orgs have access to features, but that the clock doesn't start until the first successful sync.
//   It may be possible to do this with Stigg, but if not, we may need to do something like update the trial start date via
//   OrbSubscriptionService.subscribeToSelfServePlanAndStartTrialIfNeeded
enum class EntitlementPlan(
  val id: String,
  val value: Int,
) {
  // OSS (lowest tier)
  CORE("plan-airbyte-core", 0),
  SME("plan-airbyte-sme", 0),

  // Cloud plans
  // Self-serve
  STANDARD("plan-airbyte-standard", 0),
  STANDARD_TRIAL("plan-airbyte-standard-trial", 0),

  // fka Cloud Enterprise
  FLEX("plan-airbyte-flex", 1),

  // fka TEAMS
  PRO("plan-airbyte-pro", 2),
  PRO_TRIAL("plan-airbyte-unified-trial", 2),

  // Partners who get Airbyte free, forever
  PARTNER("plan-airbyte-partner", 2),
  ;

  fun isGreaterOrEqualTo(other: EntitlementPlan): Boolean = this.value >= other.value

  fun isLessThan(other: EntitlementPlan): Boolean = this.value < other.value

  companion object {
    fun fromId(id: String): EntitlementPlan =
      entries.firstOrNull { it.id == id }
        ?: throw IllegalArgumentException("No EntitlementPlan with id=$id")
  }
}

enum class SupportedOrbPlan(
  val plan: String,
) {
  CLOUD_LEGACY("Airbyte Cloud (Legacy Plan)"),
  CLOUD_SELF_SERVE_ANNUAL("Airbyte Cloud (Annual Subscription)"),
  CLOUD_SELF_SERVE_MONTHLY("Airbyte Cloud (Monthly Subscription)"),
  PRO("Airbyte Teams"),
  PRO_LEGACY("Airbyte Teams (Legacy Plan)"),
  PARTNER("Airbyte Partner"),
}
