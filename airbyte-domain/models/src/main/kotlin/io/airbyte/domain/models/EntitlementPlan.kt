/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import javax.print.attribute.standard.ColorSupported.SUPPORTED

// Entitlement plans defined in our entitlement service (Stigg).
// Plan IDs map to a plan ID in Stigg.
//
// Note:
//   Stigg gives users access to features that they might need before they run a sync, so we need to add them to an EntitlementPlan on org creation.
//   This is different than what we do in Orb, which is start the free trial once the org has a successful sync.
//   We it this way in Stigg because we aren't using Stigg's "free trial", which would give them e.g. 30 days free once we add them to a plan.
//   Instead, we'll add them to a plan that we call "free trial" but then when they actually pay they'll be moved to the real plan.
//   If they don't pay, all of their syncs will be disabled. At that point we could also remove them from the plan that they're on.
//
//   In the future, if we integrate with Stigg billing, we may consolidate the free trials with the associated subscription.
//   If we do that, we'll want to make sure that orgs have access to features, but that the clock doesn't start until the first successful sync.
//   It may be possible to do this with Stigg, but if not, we may need to do something like update the trial start date via
//   OrbSubscriptionService.subscribeToSelfServePlanAndStartTrialIfNeeded
enum class EntitlementPlan(
  val id: String,
) {
  CORE("plan-airbyte-core"),
  SME("plan-airbyte-sme"),

  // Cloud plans
  // Self-serve
  STANDARD("plan-airbyte-standard"),
  STANDARD_TRIAL("plan-airbyte-standard-trial"),

  // fka TEAMS
  PRO("plan-airbyte-pro"),
  UNIFIED_TRIAL("plan-airbyte-unified-trial"),

  // Partners who get Airbyte free, forever
  PARTNER("plan-airbyte-partner"),
  POV("plan-airbyte-pov"),

  // fka Cloud Enterprise
  FLEX("plan-airbyte-flex"),
  ;

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
