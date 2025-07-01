/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements.models

object PlatformLlmSyncJobFailureExplanation : FeatureEntitlement(
  featureId = "feature-platform-llm-sync-job-failure-explanation",
)

object PlatformSubOneHourSyncFrequency : FeatureEntitlement(
  featureId = "feature-platform-sub-one-hour-sync-frequency",
)

object Entitlements {
  private val ALL =
    listOf(
      PlatformLlmSyncJobFailureExplanation,
      PlatformSubOneHourSyncFrequency,
    )

  private val byId = ALL.associateBy { it.featureId }

  fun fromId(featureId: String): Entitlement? =
    if (ConnectorEntitlement.isConnectorFeatureId(featureId)) {
      ConnectorEntitlement.fromFeatureId(featureId)
    } else {
      byId[featureId]
    }
}
