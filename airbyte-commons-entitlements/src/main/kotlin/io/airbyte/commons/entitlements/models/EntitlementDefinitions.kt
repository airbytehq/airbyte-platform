/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements.models

object PlatformLlmSyncJobFailureExplanation : FeatureEntitlement(
  id = "feature-platform-llm-sync-job-failure-explanation",
)

object PlatformSubOneHourSyncFrequency : FeatureEntitlement(
  id = "feature-platform-sub-one-hour-sync-frequency",
)
