/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import java.util.Objects
import java.util.UUID

/**
 * Activity that fetches feature flags.
 */
@ActivityInterface
interface FeatureFlagFetchActivity {
  /**
   * Feature flag fetch input.
   */
  class FeatureFlagFetchInput {
    @JvmField
    var connectionId: UUID? = null

    constructor()

    constructor(connectionId: UUID?) {
      this.connectionId = connectionId
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as FeatureFlagFetchInput
      return connectionId == that.connectionId
    }

    override fun hashCode(): Int = Objects.hashCode(connectionId)

    override fun toString(): String = "FeatureFlagFetchInput{connectionId=" + connectionId + '}'
  }

  /**
   * Feature flag fetch output.
   */
  class FeatureFlagFetchOutput {
    @JvmField
    var featureFlags: MutableMap<String?, Boolean?>? = null

    constructor()

    constructor(featureFlags: MutableMap<String?, Boolean?>?) {
      this.featureFlags = featureFlags
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as FeatureFlagFetchOutput
      return featureFlags == that.featureFlags
    }

    override fun hashCode(): Int = Objects.hashCode(featureFlags)

    override fun toString(): String = "FeatureFlagFetchOutput{featureFlags=" + featureFlags + '}'
  }

  /**
   * Return latest value for feature flags relevant to the ConnectionManagerWorkflow.
   */
  @ActivityMethod
  fun getFeatureFlags(input: FeatureFlagFetchInput): FeatureFlagFetchOutput
}
