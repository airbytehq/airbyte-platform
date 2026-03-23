/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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

    @JvmField
    var organizationId: UUID? = null

    constructor()

    constructor(connectionId: UUID?) {
      this.connectionId = connectionId
    }

    constructor(connectionId: UUID?, organizationId: UUID?) {
      this.connectionId = connectionId
      this.organizationId = organizationId
    }

    override fun equals(o: Any?): Boolean {
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val that = o as FeatureFlagFetchInput
      return connectionId == that.connectionId && organizationId == that.organizationId
    }

    override fun hashCode(): Int = Objects.hash(connectionId, organizationId)

    override fun toString(): String = "FeatureFlagFetchInput{connectionId=$connectionId, organizationId=$organizationId}"
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
