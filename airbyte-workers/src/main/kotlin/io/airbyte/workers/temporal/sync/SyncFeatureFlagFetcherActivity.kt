package io.airbyte.workers.temporal.sync

import io.airbyte.workers.temporal.activities.SyncFeatureFlagFetcherInput
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

@ActivityInterface
interface SyncFeatureFlagFetcherActivity {
  @ActivityMethod
  fun shouldRunAsChildWorkflow(input: SyncFeatureFlagFetcherInput): Boolean
}
