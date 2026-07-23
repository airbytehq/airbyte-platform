/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputCleanup
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

@ActivityInterface
interface CleanupActivity {
  @ActivityMethod
  fun cleanup(input: ConnectorRolloutActivityInputCleanup)
}
