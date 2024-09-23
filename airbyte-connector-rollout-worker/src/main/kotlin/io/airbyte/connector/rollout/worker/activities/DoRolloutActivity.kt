/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutOutput
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

@ActivityInterface
interface DoRolloutActivity {
  @ActivityMethod
  fun doRollout(input: ConnectorRolloutActivityInputRollout): ConnectorRolloutOutput
}
