/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputGet
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutOutput
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

@ActivityInterface
interface GetRolloutActivity {
  @ActivityMethod
  fun getRollout(input: ConnectorRolloutActivityInputGet): ConnectorRolloutOutput
}
