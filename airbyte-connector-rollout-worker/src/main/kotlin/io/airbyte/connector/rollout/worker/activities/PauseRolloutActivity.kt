package io.airbyte.connector.rollout.worker.activities

import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPause
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

@ActivityInterface
interface PauseRolloutActivity {
  @ActivityMethod
  fun pauseRollout(input: ConnectorRolloutActivityInputPause): ConnectorRolloutOutput
}
