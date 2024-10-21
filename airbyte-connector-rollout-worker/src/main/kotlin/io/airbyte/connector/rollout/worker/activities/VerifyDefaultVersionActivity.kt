package io.airbyte.connector.rollout.worker.activities

import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputVerifyDefaultVersion
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

@ActivityInterface
interface VerifyDefaultVersionActivity {
  @ActivityMethod
  fun verifyDefaultVersion(input: ConnectorRolloutActivityInputVerifyDefaultVersion): Unit
}
