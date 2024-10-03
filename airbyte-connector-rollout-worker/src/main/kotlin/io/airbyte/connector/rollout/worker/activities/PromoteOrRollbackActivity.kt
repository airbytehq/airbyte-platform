package io.airbyte.connector.rollout.worker.activities

import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPromoteOrRollback
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

@ActivityInterface
interface PromoteOrRollbackActivity {
  @ActivityMethod
  fun promoteOrRollback(input: ConnectorRolloutActivityInputPromoteOrRollback): Unit
}
