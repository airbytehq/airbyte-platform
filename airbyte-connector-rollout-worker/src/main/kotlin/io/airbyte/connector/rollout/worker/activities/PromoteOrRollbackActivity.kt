/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPromoteOrRollback
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

@ActivityInterface
interface PromoteOrRollbackActivity {
  @ActivityMethod
  fun promoteOrRollback(input: ConnectorRolloutActivityInputPromoteOrRollback): ConnectorRolloutOutput
}
