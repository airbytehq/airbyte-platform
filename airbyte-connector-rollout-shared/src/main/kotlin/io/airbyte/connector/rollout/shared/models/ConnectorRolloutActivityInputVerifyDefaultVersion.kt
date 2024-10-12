package io.airbyte.connector.rollout.shared.models

import io.airbyte.connector.rollout.shared.Constants
import java.util.UUID

data class ConnectorRolloutActivityInputVerifyDefaultVersion(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
  var rolloutId: UUID,
  // 15 minutes
  var limit: Int = Constants.VERIFY_ACTIVITY_TIMEOUT_MILLIS,
  // 30 seconds
  var timeBetweenPolls: Int = Constants.VERIFY_ACTIVITY_TIME_BETWEEN_POLLS_MILLIS,
)
