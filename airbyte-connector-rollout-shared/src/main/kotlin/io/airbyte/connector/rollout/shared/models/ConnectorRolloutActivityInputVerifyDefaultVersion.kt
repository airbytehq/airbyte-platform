package io.airbyte.connector.rollout.shared.models

import java.util.UUID

data class ConnectorRolloutActivityInputVerifyDefaultVersion(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
  var rolloutId: UUID,
  // 15 minutes
  var limit: Int = 900000,
  // 30 seconds
  var timeBetweenPolls: Int = 30000,
)
