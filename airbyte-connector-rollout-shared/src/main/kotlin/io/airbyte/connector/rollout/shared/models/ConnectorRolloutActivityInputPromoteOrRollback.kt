package io.airbyte.connector.rollout.shared.models

import java.util.UUID

data class ConnectorRolloutActivityInputPromoteOrRollback(
  var dockerRepository: String,
  var dockerImageTag: String,
  var technicalName: String,
  var action: ActionType,
  var rolloutId: UUID,
)
