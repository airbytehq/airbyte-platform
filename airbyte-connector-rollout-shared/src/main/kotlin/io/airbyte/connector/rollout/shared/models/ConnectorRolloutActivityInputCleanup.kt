package io.airbyte.connector.rollout.shared.models

import io.airbyte.config.ConnectorEnumRolloutState
import java.util.UUID

data class ConnectorRolloutActivityInputCleanup(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
  var rolloutId: UUID?,
  var newState: ConnectorEnumRolloutState,
  var errorMsg: String? = null,
  var failureMsg: String? = null,
)
