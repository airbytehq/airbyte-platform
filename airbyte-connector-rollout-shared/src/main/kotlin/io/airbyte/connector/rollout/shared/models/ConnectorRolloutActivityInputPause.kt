package io.airbyte.connector.rollout.shared.models

import io.airbyte.config.ConnectorEnumRolloutStrategy
import java.util.UUID

class ConnectorRolloutActivityInputPause(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
  var rolloutId: UUID,
  var pausedReason: String,
  var updatedBy: UUID? = null,
  var rolloutStrategy: ConnectorEnumRolloutStrategy? = null,
)
