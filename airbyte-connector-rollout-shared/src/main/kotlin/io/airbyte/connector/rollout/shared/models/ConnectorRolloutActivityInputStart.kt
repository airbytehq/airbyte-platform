package io.airbyte.connector.rollout.shared.models

import io.airbyte.config.ConnectorEnumRolloutStrategy
import java.util.UUID

data class ConnectorRolloutActivityInputStart(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
  var rolloutId: UUID,
  var updatedBy: UUID? = null,
  var rolloutStrategy: ConnectorEnumRolloutStrategy? = null,
  var migratePins: Boolean? = true,
)
