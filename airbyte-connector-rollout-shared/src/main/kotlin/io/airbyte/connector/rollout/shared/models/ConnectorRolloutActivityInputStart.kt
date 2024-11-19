package io.airbyte.connector.rollout.shared.models

import io.airbyte.api.model.generated.ConnectorRolloutActorSelectionInfo
import io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfo
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import java.util.UUID

data class ConnectorRolloutActivityInputStart(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
  var rolloutId: UUID,
  var updatedBy: UUID? = null,
  var rolloutStrategy: ConnectorEnumRolloutStrategy? = null,
  var initialVersionDockerImageTag: String? = null,
  var connectorRollout: ConnectorRollout? = null,
  var actorSelectionInfo: ConnectorRolloutActorSelectionInfo? = null,
  var actorSyncs: List<ConnectorRolloutActorSyncInfo>? = null,
)
