/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared.models

import io.airbyte.api.model.generated.ConnectorRolloutActorSelectionInfo
import io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfo
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import java.util.UUID

class ConnectorRolloutWorkflowInput(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
  var rolloutId: UUID,
  var updatedBy: UUID? = null,
  var rolloutStrategy: ConnectorEnumRolloutStrategy? = null,
  var initialVersionDockerImageTag: String? = null,
  var connectorRollout: ConnectorRollout? = null,
  var actorSelectionInfo: ConnectorRolloutActorSelectionInfo? = null,
  var actorSyncs: Map<UUID, ConnectorRolloutActorSyncInfo>? = null,
  var migratePins: Boolean? = true,
  var waitBetweenRolloutSeconds: Int? = null,
  var waitBetweenSyncResultsQueriesSeconds: Int? = null,
  var rolloutExpirationSeconds: Int? = null,
)
