/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared.models

import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRolloutFinalState
import java.util.UUID

data class ConnectorRolloutActivityInputFinalize(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
  var rolloutId: UUID,
  var previousVersionDockerImageTag: String,
  var result: ConnectorRolloutFinalState,
  var errorMsg: String? = null,
  var failedReason: String? = null,
  var updatedBy: UUID? = null,
  var rolloutStrategy: ConnectorEnumRolloutStrategy? = null,
  var retainPinsOnCancellation: Boolean = true,
)
