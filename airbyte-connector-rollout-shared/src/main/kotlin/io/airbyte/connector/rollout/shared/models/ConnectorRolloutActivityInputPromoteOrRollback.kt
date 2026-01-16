/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared.models

import io.airbyte.config.ConnectorEnumRolloutStrategy
import java.util.UUID

data class ConnectorRolloutActivityInputPromoteOrRollback(
  var dockerRepository: String,
  var dockerImageTag: String,
  var technicalName: String,
  var action: ActionType,
  var rolloutId: UUID,
  var rolloutStrategy: ConnectorEnumRolloutStrategy? = null,
)
