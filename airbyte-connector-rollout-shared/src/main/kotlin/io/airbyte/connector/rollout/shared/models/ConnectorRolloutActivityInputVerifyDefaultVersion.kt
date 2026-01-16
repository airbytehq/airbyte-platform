/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared.models

import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.connector.rollout.shared.Constants
import java.util.UUID

data class ConnectorRolloutActivityInputVerifyDefaultVersion(
  var dockerRepository: String,
  var dockerImageTag: String,
  var actorDefinitionId: UUID,
  var rolloutId: UUID,
  var previousVersionDockerImageTag: String,
  var limit: Int = Constants.VERIFY_ACTIVITY_TIMEOUT_MILLIS,
  var timeBetweenPolls: Int = Constants.VERIFY_ACTIVITY_TIME_BETWEEN_POLLS_MILLIS,
  var rolloutStrategy: ConnectorEnumRolloutStrategy? = null,
)
