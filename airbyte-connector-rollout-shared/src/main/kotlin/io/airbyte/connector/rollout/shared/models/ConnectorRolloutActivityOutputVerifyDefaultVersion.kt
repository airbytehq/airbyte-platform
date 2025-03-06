/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared.models

data class ConnectorRolloutActivityOutputVerifyDefaultVersion(
  var isReleased: Boolean,
)
