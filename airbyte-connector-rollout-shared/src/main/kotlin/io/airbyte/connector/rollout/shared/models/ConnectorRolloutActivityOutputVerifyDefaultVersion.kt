package io.airbyte.connector.rollout.shared.models

data class ConnectorRolloutActivityOutputVerifyDefaultVersion(
  var isReleased: Boolean,
)
