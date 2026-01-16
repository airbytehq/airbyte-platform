/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.runtime

import io.micronaut.context.annotation.ConfigurationProperties

internal const val DEFAULT_CONNECTOR_GITHUB_ROLLOUT_DISPATCH_URL =
  "https://api.github.com/repos/airbytehq/airbyte/actions/workflows/finalize_rollout.yml/dispatches"

@ConfigurationProperties("airbyte.connector_rollouts")
data class AirbyteConnectorRolloutConfig(
  val githubRollout: AirbyteConnectorGithubRolloutConfig = AirbyteConnectorGithubRolloutConfig(),
) {
  @ConfigurationProperties("github_workflow")
  data class AirbyteConnectorGithubRolloutConfig(
    val dispatchUrl: String = DEFAULT_CONNECTOR_GITHUB_ROLLOUT_DISPATCH_URL,
    val githubToken: String = "",
  )
}
