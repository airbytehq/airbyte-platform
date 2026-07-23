/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import com.google.gson.Gson
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectorRolloutState
import io.airbyte.api.client.model.generated.ConnectorRolloutUpdateStateRequestBody
import io.airbyte.connector.rollout.shared.ConnectorRolloutActivityHelpers
import io.airbyte.connector.rollout.shared.models.ActionType
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPromoteOrRollback
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.airbyte.connector.rollout.worker.runtime.AirbyteConnectorRolloutConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import okhttp3.Call
import okhttp3.Callback
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.io.IOException

private val logger = KotlinLogging.logger {}

@Singleton
@Requires(property = "airbyte.connector_rollouts.github_workflow.dispatch_url")
@Requires(property = "airbyte.connector_rollouts.github_workflow.github_token")
class PromoteOrRollbackActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
  private val airbyteConnectorRolloutConfig: AirbyteConnectorRolloutConfig,
) : PromoteOrRollbackActivity {
  init {
    logger.info { "Initialized PromotePromoteOrRollbackActivityImpl" }
  }

  override fun promoteOrRollback(input: ConnectorRolloutActivityInputPromoteOrRollback): ConnectorRolloutOutput {
    triggerGitHubWorkflow(input.dockerRepository, input.dockerImageTag, input.technicalName, input.action)
    val response =
      airbyteApiClient.connectorRolloutApi.updateConnectorRolloutState(
        ConnectorRolloutUpdateStateRequestBody(ConnectorRolloutState.FINALIZING, input.rolloutId),
      )
    return ConnectorRolloutActivityHelpers.mapToConnectorRollout(response.data)
  }

  private fun triggerGitHubWorkflow(
    dockerRepository: String,
    releaseCandidateVersion: String,
    connectorName: String,
    action: ActionType,
  ) {
    val client = OkHttpClient()

    val jsonBody =
      mapOf(
        "ref" to "master",
        "inputs" to
          mapOf(
            "connector_name" to connectorName,
            "action" to action.toString().lowercase(),
          ),
      )
    val jsonBodyString = Gson().toJson(jsonBody)
    val body = jsonBodyString.toRequestBody("application/json; charset=utf-8".toMediaType())

    val request =
      Request
        .Builder()
        .url(airbyteConnectorRolloutConfig.githubRollout.dispatchUrl)
        .post(body)
        .addHeader("Authorization", "Bearer ${airbyteConnectorRolloutConfig.githubRollout.githubToken}")
        .addHeader("Accept", "application/vnd.github+json")
        .addHeader("X-GitHub-Api-Version", "2022-11-28")
        .build()

    client.newCall(request).enqueue(
      object : Callback {
        override fun onFailure(
          call: Call,
          e: IOException,
        ) {
          logger.info {
            "Failed to trigger GHA workflow for $dockerRepository:$releaseCandidateVersion: " +
              "request.url: ${request.url}, request.body: $jsonBodyString"
          }
          throw e
        }

        override fun onResponse(
          call: Call,
          response: Response,
        ) {
          if (response.isSuccessful) {
            logger.info { "Successfully triggered GHA workflow to $action $dockerRepository:$releaseCandidateVersion" }
            return
          } else {
            throw IOException(
              "Failed to trigger GHA workflow for $dockerRepository:$releaseCandidateVersion: ${response.code}, ${response.body?.string()}\n" +
                "request.url: ${request.url}, request.body: $jsonBodyString",
            )
          }
        }
      },
    )
  }
}
