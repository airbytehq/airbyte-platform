/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ActorDefinitionVersionApi
import io.airbyte.api.client.model.generated.ActorDefinitionVersionRead
import io.airbyte.api.client.model.generated.GetActorDefinitionVersionDefaultRequestBody
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputVerifyDefaultVersion
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityOutputVerifyDefaultVersion
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException

private val logger = KotlinLogging.logger {}

@Singleton
class VerifyDefaultVersionActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
) : VerifyDefaultVersionActivity {
  init {
    logger.info { "Initialized VerifyDefaultVersionActivityImpl" }
  }

  override fun getAndVerifyDefaultVersion(
    input: ConnectorRolloutActivityInputVerifyDefaultVersion,
  ): ConnectorRolloutActivityOutputVerifyDefaultVersion {
    logger.info { "Verifying default version is ready ${input.dockerRepository}:${input.dockerImageTag}" }
    val client: ActorDefinitionVersionApi = airbyteApiClient.actorDefinitionVersionApi
    val body = GetActorDefinitionVersionDefaultRequestBody(input.actorDefinitionId)
    // Extract the prefix before any "-rc"
    val releaseCandidateTagPrefix = input.dockerImageTag.split("-")[0]
    val originalVersionTag = input.previousVersionDockerImageTag
    var isReleased = false

    // retry until we hit the time limit
    val startTime = System.currentTimeMillis()
    while (System.currentTimeMillis() - startTime < input.limit) {
      logger.info {
        "Trying to verify default version for ${input.dockerRepository}. " +
          "releaseCandidateVersionTag=${input.dockerImageTag} originalVersionTag=$originalVersionTag"
      }
      try {
        val response: ActorDefinitionVersionRead = client.getActorDefinitionVersionDefault(body)
        logger.info { "GetActorDefinitionVersionDefaultResponse = ${response.dockerImageTag}" }
        if (response.dockerImageTag != originalVersionTag) {
          if (response.dockerImageTag == releaseCandidateTagPrefix) {
            isReleased = true
          }
          logger.info {
            "Found new default version for ${input.dockerRepository}. " +
              "releaseCandidateVersionTag=${input.dockerImageTag} originalVersionTag=$originalVersionTag newVersionTag=${response.dockerImageTag}"
          }
          return ConnectorRolloutActivityOutputVerifyDefaultVersion(isReleased = isReleased)
        }
      } catch (e: IOException) {
        logger.error { "Error verifying default version for ${input.dockerRepository}:${input.dockerImageTag}: $e" }
        throw Activity.wrap(e)
      } catch (e: ClientException) {
        logger.error { "Error verifying default version for ${input.dockerRepository}:${input.dockerImageTag}: $e" }
        throw Activity.wrap(e)
      }

      heartbeatAndSleep(input.timeBetweenPolls.toLong())
    }

    throw IllegalStateException("Timed out waiting for default version to be ready")
  }

  fun heartbeatAndSleep(timeBetweenPolls: Long) {
    Activity.getExecutionContext().heartbeat<Any>(null)
    sleep(timeBetweenPolls)
  }

  private fun sleep(millis: Long) {
    try {
      Thread.sleep(millis)
    } catch (e: InterruptedException) {
      Thread.currentThread().interrupt()
      throw java.lang.RuntimeException(e)
    }
  }
}
