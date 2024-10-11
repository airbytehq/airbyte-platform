package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ActorDefinitionVersionApi
import io.airbyte.api.client.model.generated.ActorDefinitionVersionRead
import io.airbyte.api.client.model.generated.GetActorDefinitionVersionDefaultRequestBody
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputVerifyDefaultVersion
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.Activity
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import java.io.IOException

private val logger = KotlinLogging.logger {}

@Singleton
class VerifyDefaultVersionActivityImpl(private val airbyteApiClient: AirbyteApiClient) : VerifyDefaultVersionActivity {
  init {
    logger.info { "Initialized VerifyDefaultVersionActivityImpl" }
  }

  override fun verifyDefaultVersion(input: ConnectorRolloutActivityInputVerifyDefaultVersion) {
    logger.info { "Verifying default version is ready ${input.dockerRepository}:${input.dockerImageTag}" }
    val client: ActorDefinitionVersionApi = airbyteApiClient.actorDefinitionVersionApi
    val body = GetActorDefinitionVersionDefaultRequestBody(input.actorDefinitionId)
    val releaseCandidateTagPrefix = input.dockerImageTag.split("-")[0] // Extract the prefix before any "-rc"

    // retry until we hit the time limit
    val startTime = System.currentTimeMillis()
    while (System.currentTimeMillis() - startTime < input.limit) {
      logger.info { "Trying to verify default version for ${input.dockerRepository}:${input.dockerImageTag}" }
      try {
        val response: ActorDefinitionVersionRead = client.getActorDefinitionVersionDefault(body)
        logger.info { "GetActorDefinitionVersionDefaultResponse = ${response.dockerImageTag}" }

        if (response.dockerImageTag == releaseCandidateTagPrefix) {
          return
        } else {
          // sleep for 30 seconds and retry
          Thread.sleep(input.timeBetweenPolls.toLong())
        }
      } catch (e: Exception) {
        when (e) {
          is IOException, is ClientException -> {
            logger.error { "Error verifying default version for ${input.dockerRepository}:${input.dockerImageTag}: $e" }
            throw Activity.wrap(e)
          }
        }
      }
    }
    throw IllegalStateException("Timed out waiting for default version to be ready")
  }
}
