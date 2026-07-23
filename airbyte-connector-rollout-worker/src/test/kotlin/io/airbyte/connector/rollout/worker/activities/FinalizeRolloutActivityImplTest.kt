/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutFinalizeResponse
import io.airbyte.api.client.model.generated.ConnectorRolloutRead
import io.airbyte.api.client.model.generated.ConnectorRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFinalize
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class FinalizeRolloutActivityImplTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var connectorRolloutApi: ConnectorRolloutApi
  private lateinit var finalizeRolloutActivity: FinalizeRolloutActivityImpl

  companion object {
    private const val DOCKER_REPOSITORY = "airbyte/source-faker"
    private const val DOCKER_IMAGE_TAG = "0.2"
    private const val PREVIOUS_VERSION_DOCKER_IMAGE_TAG = "0.1"
    private val ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val ROLLOUT_ID = UUID.randomUUID()
    private val USER_ID = UUID.randomUUID()
    private val ROLLOUT_STRATEGY = ConnectorEnumRolloutStrategy.MANUAL
  }

  @BeforeEach
  fun setUp() {
    airbyteApiClient = mockk<AirbyteApiClient>()
    connectorRolloutApi = mockk<ConnectorRolloutApi>()
    every { airbyteApiClient.connectorRolloutApi } returns connectorRolloutApi
    finalizeRolloutActivity = FinalizeRolloutActivityImpl(airbyteApiClient)
  }

  @Test
  fun `test finalizeRollout calls connectorRolloutApi`() {
    every { connectorRolloutApi.finalizeConnectorRollout(any()) } returns getMockConnectorRolloutFinalizeResponse()

    val input =
      ConnectorRolloutActivityInputFinalize(
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = DOCKER_IMAGE_TAG,
        actorDefinitionId = ACTOR_DEFINITION_ID,
        rolloutId = ROLLOUT_ID,
        previousVersionDockerImageTag = PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        result = ConnectorRolloutFinalState.SUCCEEDED,
        updatedBy = USER_ID,
        rolloutStrategy = ROLLOUT_STRATEGY,
      )

    finalizeRolloutActivity.finalizeRollout(input)

    verify { connectorRolloutApi.finalizeConnectorRollout(any()) }
  }

  private fun getMockConnectorRolloutFinalizeResponse(): ConnectorRolloutFinalizeResponse =
    ConnectorRolloutFinalizeResponse(
      ConnectorRolloutRead(
        id = UUID.randomUUID(),
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = DOCKER_IMAGE_TAG,
        actorDefinitionId = ACTOR_DEFINITION_ID,
        initialVersionId = UUID.randomUUID(),
        releaseCandidateVersionId = UUID.randomUUID(),
        state = ConnectorRolloutState.SUCCEEDED,
      ),
    )
}
