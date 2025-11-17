/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.generated.HealthApi
import io.airbyte.api.client.model.generated.ConnectorRolloutRead
import io.airbyte.api.client.model.generated.ConnectorRolloutStartResponse
import io.airbyte.api.client.model.generated.ConnectorRolloutState
import io.airbyte.api.client.model.generated.HealthCheckRead
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputStart
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class StartRolloutActivityImplTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var connectorRolloutApi: ConnectorRolloutApi
  private lateinit var healthApi: HealthApi
  private lateinit var startRolloutActivity: StartRolloutActivityImpl

  companion object {
    private const val DOCKER_REPOSITORY = "airbyte/source-faker"
    private const val DOCKER_IMAGE_TAG = "0.1"
    private val ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val ROLLOUT_ID = UUID.randomUUID()
    private val USER_ID = UUID.randomUUID()
    private val ROLLOUT_STRATEGY = ConnectorEnumRolloutStrategy.MANUAL
  }

  @BeforeEach
  fun setUp() {
    airbyteApiClient = mockk<AirbyteApiClient>()
    connectorRolloutApi = mockk<ConnectorRolloutApi>()
    healthApi = mockk<HealthApi>()
    every { airbyteApiClient.connectorRolloutApi } returns connectorRolloutApi
    every { airbyteApiClient.healthApi } returns healthApi
    startRolloutActivity = StartRolloutActivityImpl(airbyteApiClient)
  }

  @Test
  fun `test startRollout calls connectorRolloutApi`() {
    every { healthApi.getHealthCheck() } returns HealthCheckRead(available = true)
    every { connectorRolloutApi.startConnectorRollout(any()) } returns getMockConnectorRolloutResponse()

    val input =
      ConnectorRolloutActivityInputStart(
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = DOCKER_IMAGE_TAG,
        actorDefinitionId = ACTOR_DEFINITION_ID,
        rolloutId = ROLLOUT_ID,
        updatedBy = USER_ID,
        rolloutStrategy = ROLLOUT_STRATEGY,
      )

    startRolloutActivity.startRollout("workflowRunId", input)

    verify { connectorRolloutApi.startConnectorRollout(any()) }
  }

  @Test
  fun `test startRollout calls connectorRolloutApi with null values`() {
    every { healthApi.getHealthCheck() } returns HealthCheckRead(available = true)
    every { connectorRolloutApi.startConnectorRollout(any()) } returns getMockConnectorRolloutResponse()

    val input =
      ConnectorRolloutActivityInputStart(
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = DOCKER_IMAGE_TAG,
        actorDefinitionId = ACTOR_DEFINITION_ID,
        rolloutId = ROLLOUT_ID,
        updatedBy = USER_ID,
        rolloutStrategy = null,
      )

    startRolloutActivity.startRollout("workflowRunId", input)

    verify { connectorRolloutApi.startConnectorRollout(any()) }
  }

  private fun getMockConnectorRolloutResponse(): ConnectorRolloutStartResponse =
    ConnectorRolloutStartResponse(
      ConnectorRolloutRead(
        id = UUID.randomUUID(),
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = DOCKER_IMAGE_TAG,
        actorDefinitionId = ACTOR_DEFINITION_ID,
        initialVersionId = UUID.randomUUID(),
        releaseCandidateVersionId = UUID.randomUUID(),
        state = ConnectorRolloutState.IN_PROGRESS,
      ),
    )
}
