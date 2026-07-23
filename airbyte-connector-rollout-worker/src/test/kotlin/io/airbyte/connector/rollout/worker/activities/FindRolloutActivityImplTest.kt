/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutListResponse
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFind
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class FindRolloutActivityImplTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var connectorRolloutApi: ConnectorRolloutApi
  private lateinit var findRolloutActivity: FindRolloutActivityImpl

  companion object {
    private const val DOCKER_REPOSITORY = "airbyte/source-faker"
    private const val DOCKER_IMAGE_TAG = "0.1"
    private val ACTOR_DEFINITION_ID = UUID.randomUUID()
  }

  @BeforeEach
  fun setUp() {
    airbyteApiClient = mockk<AirbyteApiClient>()
    connectorRolloutApi = mockk<ConnectorRolloutApi>()
    every { airbyteApiClient.connectorRolloutApi } returns connectorRolloutApi
    findRolloutActivity = FindRolloutActivityImpl(airbyteApiClient)
  }

  @Test
  fun `test findRollout calls connectorRolloutApi`() {
    every { connectorRolloutApi.getConnectorRolloutsList(any()) } returns ConnectorRolloutListResponse()

    val input =
      ConnectorRolloutActivityInputFind(
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = DOCKER_IMAGE_TAG,
        actorDefinitionId = ACTOR_DEFINITION_ID,
      )

    findRolloutActivity.findRollout(input)

    verify { connectorRolloutApi.getConnectorRolloutsList(any()) }
  }
}
