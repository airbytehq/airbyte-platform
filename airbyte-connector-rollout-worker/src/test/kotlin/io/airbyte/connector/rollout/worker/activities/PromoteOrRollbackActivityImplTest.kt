/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutRead
import io.airbyte.api.client.model.generated.ConnectorRolloutResponse
import io.airbyte.api.client.model.generated.ConnectorRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.connector.rollout.shared.models.ActionType
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPromoteOrRollback
import io.airbyte.connector.rollout.worker.runtime.AirbyteConnectorRolloutConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.verify
import okhttp3.Callback
import okhttp3.OkHttpClient
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.util.UUID

internal class PromoteOrRollbackActivityImplTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var connectorRolloutApi: ConnectorRolloutApi
  private lateinit var promoteOrRollbackActivity: PromoteOrRollbackActivityImpl
  private lateinit var airbyteConnectorRolloutConfig: AirbyteConnectorRolloutConfig

  companion object {
    private const val DOCKER_REPOSITORY = "airbyte/source-faker"
    private const val DOCKER_IMAGE_TAG = "0.1"
    private const val TECHNICAL_NAME = "source-faker"
    private val ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val ROLLOUT_ID = UUID.randomUUID()
    private val ROLLOUT_STRATEGY = ConnectorEnumRolloutStrategy.MANUAL
  }

  @BeforeEach
  fun setUp() {
    airbyteApiClient = mockk<AirbyteApiClient>()
    connectorRolloutApi = mockk<ConnectorRolloutApi>()
    every { airbyteApiClient.connectorRolloutApi } returns connectorRolloutApi
    mockkConstructor(OkHttpClient::class)
    every { anyConstructed<OkHttpClient>().newCall(any()) } returns
      mockk {
        every { enqueue(any()) } answers {
          val callback = it.invocation.args[0] as Callback
          callback.onResponse(
            mockk(),
            mockk {
              every { isSuccessful } returns true
            },
          )
        }
      }
    airbyteConnectorRolloutConfig =
      AirbyteConnectorRolloutConfig(
        githubRollout =
          AirbyteConnectorRolloutConfig.AirbyteConnectorGithubRolloutConfig(
            dispatchUrl = "https://fakeUrl.com",
            githubToken = "fakeToken",
          ),
      )
    promoteOrRollbackActivity = PromoteOrRollbackActivityImpl(airbyteApiClient, airbyteConnectorRolloutConfig)
  }

  @ParameterizedTest
  @EnumSource(ActionType::class)
  fun `test promoteOrRollback starts GHA and calls connectorRolloutApi`(actionType: ActionType) {
    every { connectorRolloutApi.updateConnectorRolloutState(any()) } returns getMockConnectorRolloutResponse()

    val input =
      ConnectorRolloutActivityInputPromoteOrRollback(
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = DOCKER_IMAGE_TAG,
        technicalName = TECHNICAL_NAME,
        rolloutId = ROLLOUT_ID,
        action = actionType,
        rolloutStrategy = ROLLOUT_STRATEGY,
      )

    promoteOrRollbackActivity.promoteOrRollback(input)

    verify { connectorRolloutApi.updateConnectorRolloutState(any()) }
    verify { anyConstructed<OkHttpClient>().newCall(any()) }
  }

  private fun getMockConnectorRolloutResponse(): ConnectorRolloutResponse =
    ConnectorRolloutResponse(
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
