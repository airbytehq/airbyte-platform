/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ActorDefinitionVersionApi
import io.airbyte.api.client.model.generated.ActorDefinitionVersionRead
import io.airbyte.api.client.model.generated.SupportState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputVerifyDefaultVersion
import io.airbyte.connector.rollout.worker.activities.VerifyDefaultVersionActivityImpl
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class VerifyDefaultVersionActivityImplTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var actorDefinitionVersionApi: ActorDefinitionVersionApi
  private lateinit var verifyDefaultVersionActivity: VerifyDefaultVersionActivityImpl

  companion object {
    private const val DOCKER_REPOSITORY = "airbyte/source-faker"
    private const val PREVIOUS_VERSION_DOCKER_IMAGE_TAG = "0.1"
    private const val NEWER_VERSION_DOCKER_IMAGE_TAG = "0.3"
    private const val DOCKER_IMAGE_TAG = "0.2"
    private val ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val ROLLOUT_ID = UUID.randomUUID()
    private val ROLLOUT_STRATEGY = ConnectorEnumRolloutStrategy.MANUAL
  }

  @BeforeEach
  fun setUp() {
    airbyteApiClient = mockk<AirbyteApiClient>()
    actorDefinitionVersionApi = mockk<ActorDefinitionVersionApi>()
    every { airbyteApiClient.actorDefinitionVersionApi } returns actorDefinitionVersionApi

    val realActivity = VerifyDefaultVersionActivityImpl(airbyteApiClient)
    verifyDefaultVersionActivity = spyk(realActivity)

    every { verifyDefaultVersionActivity.heartbeatAndSleep(any()) } just Runs
  }

  @Test
  fun `test verifyDefaultVersion`() {
    // Mock the ActorDefinitionVersionApi to return the response dynamically
    every {
      actorDefinitionVersionApi.getActorDefinitionVersionDefault(any())
    } returnsMany
      listOf(
        ActorDefinitionVersionRead(
          // Initial matching tag
          dockerImageTag = PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
          dockerRepository = DOCKER_REPOSITORY,
          isVersionOverrideApplied = true,
          supportState = SupportState.SUPPORTED,
          supportsRefreshes = true,
          supportsFileTransfer = false,
          supportsDataActivation = false,
        ),
        ActorDefinitionVersionRead(
          // Different tag for subsequent verification
          dockerImageTag = DOCKER_IMAGE_TAG,
          dockerRepository = DOCKER_REPOSITORY,
          isVersionOverrideApplied = true,
          supportState = SupportState.SUPPORTED,
          supportsRefreshes = true,
          supportsFileTransfer = false,
          supportsDataActivation = false,
        ),
      )

    // Test without "-rc" suffix in the input dockerImageTag
    val input =
      ConnectorRolloutActivityInputVerifyDefaultVersion(
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = DOCKER_IMAGE_TAG,
        actorDefinitionId = ACTOR_DEFINITION_ID,
        rolloutId = ROLLOUT_ID,
        previousVersionDockerImageTag = PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        // 1 second limit
        limit = 1000,
        // Poll every half second
        timeBetweenPolls = 500,
        ROLLOUT_STRATEGY,
      )

    val output = verifyDefaultVersionActivity.getAndVerifyDefaultVersion(input)
    assertEquals(true, output.isReleased)

    verify { actorDefinitionVersionApi.getActorDefinitionVersionDefault(any()) }

    // Test with "-rc" suffix in the input dockerImageTag
    val inputWithRcSuffix =
      ConnectorRolloutActivityInputVerifyDefaultVersion(
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = "$DOCKER_IMAGE_TAG-rc.1",
        actorDefinitionId = ACTOR_DEFINITION_ID,
        rolloutId = ROLLOUT_ID,
        previousVersionDockerImageTag = PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        rolloutStrategy = ROLLOUT_STRATEGY,
      )

    val outputWithRcSuffix = verifyDefaultVersionActivity.getAndVerifyDefaultVersion(inputWithRcSuffix)
    assertEquals(true, outputWithRcSuffix.isReleased)

    verify(exactly = 3) { actorDefinitionVersionApi.getActorDefinitionVersionDefault(any()) }
  }

  @Test
  fun `test verifyDefaultVersion rollout was superseded`() {
    // Mock the ActorDefinitionVersionApi to return the response dynamically
    every {
      actorDefinitionVersionApi.getActorDefinitionVersionDefault(any())
    } returns
      ActorDefinitionVersionRead(
        dockerImageTag = NEWER_VERSION_DOCKER_IMAGE_TAG,
        dockerRepository = DOCKER_REPOSITORY,
        isVersionOverrideApplied = true,
        supportState = SupportState.SUPPORTED,
        supportsRefreshes = true,
        supportsFileTransfer = false,
        supportsDataActivation = false,
      )

    val input =
      ConnectorRolloutActivityInputVerifyDefaultVersion(
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = "$DOCKER_IMAGE_TAG-rc.1",
        actorDefinitionId = ACTOR_DEFINITION_ID,
        rolloutId = ROLLOUT_ID,
        previousVersionDockerImageTag = PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        rolloutStrategy = ROLLOUT_STRATEGY,
      )

    val output = verifyDefaultVersionActivity.getAndVerifyDefaultVersion(input)
    assertEquals(false, output.isReleased)

    verify { actorDefinitionVersionApi.getActorDefinitionVersionDefault(any()) }
  }

  @Test
  fun `test verifyDefaultVersion throws exception on timeout`() {
    // Simulate the scenario where the response always returns a different tag, causing the retry to continue
    every {
      actorDefinitionVersionApi.getActorDefinitionVersionDefault(any())
    } returns
      ActorDefinitionVersionRead(
        // Same as previous version will cause a timeout
        dockerImageTag = PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        dockerRepository = DOCKER_REPOSITORY,
        isVersionOverrideApplied = true,
        supportState = SupportState.SUPPORTED,
        supportsRefreshes = true,
        supportsFileTransfer = false,
        supportsDataActivation = false,
      )

    val input =
      ConnectorRolloutActivityInputVerifyDefaultVersion(
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = DOCKER_IMAGE_TAG,
        actorDefinitionId = ACTOR_DEFINITION_ID,
        rolloutId = ROLLOUT_ID,
        previousVersionDockerImageTag = PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        // 1 second limit
        limit = 1000,
        // Poll every half second
        timeBetweenPolls = 500,
        rolloutStrategy = ROLLOUT_STRATEGY,
      )

    // Use assertThrows to verify that the exception is thrown due to timeout
    val exception =
      assertThrows<IllegalStateException> {
        verifyDefaultVersionActivity.getAndVerifyDefaultVersion(input)
      }

    verify(atLeast = 1) { actorDefinitionVersionApi.getActorDefinitionVersionDefault(any()) }
    assert(exception.message!!.contains("Timed out"))
  }
}
