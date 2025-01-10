
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutRead
import io.airbyte.api.client.model.generated.ConnectorRolloutResponse
import io.airbyte.api.client.model.generated.ConnectorRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPause
import io.airbyte.connector.rollout.worker.activities.PauseRolloutActivityImpl
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class PauseRolloutActivityImplTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var connectorRolloutApi: ConnectorRolloutApi
  private lateinit var pauseRolloutActivity: PauseRolloutActivityImpl

  companion object {
    private const val DOCKER_REPOSITORY = "airbyte/source-faker"
    private const val DOCKER_IMAGE_TAG = "0.1"
    private val ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val ROLLOUT_ID = UUID.randomUUID()
    private val USER_ID = UUID.randomUUID()
    private val ROLLOUT_STRATEGY = ConnectorEnumRolloutStrategy.MANUAL
    private val PAUSED_REASON = "paused reason"
  }

  @BeforeEach
  fun setUp() {
    airbyteApiClient = mockk<AirbyteApiClient>()
    connectorRolloutApi = mockk<ConnectorRolloutApi>()
    every { airbyteApiClient.connectorRolloutApi } returns connectorRolloutApi
    pauseRolloutActivity = PauseRolloutActivityImpl(airbyteApiClient)
  }

  @Test
  fun `test cleanup calls connectorRolloutApi`() {
    every { connectorRolloutApi.updateConnectorRolloutState(any()) } returns getMockConnectorRolloutResponse()

    val input =
      ConnectorRolloutActivityInputPause(
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = DOCKER_IMAGE_TAG,
        actorDefinitionId = ACTOR_DEFINITION_ID,
        rolloutId = ROLLOUT_ID,
        updatedBy = USER_ID,
        rolloutStrategy = ROLLOUT_STRATEGY,
        pausedReason = PAUSED_REASON,
      )

    pauseRolloutActivity.pauseRollout(input)

    verify { connectorRolloutApi.updateConnectorRolloutState(any()) }
  }

  private fun getMockConnectorRolloutResponse(): ConnectorRolloutResponse {
    return ConnectorRolloutResponse(
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
}
