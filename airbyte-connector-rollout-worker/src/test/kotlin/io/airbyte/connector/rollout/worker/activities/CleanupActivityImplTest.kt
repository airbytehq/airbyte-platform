
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectorRolloutApi
import io.airbyte.api.client.model.generated.ConnectorRolloutRead
import io.airbyte.api.client.model.generated.ConnectorRolloutResponse
import io.airbyte.api.client.model.generated.ConnectorRolloutState
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputCleanup
import io.airbyte.connector.rollout.worker.activities.CleanupActivityImpl
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class CleanupActivityImplTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var connectorRolloutApi: ConnectorRolloutApi
  private lateinit var cleanupActivity: CleanupActivityImpl

  companion object {
    private const val DOCKER_REPOSITORY = "airbyte/source-faker"
    private const val DOCKER_IMAGE_TAG = "0.1"
    private val ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val ROLLOUT_ID = UUID.randomUUID()
  }

  @BeforeEach
  fun setUp() {
    airbyteApiClient = mockk<AirbyteApiClient>()
    connectorRolloutApi = mockk<ConnectorRolloutApi>()
    every { airbyteApiClient.connectorRolloutApi } returns connectorRolloutApi
    cleanupActivity = CleanupActivityImpl(airbyteApiClient)
  }

  @Test
  fun `test cleanup calls connectorRolloutApi`() {
    every { connectorRolloutApi.updateConnectorRolloutState(any()) } returns getMockConnectorRolloutResponse()

    val input =
      ConnectorRolloutActivityInputCleanup(
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = DOCKER_IMAGE_TAG,
        actorDefinitionId = ACTOR_DEFINITION_ID,
        rolloutId = ROLLOUT_ID,
        newState = ConnectorEnumRolloutState.SUCCEEDED,
      )

    cleanupActivity.cleanup(input)

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
