package io.airbyte.workers.general

import com.amazonaws.internal.ExceptionUtils
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.DestinationDefinitionRead
import io.airbyte.api.client.model.generated.DestinationRead
import io.airbyte.api.client.model.generated.ReleaseStage
import io.airbyte.api.client.model.generated.SourceDefinitionRead
import io.airbyte.api.client.model.generated.SourceRead
import io.airbyte.commons.json.Jsons
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.State
import io.airbyte.persistence.job.WebUrlHelper
import io.airbyte.persistence.job.errorreporter.AttemptConfigReportingContext
import io.airbyte.persistence.job.errorreporter.JobErrorReporter
import io.airbyte.persistence.job.errorreporter.JobErrorReportingClient
import io.airbyte.protocol.models.AirbyteStateMessage
import io.airbyte.protocol.models.AirbyteStreamState
import io.airbyte.protocol.models.StreamDescriptor
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.Mockito.any
import org.mockito.Mockito.anyMap
import org.mockito.Mockito.anyString
import org.mockito.Mockito.eq
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.mockito.junit.jupiter.MockitoExtension
import java.net.URI
import java.time.Instant
import java.util.Optional
import java.util.UUID

@ExtendWith(MockitoExtension::class)
class StateCheckSumErrorReporterTest {
  @Mock
  private lateinit var jobErrorReportingClient: JobErrorReportingClient

  private lateinit var airbyteApiClient: AirbyteApiClient

  @Mock
  private lateinit var webUrlHelper: WebUrlHelper

  private lateinit var stateMessage: AirbyteStateMessage

  private lateinit var stateCheckSumErrorReporter: StateCheckSumErrorReporter

  private val airbyteVersion = "0.1.0"
  private val deploymentMode = "CLOUD"

  @BeforeEach
  fun setup() {
    airbyteApiClient = mockk<AirbyteApiClient>()
    stateCheckSumErrorReporter =
      StateCheckSumErrorReporter(
        Optional.of(jobErrorReportingClient),
        airbyteVersion,
        deploymentMode,
        airbyteApiClient,
        webUrlHelper,
      )
    stateMessage =
      AirbyteStateMessage().withType(AirbyteStateMessage.AirbyteStateType.STREAM)
        .withStream(
          AirbyteStreamState().withStreamState(Jsons.emptyObject()).withStreamDescriptor(
            StreamDescriptor()
              .withNamespace("namespace").withName("name"),
          ),
        )
  }

  @AfterEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test reportError with source origin`() {
    val workspaceId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()
    val jobId = 123L
    val attemptNumber = 1
    val origin = FailureReason.FailureOrigin.SOURCE
    val internalMessage = "Internal message"
    val externalMessage = "External message"
    val exception = RuntimeException("Test exception")

    val sourceId = UUID.randomUUID()
    val sourceDefinitionId = UUID.randomUUID()
    val sourceDefinition =
      SourceDefinitionRead(
        sourceDefinitionId,
        "Test Source",
        "source-repo",
        "0.1.0",
        releaseStage = ReleaseStage.BETA,
      )

    val connection = mockk<ConnectionRead>()
    every { connection.sourceId } returns sourceId

    val source = mockk<SourceRead>()
    every { source.sourceDefinitionId } returns sourceDefinitionId

    every { airbyteApiClient.connectionApi.getConnection(any()) } returns connection
    every { airbyteApiClient.sourceApi.getSource(any()) } returns source
    every { airbyteApiClient.sourceDefinitionApi.getSourceDefinition(any()) } returns sourceDefinition

    stateCheckSumErrorReporter.reportError(
      workspaceId, connectionId, jobId, attemptNumber, origin, internalMessage, externalMessage,
      exception, stateMessage,
    )

    verify(
      jobErrorReportingClient,
      times(1),
    ).reportJobFailureReason(
      any(StandardWorkspace::class.java),
      any(FailureReason::class.java),
      eq("source-repo:0.1.0"),
      anyMap(),
      eq(AttemptConfigReportingContext(null, null, State().withState(Jsons.jsonNode(stateMessage)))),
    )
  }

  @Test
  fun `test reportError with destination origin`() {
    val workspaceId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()
    val jobId = 123L
    val attemptNumber = 1
    val origin = FailureReason.FailureOrigin.DESTINATION
    val internalMessage = "Internal message"
    val externalMessage = "External message"
    val exception = RuntimeException("Test exception")

    val destinationId = UUID.randomUUID()
    val destinationDefinitionId = UUID.randomUUID()

    val destinationDefinition =
      DestinationDefinitionRead(
        destinationDefinitionId,
        "Test Destination",
        "destination-repo",
        "0.1.0",
        documentationUrl = URI(""),
        releaseStage = ReleaseStage.BETA,
      )

    val connection = mockk<ConnectionRead>()
    every { connection.destinationId } returns destinationId

    val destination = mockk<DestinationRead>()
    every { destination.destinationDefinitionId } returns destinationDefinitionId

    every { airbyteApiClient.connectionApi.getConnection(any()) } returns connection
    every { airbyteApiClient.destinationApi.getDestination(any()) } returns destination
    every { airbyteApiClient.destinationDefinitionApi.getDestinationDefinition(any()) } returns destinationDefinition

    stateCheckSumErrorReporter.reportError(
      workspaceId, connectionId, jobId, attemptNumber, origin, internalMessage, externalMessage,
      exception, stateMessage,
    )

    verify(
      jobErrorReportingClient,
      times(1),
    ).reportJobFailureReason(
      any(StandardWorkspace::class.java),
      any(FailureReason::class.java),
      eq("destination-repo:0.1.0"),
      anyMap(),
      eq(AttemptConfigReportingContext(null, null, State().withState(Jsons.jsonNode(stateMessage)))),
    )
  }

  @Test
  fun `test reportError when jobErrorReportingClient is absent`() {
    stateCheckSumErrorReporter =
      StateCheckSumErrorReporter(
        Optional.of(jobErrorReportingClient),
        airbyteVersion,
        deploymentMode,
        airbyteApiClient,
        webUrlHelper,
      )

    stateCheckSumErrorReporter.reportError(
      UUID.randomUUID(),
      UUID.randomUUID(),
      123L,
      1,
      FailureReason.FailureOrigin.SOURCE,
      "Internal message",
      "External message",
      RuntimeException("Test exception"),
      stateMessage,
    )

    verify(
      jobErrorReportingClient,
      times(0),
    ).reportJobFailureReason(any(StandardWorkspace::class.java), any(FailureReason::class.java), anyString(), anyMap(), any())
  }

  @Test
  fun `test createFailureReason`() {
    val origin = FailureReason.FailureOrigin.SOURCE
    val internalMessage = "Internal message"
    val externalMessage = "External message"
    val exception = RuntimeException("Test exception")
    val jobId = 123L
    val attemptNumber = 1

    val failureReason = stateCheckSumErrorReporter.createFailureReason(origin, internalMessage, externalMessage, exception, jobId, attemptNumber)

    assert(failureReason.failureOrigin == origin)
    assert(failureReason.internalMessage == internalMessage)
    assert(failureReason.externalMessage == externalMessage)
    assert(failureReason.timestamp <= Instant.now().toEpochMilli())
    assert(failureReason.stacktrace == ExceptionUtils.exceptionStackTrace(exception))
  }

  @Test
  fun `test getDockerImageName`() {
    val dockerRepository = "test-repo"
    val dockerImageTag = "0.1.0"
    val dockerImageName = stateCheckSumErrorReporter.getDockerImageName(dockerRepository, dockerImageTag)

    assert(dockerImageName == "$dockerRepository:$dockerImageTag")
  }

  @Test
  fun `test airbyteMetadata`() {
    val metadata = stateCheckSumErrorReporter.airbyteMetadata()

    assert(metadata[JobErrorReporter.AIRBYTE_VERSION_META_KEY] == airbyteVersion)
    assert(metadata[JobErrorReporter.DEPLOYMENT_MODE_META_KEY] == deploymentMode)
  }

  @Test
  fun `test getFailureReasonMetadata`() {
    val failureReason =
      FailureReason().apply {
        failureOrigin = FailureReason.FailureOrigin.SOURCE
        failureType = FailureReason.FailureType.SYSTEM_ERROR
      }
    val metadata = stateCheckSumErrorReporter.getFailureReasonMetadata(failureReason)

    assert(metadata[JobErrorReporter.FAILURE_ORIGIN_META_KEY] == "source")
    assert(metadata[JobErrorReporter.FAILURE_TYPE_META_KEY] == "system_error")
  }

  @Test
  fun `test getDefinitionMetadata`() {
    val definitionId = UUID.randomUUID()
    val name = "Test Connector"
    val dockerImage = "test-repo:0.1.0"
    val releaseStage = ReleaseStage.BETA
    val metadata = stateCheckSumErrorReporter.getDefinitionMetadata(definitionId, name, dockerImage, releaseStage)

    assert(metadata[JobErrorReporter.CONNECTOR_DEFINITION_ID_META_KEY] == definitionId.toString())
    assert(metadata[JobErrorReporter.CONNECTOR_NAME_META_KEY] == name)
    assert(metadata[JobErrorReporter.CONNECTOR_REPOSITORY_META_KEY] == "test-repo")
    assert(metadata[JobErrorReporter.CONNECTOR_RELEASE_STAGE_META_KEY] == "beta")
  }

  @Test
  fun `test getConnectionMetadata`() {
    val workspaceId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()
    val connectionUrl = "https://test.url/connection"
    `when`(webUrlHelper.getConnectionUrl(workspaceId, connectionId)).thenReturn(connectionUrl)

    val metadata = stateCheckSumErrorReporter.getConnectionMetadata(workspaceId, connectionId)

    assert(metadata[JobErrorReporter.CONNECTION_ID_META_KEY] == connectionId.toString())
    assert(metadata[JobErrorReporter.CONNECTION_URL_META_KEY] == connectionUrl)
  }

  @Test
  fun `test getWorkspaceMetadata`() {
    val workspaceId = UUID.randomUUID()
    val workspaceUrl = "https://test.url/workspace"
    `when`(webUrlHelper.getWorkspaceUrl(workspaceId)).thenReturn(workspaceUrl)

    val metadata = stateCheckSumErrorReporter.getWorkspaceMetadata(workspaceId)

    assert(metadata[JobErrorReporter.WORKSPACE_ID_META_KEY] == workspaceId.toString())
    assert(metadata[JobErrorReporter.WORKSPACE_URL_META_KEY] == workspaceUrl)
  }
}
