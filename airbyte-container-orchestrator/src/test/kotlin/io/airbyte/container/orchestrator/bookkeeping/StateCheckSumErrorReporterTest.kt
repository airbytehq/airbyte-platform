/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping

import com.amazonaws.internal.ExceptionUtils
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.WebUrlHelper
import io.airbyte.api.client.model.generated.ActorDefinitionVersionRead
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.DestinationDefinitionRead
import io.airbyte.api.client.model.generated.DestinationRead
import io.airbyte.api.client.model.generated.ReleaseStage
import io.airbyte.api.client.model.generated.SourceDefinitionRead
import io.airbyte.api.client.model.generated.SourceRead
import io.airbyte.api.client.model.generated.SupportState
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Configs
import io.airbyte.config.FailureReason
import io.airbyte.config.State
import io.airbyte.persistence.job.errorreporter.AttemptConfigReportingContext
import io.airbyte.persistence.job.errorreporter.JobErrorReporter
import io.airbyte.persistence.job.errorreporter.JobErrorReportingClient
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.net.URI
import java.time.Instant
import java.util.Optional
import java.util.UUID

class StateCheckSumErrorReporterTest {
  private lateinit var jobErrorReportingClient: JobErrorReportingClient

  private lateinit var airbyteApiClient: AirbyteApiClient

  private lateinit var webUrlHelper: WebUrlHelper

  private lateinit var stateMessage: AirbyteStateMessage

  private lateinit var stateCheckSumErrorReporter: StateCheckSumErrorReporter

  private val airbyteVersion = "0.1.0"
  private val airbyteEdition = Configs.AirbyteEdition.CLOUD

  @BeforeEach
  fun setup() {
    jobErrorReportingClient = mockk(relaxed = true)
    webUrlHelper = mockk(relaxed = true)

    airbyteApiClient = mockk<AirbyteApiClient>()
    stateCheckSumErrorReporter =
      StateCheckSumErrorReporter(
        Optional.of(jobErrorReportingClient),
        airbyteVersion,
        airbyteEdition,
        airbyteApiClient,
        webUrlHelper,
      )
    stateMessage =
      AirbyteStateMessage()
        .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
        .withStream(
          AirbyteStreamState().withStreamState(Jsons.emptyObject()).withStreamDescriptor(
            StreamDescriptor()
              .withNamespace("namespace")
              .withName("name"),
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
    val sourceVersion =
      ActorDefinitionVersionRead(
        "source-repo",
        "0.0.1",
        supportsRefreshes = true,
        isVersionOverrideApplied = true,
        SupportState.SUPPORTED,
        supportsFileTransfer = false,
        supportsDataActivation = false,
      )
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
    every { airbyteApiClient.actorDefinitionVersionApi.getActorDefinitionVersionForSourceId(any()) } returns sourceVersion
    every { airbyteApiClient.sourceDefinitionApi.getSourceDefinition(any()) } returns sourceDefinition

    stateCheckSumErrorReporter.reportError(
      workspaceId,
      connectionId,
      jobId,
      attemptNumber,
      origin,
      internalMessage,
      externalMessage,
      exception,
      stateMessage,
    )

    verify(exactly = 1) {
      jobErrorReportingClient.reportJobFailureReason(
        any(),
        any(),
        "source-repo:0.0.1",
        any(),
        AttemptConfigReportingContext(null, null, State().withState(Jsons.jsonNode(stateMessage))),
      )
    }
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
    val destinationVersion =
      ActorDefinitionVersionRead(
        "destination-repo",
        "0.0.1",
        supportsRefreshes = true,
        isVersionOverrideApplied = true,
        SupportState.SUPPORTED,
        supportsFileTransfer = false,
        supportsDataActivation = false,
      )

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
    every { airbyteApiClient.actorDefinitionVersionApi.getActorDefinitionVersionForDestinationId(any()) } returns destinationVersion
    every { airbyteApiClient.destinationDefinitionApi.getDestinationDefinition(any()) } returns destinationDefinition

    stateCheckSumErrorReporter.reportError(
      workspaceId,
      connectionId,
      jobId,
      attemptNumber,
      origin,
      internalMessage,
      externalMessage,
      exception,
      stateMessage,
    )

    verify(exactly = 1) {
      jobErrorReportingClient.reportJobFailureReason(
        any(),
        any(),
        "destination-repo:0.0.1",
        any(),
        AttemptConfigReportingContext(null, null, State().withState(Jsons.jsonNode(stateMessage))),
      )
    }
  }

  @Test
  fun `test reportError when jobErrorReportingClient is absent`() {
    stateCheckSumErrorReporter =
      StateCheckSumErrorReporter(
        Optional.of(jobErrorReportingClient),
        airbyteVersion,
        airbyteEdition,
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

    verify(exactly = 0) {
      jobErrorReportingClient.reportJobFailureReason(
        any(),
        any(),
        any(),
        any(),
        any(),
      )
    }
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
    assert(metadata[JobErrorReporter.AIRBYTE_EDITION_META_KEY] == airbyteEdition.name)
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
    every { webUrlHelper.getConnectionUrl(workspaceId, connectionId) } returns connectionUrl

    val metadata = stateCheckSumErrorReporter.getConnectionMetadata(workspaceId, connectionId)

    assert(metadata[JobErrorReporter.CONNECTION_ID_META_KEY] == connectionId.toString())
    assert(metadata[JobErrorReporter.CONNECTION_URL_META_KEY] == connectionUrl)
  }

  @Test
  fun `test getWorkspaceMetadata`() {
    val workspaceId = UUID.randomUUID()
    val workspaceUrl = "https://test.url/workspace"
    every { webUrlHelper.getWorkspaceUrl(workspaceId) } returns workspaceUrl

    val metadata = stateCheckSumErrorReporter.getWorkspaceMetadata(workspaceId)

    assert(metadata[JobErrorReporter.WORKSPACE_ID_META_KEY] == workspaceId.toString())
    assert(metadata[JobErrorReporter.WORKSPACE_URL_META_KEY] == workspaceUrl)
  }
}
