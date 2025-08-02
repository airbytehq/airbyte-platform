/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.client.WebUrlHelper
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.FailureReason
import io.airbyte.config.Metadata
import io.airbyte.config.ReleaseStage
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.State
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.metrics.MetricClient
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.kotlin.any
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever
import java.io.IOException
import java.util.List
import java.util.Map
import java.util.UUID

internal class JobErrorReporterTest {
  private var actorDefinitionService: ActorDefinitionService? = null
  private var sourceService: SourceService? = null
  private var destinationService: DestinationService? = null
  private var workspaceService: WorkspaceService? = null
  private lateinit var jobErrorReportingClient: JobErrorReportingClient
  private var webUrlHelper: WebUrlHelper? = null
  private var jobErrorReporter: JobErrorReporter? = null

  @BeforeEach
  fun setup() {
    actorDefinitionService = mock<ActorDefinitionService>()
    sourceService = mock<SourceService>()
    destinationService = mock<DestinationService>()
    workspaceService = mock<WorkspaceService>()
    jobErrorReportingClient = mock<JobErrorReportingClient>()
    webUrlHelper = mock<WebUrlHelper>()
    jobErrorReporter =
      JobErrorReporter(
        actorDefinitionService!!,
        sourceService!!,
        destinationService!!,
        workspaceService!!,
        AIRBYTE_EDITION,
        AIRBYTE_VERSION,
        webUrlHelper!!,
        jobErrorReportingClient!!,
        mock<MetricClient>(),
      )

    whenever(webUrlHelper!!.getConnectionUrl(WORKSPACE_ID, CONNECTION_ID)).thenReturn(CONNECTION_URL)
    whenever(webUrlHelper!!.getWorkspaceUrl(WORKSPACE_ID)).thenReturn(WORKSPACE_URL)
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testReportSyncJobFailure() {
    val mFailureSummary = mock<AttemptFailureSummary>()

    val sourceFailureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, READ_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val destinationFailureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, WRITE_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.DESTINATION)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val nonTraceMessageFailureReason = FailureReason().withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
    val replicationFailureReason = FailureReason().withFailureOrigin(FailureReason.FailureOrigin.REPLICATION)

    whenever(mFailureSummary.getFailures()).thenReturn(
      List.of<FailureReason?>(
        sourceFailureReason,
        destinationFailureReason,
        nonTraceMessageFailureReason,
        replicationFailureReason,
      ),
    )

    val syncJobId = 1L
    val jobReportingContext =
      SyncJobReportingContext(
        syncJobId,
        SOURCE_DEFINITION_VERSION_ID,
        DESTINATION_DEFINITION_VERSION_ID,
      )

    val objectMapper = ObjectMapper()
    val attemptConfig =
      AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), State())

    whenever(sourceService!!.getSourceDefinitionFromConnection(CONNECTION_ID))
      .thenReturn(
        StandardSourceDefinition()
          .withSourceDefinitionId(SOURCE_DEFINITION_ID)
          .withName(SOURCE_DEFINITION_NAME),
      )

    whenever(destinationService!!.getDestinationDefinitionFromConnection(CONNECTION_ID))
      .thenReturn(
        StandardDestinationDefinition()
          .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
          .withName(DESTINATION_DEFINITION_NAME),
      )

    whenever(actorDefinitionService!!.getActorDefinitionVersion(SOURCE_DEFINITION_VERSION_ID))
      .thenReturn(
        ActorDefinitionVersion()
          .withDockerRepository(SOURCE_DOCKER_REPOSITORY)
          .withDockerImageTag(DOCKER_IMAGE_TAG)
          .withReleaseStage(SOURCE_RELEASE_STAGE)
          .withInternalSupportLevel(SOURCE_INTERNAL_SUPPORT_LEVEL),
      )

    whenever(actorDefinitionService!!.getActorDefinitionVersion(DESTINATION_DEFINITION_VERSION_ID))
      .thenReturn(
        ActorDefinitionVersion()
          .withDockerRepository(DESTINATION_DOCKER_REPOSITORY)
          .withDockerImageTag(DOCKER_IMAGE_TAG)
          .withReleaseStage(DESTINATION_RELEASE_STAGE)
          .withInternalSupportLevel(DESTINATION_INTERNAL_SUPPORT_LEVEL),
      )

    val mWorkspace = mock<StandardWorkspace>()
    whenever(mWorkspace.getWorkspaceId()).thenReturn(WORKSPACE_ID)
    whenever(workspaceService!!.getStandardWorkspaceFromConnection(CONNECTION_ID, true)).thenReturn(mWorkspace)

    jobErrorReporter!!.reportSyncJobFailure(CONNECTION_ID, mFailureSummary, jobReportingContext, attemptConfig)

    val expectedSourceMetadata =
      Map.ofEntries<String?, String?>(
        Map.entry<String?, String?>(JOB_ID_KEY, syncJobId.toString()),
        Map.entry<String?, String?>(WORKSPACE_ID_KEY, WORKSPACE_ID.toString()),
        Map.entry<String?, String?>(WORKSPACE_URL_KEY, WORKSPACE_URL),
        Map.entry<String?, String?>(CONNECTION_ID_KEY, CONNECTION_ID.toString()),
        Map.entry<String?, String?>(CONNECTION_URL_KEY, CONNECTION_URL),
        Map.entry<String?, String?>(AIRBYTE_EDITION_KEY, AIRBYTE_EDITION.name),
        Map.entry<String?, String?>(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry<String?, String?>(FAILURE_ORIGIN_KEY, SOURCE),
        Map.entry<String?, String?>(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry<String?, String?>(CONNECTOR_COMMAND_KEY, READ_COMMAND),
        Map.entry<String?, String?>(CONNECTOR_DEFINITION_ID_KEY, SOURCE_DEFINITION_ID.toString()),
        Map.entry<String?, String?>(CONNECTOR_REPOSITORY_KEY, SOURCE_DOCKER_REPOSITORY),
        Map.entry<String?, String?>(CONNECTOR_NAME_KEY, SOURCE_DEFINITION_NAME),
        Map.entry<String?, String?>(CONNECTOR_RELEASE_STAGE_KEY, SOURCE_RELEASE_STAGE.toString()),
        Map.entry<String?, String?>(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, SOURCE_INTERNAL_SUPPORT_LEVEL.toString()),
      )

    val expectedDestinationMetadata =
      Map.ofEntries<String?, String?>(
        Map.entry<String?, String?>(JOB_ID_KEY, syncJobId.toString()),
        Map.entry<String?, String?>(WORKSPACE_ID_KEY, WORKSPACE_ID.toString()),
        Map.entry<String?, String?>(WORKSPACE_URL_KEY, WORKSPACE_URL),
        Map.entry<String?, String?>(CONNECTION_ID_KEY, CONNECTION_ID.toString()),
        Map.entry<String?, String?>(CONNECTION_URL_KEY, CONNECTION_URL),
        Map.entry<String?, String?>(AIRBYTE_EDITION_KEY, AIRBYTE_EDITION.name),
        Map.entry<String?, String?>(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry<String?, String?>(FAILURE_ORIGIN_KEY, "destination"),
        Map.entry<String?, String?>(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry<String?, String?>(CONNECTOR_COMMAND_KEY, WRITE_COMMAND),
        Map.entry<String?, String?>(CONNECTOR_DEFINITION_ID_KEY, DESTINATION_DEFINITION_ID.toString()),
        Map.entry<String?, String?>(CONNECTOR_REPOSITORY_KEY, DESTINATION_DOCKER_REPOSITORY),
        Map.entry<String?, String?>(CONNECTOR_NAME_KEY, DESTINATION_DEFINITION_NAME),
        Map.entry<String?, String?>(CONNECTOR_RELEASE_STAGE_KEY, DESTINATION_RELEASE_STAGE.toString()),
        Map.entry<String?, String?>(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, DESTINATION_INTERNAL_SUPPORT_LEVEL.toString()),
      )

    verify(jobErrorReportingClient).reportJobFailureReason(
      mWorkspace,
      sourceFailureReason,
      SOURCE_DOCKER_IMAGE,
      expectedSourceMetadata,
      attemptConfig,
    )
    verify(jobErrorReportingClient).reportJobFailureReason(
      mWorkspace,
      destinationFailureReason,
      DESTINATION_DOCKER_IMAGE,
      expectedDestinationMetadata,
      attemptConfig,
    )
    verifyNoMoreInteractions(jobErrorReportingClient)
  }

  @Test
  @Throws(ConfigNotFoundException::class, IOException::class, ConfigNotFoundException::class)
  fun testReportSyncJobFailureDoesNotThrow() {
    val mFailureSummary = mock<AttemptFailureSummary>()
    val jobContext = SyncJobReportingContext(1L, SOURCE_DEFINITION_VERSION_ID, DESTINATION_DEFINITION_VERSION_ID)

    val sourceFailureReason =
      FailureReason()
        .withMetadata(Metadata().withAdditionalProperty(FROM_TRACE_MESSAGE, true))
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val objectMapper = ObjectMapper()
    val attemptConfig =
      AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), State())

    whenever(mFailureSummary.getFailures()).thenReturn(List.of<FailureReason?>(sourceFailureReason))

    whenever(sourceService!!.getSourceDefinitionFromConnection(CONNECTION_ID))
      .thenReturn(
        StandardSourceDefinition()
          .withSourceDefinitionId(SOURCE_DEFINITION_ID)
          .withName(SOURCE_DEFINITION_NAME),
      )

    whenever(actorDefinitionService!!.getActorDefinitionVersion(SOURCE_DEFINITION_VERSION_ID))
      .thenReturn(
        ActorDefinitionVersion()
          .withDockerRepository(SOURCE_DOCKER_REPOSITORY)
          .withDockerImageTag(DOCKER_IMAGE_TAG)
          .withReleaseStage(SOURCE_RELEASE_STAGE),
      )

    val mWorkspace = mock<StandardWorkspace>()
    whenever(mWorkspace.getWorkspaceId()).thenReturn(WORKSPACE_ID)
    whenever(workspaceService!!.getStandardWorkspaceFromConnection(CONNECTION_ID, true)).thenReturn(mWorkspace)
    whenever(webUrlHelper!!.getConnectionUrl(WORKSPACE_ID, CONNECTION_ID)).thenReturn(CONNECTION_URL)

    doThrow(RuntimeException("some exception"))
      .whenever(jobErrorReportingClient)
      .reportJobFailureReason(
        any<StandardWorkspace>(),
        eq(sourceFailureReason),
        any<String>(),
        any<kotlin.collections.Map<String?, String?>>(),
        any<AttemptConfigReportingContext>(),
      )

    Assertions.assertDoesNotThrow(
      Executable {
        jobErrorReporter!!.reportSyncJobFailure(
          CONNECTION_ID,
          mFailureSummary,
          jobContext,
          attemptConfig,
        )
      },
    )
    verify(jobErrorReportingClient, times(1))
      .reportJobFailureReason(
        any<StandardWorkspace>(),
        any<FailureReason>(),
        any<String>(),
        any<kotlin.collections.Map<String?, String?>>(),
        any<AttemptConfigReportingContext>(),
      )
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testReportSourceCheckJobFailureNullWorkspaceId() {
    val failureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val jobContext =
      ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL)

    whenever(sourceService!!.getStandardSourceDefinition(SOURCE_DEFINITION_ID))
      .thenReturn(
        StandardSourceDefinition()
          .withSourceDefinitionId(SOURCE_DEFINITION_ID)
          .withName(SOURCE_DEFINITION_NAME),
      )

    jobErrorReporter!!.reportSourceCheckJobFailure(SOURCE_DEFINITION_ID, null, failureReason, jobContext)

    val expectedMetadata =
      Map.ofEntries<String?, String?>(
        Map.entry<String?, String?>(JOB_ID_KEY, JOB_ID.toString()),
        Map.entry<String?, String?>(AIRBYTE_EDITION_KEY, AIRBYTE_EDITION.name),
        Map.entry<String?, String?>(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry<String?, String?>(FAILURE_ORIGIN_KEY, SOURCE),
        Map.entry<String?, String?>(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry<String?, String?>(CONNECTOR_DEFINITION_ID_KEY, SOURCE_DEFINITION_ID.toString()),
        Map.entry<String?, String?>(CONNECTOR_REPOSITORY_KEY, SOURCE_DOCKER_REPOSITORY),
        Map.entry<String?, String?>(CONNECTOR_NAME_KEY, SOURCE_DEFINITION_NAME),
        Map.entry<String?, String?>(CONNECTOR_RELEASE_STAGE_KEY, SOURCE_RELEASE_STAGE.toString()),
        Map.entry<String?, String?>(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, SOURCE_INTERNAL_SUPPORT_LEVEL.toString()),
        Map.entry<String?, String?>(CONNECTOR_COMMAND_KEY, CHECK_COMMAND),
      )

    verify(jobErrorReportingClient)
      .reportJobFailureReason(null, failureReason, SOURCE_DOCKER_IMAGE, expectedMetadata, null)
    verifyNoMoreInteractions(jobErrorReportingClient)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testDoNotReportSourceCheckJobFailureFromOtherOrigins() {
    val failureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val jobContext =
      ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL)

    jobErrorReporter!!.reportSourceCheckJobFailure(SOURCE_DEFINITION_ID, WORKSPACE_ID, failureReason, jobContext)

    verifyNoInteractions(jobErrorReportingClient)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testReportDestinationCheckJobFailure() {
    val failureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.DESTINATION)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val jobContext =
      ConnectorJobReportingContext(JOB_ID, DESTINATION_DOCKER_IMAGE, DESTINATION_RELEASE_STAGE, DESTINATION_INTERNAL_SUPPORT_LEVEL)

    whenever(destinationService!!.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID))
      .thenReturn(
        StandardDestinationDefinition()
          .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
          .withName(DESTINATION_DEFINITION_NAME),
      )

    val mWorkspace = mock<StandardWorkspace>()
    whenever(mWorkspace.getWorkspaceId()).thenReturn(WORKSPACE_ID)
    whenever(workspaceService!!.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true)).thenReturn(mWorkspace)

    jobErrorReporter!!.reportDestinationCheckJobFailure(DESTINATION_DEFINITION_ID, WORKSPACE_ID, failureReason, jobContext)

    val expectedMetadata =
      Map.ofEntries<String?, String?>(
        Map.entry<String?, String?>(JOB_ID_KEY, JOB_ID.toString()),
        Map.entry<String?, String?>(WORKSPACE_ID_KEY, WORKSPACE_ID.toString()),
        Map.entry<String?, String?>(WORKSPACE_URL_KEY, WORKSPACE_URL),
        Map.entry<String?, String?>(AIRBYTE_EDITION_KEY, AIRBYTE_EDITION.name),
        Map.entry<String?, String?>(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry<String?, String?>(FAILURE_ORIGIN_KEY, "destination"),
        Map.entry<String?, String?>(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry<String?, String?>(CONNECTOR_DEFINITION_ID_KEY, DESTINATION_DEFINITION_ID.toString()),
        Map.entry<String?, String?>(CONNECTOR_REPOSITORY_KEY, DESTINATION_DOCKER_REPOSITORY),
        Map.entry<String?, String?>(CONNECTOR_NAME_KEY, DESTINATION_DEFINITION_NAME),
        Map.entry<String?, String?>(CONNECTOR_RELEASE_STAGE_KEY, DESTINATION_RELEASE_STAGE.toString()),
        Map.entry<String?, String?>(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, DESTINATION_INTERNAL_SUPPORT_LEVEL.toString()),
        Map.entry<String?, String?>(CONNECTOR_COMMAND_KEY, CHECK_COMMAND),
      )

    verify(jobErrorReportingClient)
      .reportJobFailureReason(mWorkspace, failureReason, DESTINATION_DOCKER_IMAGE, expectedMetadata, null)
    verifyNoMoreInteractions(jobErrorReportingClient)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testDoNotReportDestinationCheckJobFailureFromOtherOrigins() {
    val failureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val jobContext =
      ConnectorJobReportingContext(JOB_ID, DESTINATION_DOCKER_IMAGE, DESTINATION_RELEASE_STAGE, DESTINATION_INTERNAL_SUPPORT_LEVEL)

    jobErrorReporter!!.reportDestinationCheckJobFailure(DESTINATION_DEFINITION_ID, WORKSPACE_ID, failureReason, jobContext)

    verifyNoInteractions(jobErrorReportingClient)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testReportDestinationCheckJobFailureNullWorkspaceId() {
    val failureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.DESTINATION)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val jobContext =
      ConnectorJobReportingContext(JOB_ID, DESTINATION_DOCKER_IMAGE, DESTINATION_RELEASE_STAGE, DESTINATION_INTERNAL_SUPPORT_LEVEL)

    whenever(destinationService!!.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID))
      .thenReturn(
        StandardDestinationDefinition()
          .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
          .withName(DESTINATION_DEFINITION_NAME),
      )

    jobErrorReporter!!.reportDestinationCheckJobFailure(DESTINATION_DEFINITION_ID, null, failureReason, jobContext)

    val expectedMetadata =
      Map.ofEntries<String?, String?>(
        Map.entry<String?, String?>(JOB_ID_KEY, JOB_ID.toString()),
        Map.entry<String?, String?>(AIRBYTE_EDITION_KEY, AIRBYTE_EDITION.name),
        Map.entry<String?, String?>(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry<String?, String?>(FAILURE_ORIGIN_KEY, "destination"),
        Map.entry<String?, String?>(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry<String?, String?>(CONNECTOR_DEFINITION_ID_KEY, DESTINATION_DEFINITION_ID.toString()),
        Map.entry<String?, String?>(CONNECTOR_REPOSITORY_KEY, DESTINATION_DOCKER_REPOSITORY),
        Map.entry<String?, String?>(CONNECTOR_NAME_KEY, DESTINATION_DEFINITION_NAME),
        Map.entry<String?, String?>(CONNECTOR_RELEASE_STAGE_KEY, DESTINATION_RELEASE_STAGE.toString()),
        Map.entry<String?, String?>(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, DESTINATION_INTERNAL_SUPPORT_LEVEL.toString()),
        Map.entry<String?, String?>(CONNECTOR_COMMAND_KEY, CHECK_COMMAND),
      )

    verify(jobErrorReportingClient)
      .reportJobFailureReason(null, failureReason, DESTINATION_DOCKER_IMAGE, expectedMetadata, null)
    verifyNoMoreInteractions(jobErrorReportingClient)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testReportDiscoverJobFailure() {
    val failureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, DISCOVER_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val jobContext =
      ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL)

    whenever(sourceService!!.getStandardSourceDefinition(SOURCE_DEFINITION_ID))
      .thenReturn(
        StandardSourceDefinition()
          .withSourceDefinitionId(SOURCE_DEFINITION_ID)
          .withName(SOURCE_DEFINITION_NAME),
      )

    val mWorkspace = mock<StandardWorkspace>()
    whenever(mWorkspace.getWorkspaceId()).thenReturn(WORKSPACE_ID)
    whenever(workspaceService!!.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true)).thenReturn(mWorkspace)

    jobErrorReporter!!.reportDiscoverJobFailure(SOURCE_DEFINITION_ID, ActorType.SOURCE, WORKSPACE_ID, failureReason, jobContext)

    val expectedMetadata =
      Map.ofEntries<String?, String?>(
        Map.entry<String?, String?>(JOB_ID_KEY, JOB_ID.toString()),
        Map.entry<String?, String?>(WORKSPACE_ID_KEY, WORKSPACE_ID.toString()),
        Map.entry<String?, String?>(WORKSPACE_URL_KEY, WORKSPACE_URL),
        Map.entry<String?, String?>(AIRBYTE_EDITION_KEY, AIRBYTE_EDITION.name),
        Map.entry<String?, String?>(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry<String?, String?>(FAILURE_ORIGIN_KEY, SOURCE),
        Map.entry<String?, String?>(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry<String?, String?>(CONNECTOR_DEFINITION_ID_KEY, SOURCE_DEFINITION_ID.toString()),
        Map.entry<String?, String?>(CONNECTOR_REPOSITORY_KEY, SOURCE_DOCKER_REPOSITORY),
        Map.entry<String?, String?>(CONNECTOR_NAME_KEY, SOURCE_DEFINITION_NAME),
        Map.entry<String?, String?>(CONNECTOR_RELEASE_STAGE_KEY, SOURCE_RELEASE_STAGE.toString()),
        Map.entry<String?, String?>(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, SOURCE_INTERNAL_SUPPORT_LEVEL.toString()),
        Map.entry<String?, String?>(CONNECTOR_COMMAND_KEY, DISCOVER_COMMAND),
      )

    verify(jobErrorReportingClient)
      .reportJobFailureReason(mWorkspace, failureReason, SOURCE_DOCKER_IMAGE, expectedMetadata, null)
    verifyNoMoreInteractions(jobErrorReportingClient)
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun testReportDiscoverJobFailureNullWorkspaceId() {
    val failureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, DISCOVER_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val jobContext =
      ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL)

    whenever(sourceService!!.getStandardSourceDefinition(SOURCE_DEFINITION_ID))
      .thenReturn(
        StandardSourceDefinition()
          .withSourceDefinitionId(SOURCE_DEFINITION_ID)
          .withName(SOURCE_DEFINITION_NAME),
      )

    jobErrorReporter!!.reportDiscoverJobFailure(SOURCE_DEFINITION_ID, ActorType.SOURCE, null, failureReason, jobContext)

    val expectedMetadata =
      Map.ofEntries<String?, String?>(
        Map.entry<String?, String?>(JOB_ID_KEY, JOB_ID.toString()),
        Map.entry<String?, String?>(AIRBYTE_EDITION_KEY, AIRBYTE_EDITION.name),
        Map.entry<String?, String?>(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry<String?, String?>(FAILURE_ORIGIN_KEY, SOURCE),
        Map.entry<String?, String?>(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry<String?, String?>(CONNECTOR_DEFINITION_ID_KEY, SOURCE_DEFINITION_ID.toString()),
        Map.entry<String?, String?>(CONNECTOR_REPOSITORY_KEY, SOURCE_DOCKER_REPOSITORY),
        Map.entry<String?, String?>(CONNECTOR_NAME_KEY, SOURCE_DEFINITION_NAME),
        Map.entry<String?, String?>(CONNECTOR_RELEASE_STAGE_KEY, SOURCE_RELEASE_STAGE.toString()),
        Map.entry<String?, String?>(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, SOURCE_INTERNAL_SUPPORT_LEVEL.toString()),
        Map.entry<String?, String?>(CONNECTOR_COMMAND_KEY, DISCOVER_COMMAND),
      )

    verify(jobErrorReportingClient)
      .reportJobFailureReason(null, failureReason, SOURCE_DOCKER_IMAGE, expectedMetadata, null)
    verifyNoMoreInteractions(jobErrorReportingClient)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testDoNotReportDiscoverJobFailureFromOtherOrigins() {
    val failureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val jobContext =
      ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL)

    jobErrorReporter!!.reportDiscoverJobFailure(SOURCE_DEFINITION_ID, ActorType.SOURCE, WORKSPACE_ID, failureReason, jobContext)

    verifyNoInteractions(jobErrorReportingClient)
  }

  @Test
  fun testReportSpecJobFailure() {
    val failureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, SPEC_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val jobContext = ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, null, null)

    jobErrorReporter!!.reportSpecJobFailure(failureReason, jobContext)

    val expectedMetadata =
      Map.ofEntries<String?, String?>(
        Map.entry<String?, String?>(JOB_ID_KEY, JOB_ID.toString()),
        Map.entry<String?, String?>(AIRBYTE_EDITION_KEY, AIRBYTE_EDITION.name),
        Map.entry<String?, String?>(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry<String?, String?>(FAILURE_ORIGIN_KEY, SOURCE),
        Map.entry<String?, String?>(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry<String?, String?>(CONNECTOR_REPOSITORY_KEY, SOURCE_DOCKER_REPOSITORY),
        Map.entry<String?, String?>(CONNECTOR_COMMAND_KEY, SPEC_COMMAND),
      )

    verify(jobErrorReportingClient)
      .reportJobFailureReason(null, failureReason, SOURCE_DOCKER_IMAGE, expectedMetadata, null)
    verifyNoMoreInteractions(jobErrorReportingClient)
  }

  @Test
  fun testDoNotReportSpecJobFailureFromOtherOrigins() {
    val failureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val jobContext =
      ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL)

    jobErrorReporter!!.reportSpecJobFailure(failureReason, jobContext)

    verifyNoInteractions(jobErrorReportingClient)
  }

  @Test
  fun testReportUnsupportedFailureType() {
    val readFailureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, READ_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.CONFIG_ERROR)

    val discoverFailureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, DISCOVER_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.MANUAL_CANCELLATION)

    val checkFailureReason =
      FailureReason()
        .withMetadata(
          Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND),
        ).withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.CONFIG_ERROR)

    val jobContext =
      ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL)

    jobErrorReporter!!.reportSpecJobFailure(readFailureReason, jobContext)
    jobErrorReporter!!.reportSpecJobFailure(discoverFailureReason, jobContext)
    jobErrorReporter!!.reportSpecJobFailure(checkFailureReason, jobContext)

    verifyNoInteractions(jobErrorReportingClient)
  }

  companion object {
    private val JOB_ID: UUID = UUID.randomUUID()
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private const val CONNECTION_URL = "http://localhost:8000/connection/my_connection"
    private const val WORKSPACE_URL = "http://localhost:8000/workspace/my_workspace"
    private val AIRBYTE_EDITION = AirbyteEdition.CLOUD
    private const val AIRBYTE_VERSION = "0.1.40"
    private const val DOCKER_IMAGE_TAG = "1.2.3"

    private val SOURCE_DEFINITION_ID: UUID = UUID.randomUUID()
    private val SOURCE_DEFINITION_VERSION_ID: UUID = UUID.randomUUID()
    private const val SOURCE_DEFINITION_NAME = "stripe"
    private const val SOURCE_DOCKER_REPOSITORY = "airbyte/source-stripe"
    private val SOURCE_DOCKER_IMAGE: String = SOURCE_DOCKER_REPOSITORY + ":" + DOCKER_IMAGE_TAG
    private val SOURCE_RELEASE_STAGE = ReleaseStage.BETA
    private const val SOURCE_INTERNAL_SUPPORT_LEVEL = 200L
    private val DESTINATION_DEFINITION_ID: UUID = UUID.randomUUID()
    private val DESTINATION_DEFINITION_VERSION_ID: UUID = UUID.randomUUID()
    private const val DESTINATION_DEFINITION_NAME = "snowflake"
    private const val DESTINATION_DOCKER_REPOSITORY = "airbyte/destination-snowflake"
    private val DESTINATION_DOCKER_IMAGE: String = DESTINATION_DOCKER_REPOSITORY + ":" + DOCKER_IMAGE_TAG
    private val DESTINATION_RELEASE_STAGE = ReleaseStage.BETA
    private const val DESTINATION_INTERNAL_SUPPORT_LEVEL = 100L
    private const val FROM_TRACE_MESSAGE = "from_trace_message"
    private const val JOB_ID_KEY = "job_id"
    private const val WORKSPACE_ID_KEY = "workspace_id"
    private const val WORKSPACE_URL_KEY = "workspace_url"
    private const val CONNECTION_ID_KEY = "connection_id"
    private const val CONNECTION_URL_KEY = "connection_url"
    private const val AIRBYTE_EDITION_KEY = "airbyte_edition"
    private const val AIRBYTE_VERSION_KEY = "airbyte_version"
    private const val FAILURE_ORIGIN_KEY = "failure_origin"
    private const val SOURCE = "source"
    private const val FAILURE_TYPE_KEY = "failure_type"
    private const val SYSTEM_ERROR = "system_error"
    private const val CONNECTOR_DEFINITION_ID_KEY = "connector_definition_id"
    private const val CONNECTOR_REPOSITORY_KEY = "connector_repository"
    private const val CONNECTOR_NAME_KEY = "connector_name"
    private const val CONNECTOR_RELEASE_STAGE_KEY = "connector_release_stage"
    private const val CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY = "connector_internal_support_level"
    private const val CONNECTOR_COMMAND_KEY = "connector_command"
    private const val CHECK_COMMAND = "check"
    private const val DISCOVER_COMMAND = "discover"
    private const val SPEC_COMMAND = "spec"
    private const val READ_COMMAND = "read"
    private const val WRITE_COMMAND = "write"
  }
}
