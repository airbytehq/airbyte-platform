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
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.metrics.MetricClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

internal class JobErrorReporterTest {
  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var jobErrorReportingClient: JobErrorReportingClient
  private lateinit var webUrlHelper: WebUrlHelper
  private lateinit var jobErrorReporter: JobErrorReporter

  @BeforeEach
  fun setup() {
    actorDefinitionService = mockk<ActorDefinitionService>()
    sourceService = mockk<SourceService>()
    destinationService = mockk<DestinationService>()
    workspaceService = mockk<WorkspaceService>()
    jobErrorReportingClient = mockk<JobErrorReportingClient>(relaxed = true)
    webUrlHelper = mockk<WebUrlHelper>()
    jobErrorReporter =
      JobErrorReporter(
        actorDefinitionService,
        sourceService,
        destinationService,
        workspaceService,
        AIRBYTE_EDITION,
        AIRBYTE_VERSION,
        webUrlHelper,
        jobErrorReportingClient,
        mockk<MetricClient>(relaxed = true),
      )

    every { webUrlHelper.getConnectionUrl(WORKSPACE_ID, CONNECTION_ID) } returns CONNECTION_URL
    every { webUrlHelper.getWorkspaceUrl(WORKSPACE_ID) } returns WORKSPACE_URL
  }

  @Test
  fun testReportSyncJobFailure() {
    val mFailureSummary = mockk<AttemptFailureSummary>()

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

    every { mFailureSummary.failures } returns
      listOf<FailureReason?>(
        sourceFailureReason,
        destinationFailureReason,
        nonTraceMessageFailureReason,
        replicationFailureReason,
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

    every { sourceService.getSourceDefinitionFromConnection(CONNECTION_ID) } returns
      StandardSourceDefinition()
        .withSourceDefinitionId(SOURCE_DEFINITION_ID)
        .withName(SOURCE_DEFINITION_NAME)

    every { destinationService.getDestinationDefinitionFromConnection(CONNECTION_ID) } returns
      StandardDestinationDefinition()
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withName(DESTINATION_DEFINITION_NAME)

    every { actorDefinitionService.getActorDefinitionVersion(SOURCE_DEFINITION_VERSION_ID) } returns
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withReleaseStage(SOURCE_RELEASE_STAGE)
        .withInternalSupportLevel(SOURCE_INTERNAL_SUPPORT_LEVEL)

    every { actorDefinitionService.getActorDefinitionVersion(DESTINATION_DEFINITION_VERSION_ID) } returns
      ActorDefinitionVersion()
        .withDockerRepository(DESTINATION_DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withReleaseStage(DESTINATION_RELEASE_STAGE)
        .withInternalSupportLevel(DESTINATION_INTERNAL_SUPPORT_LEVEL)

    val mWorkspace = mockk<StandardWorkspace>()
    every { mWorkspace.workspaceId } returns WORKSPACE_ID
    every { workspaceService.getStandardWorkspaceFromConnection(CONNECTION_ID, true) } returns mWorkspace
    every { workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID) } returns Optional.of(ORGANIZATION_ID)

    jobErrorReporter.reportSyncJobFailure(CONNECTION_ID, mFailureSummary, jobReportingContext, attemptConfig)

    val expectedSourceMetadata =
      mapOf<String?, String?>(
        JOB_ID_KEY to syncJobId.toString(),
        WORKSPACE_ID_KEY to WORKSPACE_ID.toString(),
        WORKSPACE_URL_KEY to WORKSPACE_URL,
        CONNECTION_ID_KEY to CONNECTION_ID.toString(),
        CONNECTION_URL_KEY to CONNECTION_URL,
        AIRBYTE_EDITION_KEY to AIRBYTE_EDITION.name,
        AIRBYTE_VERSION_KEY to AIRBYTE_VERSION,
        FAILURE_ORIGIN_KEY to SOURCE,
        FAILURE_TYPE_KEY to SYSTEM_ERROR,
        CONNECTOR_COMMAND_KEY to READ_COMMAND,
        CONNECTOR_DEFINITION_ID_KEY to SOURCE_DEFINITION_ID.toString(),
        CONNECTOR_REPOSITORY_KEY to SOURCE_DOCKER_REPOSITORY,
        CONNECTOR_NAME_KEY to SOURCE_DEFINITION_NAME,
        CONNECTOR_RELEASE_STAGE_KEY to SOURCE_RELEASE_STAGE.toString(),
        CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY to SOURCE_INTERNAL_SUPPORT_LEVEL.toString(),
        ORGANIZATION_ID_META_KEY to ORGANIZATION_ID.toString(),
      )

    val expectedDestinationMetadata =
      mapOf<String?, String?>(
        JOB_ID_KEY to syncJobId.toString(),
        WORKSPACE_ID_KEY to WORKSPACE_ID.toString(),
        WORKSPACE_URL_KEY to WORKSPACE_URL,
        CONNECTION_ID_KEY to CONNECTION_ID.toString(),
        CONNECTION_URL_KEY to CONNECTION_URL,
        AIRBYTE_EDITION_KEY to AIRBYTE_EDITION.name,
        AIRBYTE_VERSION_KEY to AIRBYTE_VERSION,
        FAILURE_ORIGIN_KEY to "destination",
        FAILURE_TYPE_KEY to SYSTEM_ERROR,
        CONNECTOR_COMMAND_KEY to WRITE_COMMAND,
        CONNECTOR_DEFINITION_ID_KEY to DESTINATION_DEFINITION_ID.toString(),
        CONNECTOR_REPOSITORY_KEY to DESTINATION_DOCKER_REPOSITORY,
        CONNECTOR_NAME_KEY to DESTINATION_DEFINITION_NAME,
        CONNECTOR_RELEASE_STAGE_KEY to DESTINATION_RELEASE_STAGE.toString(),
        CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY to DESTINATION_INTERNAL_SUPPORT_LEVEL.toString(),
        ORGANIZATION_ID_META_KEY to ORGANIZATION_ID.toString(),
      )

    verifyAll {
      jobErrorReportingClient.reportJobFailureReason(
        mWorkspace,
        sourceFailureReason,
        SOURCE_DOCKER_IMAGE,
        expectedSourceMetadata,
        attemptConfig,
      )
      jobErrorReportingClient.reportJobFailureReason(
        mWorkspace,
        destinationFailureReason,
        DESTINATION_DOCKER_IMAGE,
        expectedDestinationMetadata,
        attemptConfig,
      )
    }
  }

  @Test
  fun testReportSyncJobFailureDoesNotThrow() {
    val mFailureSummary = mockk<AttemptFailureSummary>()
    val jobContext = SyncJobReportingContext(1L, SOURCE_DEFINITION_VERSION_ID, DESTINATION_DEFINITION_VERSION_ID)

    val sourceFailureReason =
      FailureReason()
        .withMetadata(Metadata().withAdditionalProperty(FROM_TRACE_MESSAGE, true))
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)

    val objectMapper = ObjectMapper()
    val attemptConfig =
      AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), State())

    every { mFailureSummary.failures } returns listOf<FailureReason?>(sourceFailureReason)

    every { sourceService.getSourceDefinitionFromConnection(CONNECTION_ID) } returns
      StandardSourceDefinition()
        .withSourceDefinitionId(SOURCE_DEFINITION_ID)
        .withName(SOURCE_DEFINITION_NAME)

    every { actorDefinitionService.getActorDefinitionVersion(SOURCE_DEFINITION_VERSION_ID) } returns
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withReleaseStage(SOURCE_RELEASE_STAGE)

    val mWorkspace = mockk<StandardWorkspace>()
    every { mWorkspace.workspaceId } returns WORKSPACE_ID
    every { workspaceService.getStandardWorkspaceFromConnection(CONNECTION_ID, true) } returns mWorkspace
    every { webUrlHelper.getConnectionUrl(WORKSPACE_ID, CONNECTION_ID) } returns CONNECTION_URL
    every { workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID) } returns Optional.empty()

    every {
      jobErrorReportingClient.reportJobFailureReason(
        any<StandardWorkspace>(),
        sourceFailureReason,
        any<String>(),
        any<Map<String?, String?>>(),
        any<AttemptConfigReportingContext>(),
      )
    } throws RuntimeException("some exception")

    Assertions.assertDoesNotThrow {
      jobErrorReporter.reportSyncJobFailure(
        CONNECTION_ID,
        mFailureSummary,
        jobContext,
        attemptConfig,
      )
    }
    verify(exactly = 1) {
      jobErrorReportingClient.reportJobFailureReason(
        any<StandardWorkspace>(),
        any<FailureReason>(),
        any<String>(),
        any<Map<String?, String?>>(),
        any<AttemptConfigReportingContext>(),
      )
    }
  }

  @Test
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

    every { sourceService.getStandardSourceDefinition(SOURCE_DEFINITION_ID) } returns
      StandardSourceDefinition()
        .withSourceDefinitionId(SOURCE_DEFINITION_ID)
        .withName(SOURCE_DEFINITION_NAME)

    jobErrorReporter.reportSourceCheckJobFailure(SOURCE_DEFINITION_ID, null, failureReason, jobContext)

    val expectedMetadata =
      mapOf<String?, String?>(
        JOB_ID_KEY to JOB_ID.toString(),
        AIRBYTE_EDITION_KEY to AIRBYTE_EDITION.name,
        AIRBYTE_VERSION_KEY to AIRBYTE_VERSION,
        FAILURE_ORIGIN_KEY to SOURCE,
        FAILURE_TYPE_KEY to SYSTEM_ERROR,
        CONNECTOR_DEFINITION_ID_KEY to SOURCE_DEFINITION_ID.toString(),
        CONNECTOR_REPOSITORY_KEY to SOURCE_DOCKER_REPOSITORY,
        CONNECTOR_NAME_KEY to SOURCE_DEFINITION_NAME,
        CONNECTOR_RELEASE_STAGE_KEY to SOURCE_RELEASE_STAGE.toString(),
        CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY to SOURCE_INTERNAL_SUPPORT_LEVEL.toString(),
        CONNECTOR_COMMAND_KEY to CHECK_COMMAND,
      )

    verify(exactly = 1) {
      jobErrorReportingClient.reportJobFailureReason(null, failureReason, SOURCE_DOCKER_IMAGE, expectedMetadata, null)
    }
  }

  @Test
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

    jobErrorReporter.reportSourceCheckJobFailure(SOURCE_DEFINITION_ID, WORKSPACE_ID, failureReason, jobContext)

    verify(exactly = 0) { jobErrorReportingClient.reportJobFailureReason(any(), any(), any(), any(), any()) }
  }

  @Test
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

    every { destinationService.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID) } returns
      StandardDestinationDefinition()
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withName(DESTINATION_DEFINITION_NAME)

    val mWorkspace = mockk<StandardWorkspace>()
    every { mWorkspace.workspaceId } returns WORKSPACE_ID
    every { workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true) } returns mWorkspace
    every { workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID) } returns Optional.of(ORGANIZATION_ID)

    jobErrorReporter.reportDestinationCheckJobFailure(DESTINATION_DEFINITION_ID, WORKSPACE_ID, failureReason, jobContext)

    val expectedMetadata =
      mapOf<String?, String?>(
        JOB_ID_KEY to JOB_ID.toString(),
        WORKSPACE_ID_KEY to WORKSPACE_ID.toString(),
        WORKSPACE_URL_KEY to WORKSPACE_URL,
        AIRBYTE_EDITION_KEY to AIRBYTE_EDITION.name,
        AIRBYTE_VERSION_KEY to AIRBYTE_VERSION,
        FAILURE_ORIGIN_KEY to "destination",
        FAILURE_TYPE_KEY to SYSTEM_ERROR,
        CONNECTOR_DEFINITION_ID_KEY to DESTINATION_DEFINITION_ID.toString(),
        CONNECTOR_REPOSITORY_KEY to DESTINATION_DOCKER_REPOSITORY,
        CONNECTOR_NAME_KEY to DESTINATION_DEFINITION_NAME,
        CONNECTOR_RELEASE_STAGE_KEY to DESTINATION_RELEASE_STAGE.toString(),
        CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY to DESTINATION_INTERNAL_SUPPORT_LEVEL.toString(),
        CONNECTOR_COMMAND_KEY to CHECK_COMMAND,
        ORGANIZATION_ID_META_KEY to ORGANIZATION_ID.toString(),
      )

    verify(exactly = 1) {
      jobErrorReportingClient.reportJobFailureReason(mWorkspace, failureReason, DESTINATION_DOCKER_IMAGE, expectedMetadata, null)
    }
  }

  @Test
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

    jobErrorReporter.reportDestinationCheckJobFailure(DESTINATION_DEFINITION_ID, WORKSPACE_ID, failureReason, jobContext)

    verify(exactly = 0) { jobErrorReportingClient.reportJobFailureReason(any(), any(), any(), any(), any()) }
  }

  @Test
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

    every { destinationService.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID) } returns
      StandardDestinationDefinition()
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withName(DESTINATION_DEFINITION_NAME)

    jobErrorReporter.reportDestinationCheckJobFailure(DESTINATION_DEFINITION_ID, null, failureReason, jobContext)

    val expectedMetadata =
      mapOf<String?, String?>(
        JOB_ID_KEY to JOB_ID.toString(),
        AIRBYTE_EDITION_KEY to AIRBYTE_EDITION.name,
        AIRBYTE_VERSION_KEY to AIRBYTE_VERSION,
        FAILURE_ORIGIN_KEY to "destination",
        FAILURE_TYPE_KEY to SYSTEM_ERROR,
        CONNECTOR_DEFINITION_ID_KEY to DESTINATION_DEFINITION_ID.toString(),
        CONNECTOR_REPOSITORY_KEY to DESTINATION_DOCKER_REPOSITORY,
        CONNECTOR_NAME_KEY to DESTINATION_DEFINITION_NAME,
        CONNECTOR_RELEASE_STAGE_KEY to DESTINATION_RELEASE_STAGE.toString(),
        CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY to DESTINATION_INTERNAL_SUPPORT_LEVEL.toString(),
        CONNECTOR_COMMAND_KEY to CHECK_COMMAND,
      )

    verify(exactly = 1) {
      jobErrorReportingClient.reportJobFailureReason(null, failureReason, DESTINATION_DOCKER_IMAGE, expectedMetadata, null)
    }
  }

  @Test
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

    every { sourceService.getStandardSourceDefinition(SOURCE_DEFINITION_ID) } returns
      StandardSourceDefinition()
        .withSourceDefinitionId(SOURCE_DEFINITION_ID)
        .withName(SOURCE_DEFINITION_NAME)

    val mWorkspace = mockk<StandardWorkspace>()
    every { mWorkspace.workspaceId } returns WORKSPACE_ID
    every { workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true) } returns mWorkspace
    every { workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID) } returns Optional.of(ORGANIZATION_ID)

    jobErrorReporter.reportDiscoverJobFailure(SOURCE_DEFINITION_ID, ActorType.SOURCE, WORKSPACE_ID, failureReason, jobContext)

    val expectedMetadata =
      mapOf<String?, String?>(
        JOB_ID_KEY to JOB_ID.toString(),
        WORKSPACE_ID_KEY to WORKSPACE_ID.toString(),
        WORKSPACE_URL_KEY to WORKSPACE_URL,
        AIRBYTE_EDITION_KEY to AIRBYTE_EDITION.name,
        AIRBYTE_VERSION_KEY to AIRBYTE_VERSION,
        FAILURE_ORIGIN_KEY to SOURCE,
        FAILURE_TYPE_KEY to SYSTEM_ERROR,
        CONNECTOR_DEFINITION_ID_KEY to SOURCE_DEFINITION_ID.toString(),
        CONNECTOR_REPOSITORY_KEY to SOURCE_DOCKER_REPOSITORY,
        CONNECTOR_NAME_KEY to SOURCE_DEFINITION_NAME,
        CONNECTOR_RELEASE_STAGE_KEY to SOURCE_RELEASE_STAGE.toString(),
        CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY to SOURCE_INTERNAL_SUPPORT_LEVEL.toString(),
        CONNECTOR_COMMAND_KEY to DISCOVER_COMMAND,
        ORGANIZATION_ID_META_KEY to ORGANIZATION_ID.toString(),
      )

    verify(exactly = 1) {
      jobErrorReportingClient.reportJobFailureReason(mWorkspace, failureReason, SOURCE_DOCKER_IMAGE, expectedMetadata, null)
    }
  }

  @Test
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

    every { sourceService.getStandardSourceDefinition(SOURCE_DEFINITION_ID) } returns
      StandardSourceDefinition()
        .withSourceDefinitionId(SOURCE_DEFINITION_ID)
        .withName(SOURCE_DEFINITION_NAME)

    jobErrorReporter.reportDiscoverJobFailure(SOURCE_DEFINITION_ID, ActorType.SOURCE, null, failureReason, jobContext)

    val expectedMetadata =
      mapOf<String?, String?>(
        JOB_ID_KEY to JOB_ID.toString(),
        AIRBYTE_EDITION_KEY to AIRBYTE_EDITION.name,
        AIRBYTE_VERSION_KEY to AIRBYTE_VERSION,
        FAILURE_ORIGIN_KEY to SOURCE,
        FAILURE_TYPE_KEY to SYSTEM_ERROR,
        CONNECTOR_DEFINITION_ID_KEY to SOURCE_DEFINITION_ID.toString(),
        CONNECTOR_REPOSITORY_KEY to SOURCE_DOCKER_REPOSITORY,
        CONNECTOR_NAME_KEY to SOURCE_DEFINITION_NAME,
        CONNECTOR_RELEASE_STAGE_KEY to SOURCE_RELEASE_STAGE.toString(),
        CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY to SOURCE_INTERNAL_SUPPORT_LEVEL.toString(),
        CONNECTOR_COMMAND_KEY to DISCOVER_COMMAND,
      )

    verify(exactly = 1) {
      jobErrorReportingClient.reportJobFailureReason(null, failureReason, SOURCE_DOCKER_IMAGE, expectedMetadata, null)
    }
  }

  @Test
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

    jobErrorReporter.reportDiscoverJobFailure(SOURCE_DEFINITION_ID, ActorType.SOURCE, WORKSPACE_ID, failureReason, jobContext)

    verify(exactly = 0) { jobErrorReportingClient.reportJobFailureReason(any(), any(), any(), any(), any()) }
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

    jobErrorReporter.reportSpecJobFailure(failureReason, jobContext)

    val expectedMetadata =
      mapOf<String?, String?>(
        JOB_ID_KEY to JOB_ID.toString(),
        AIRBYTE_EDITION_KEY to AIRBYTE_EDITION.name,
        AIRBYTE_VERSION_KEY to AIRBYTE_VERSION,
        FAILURE_ORIGIN_KEY to SOURCE,
        FAILURE_TYPE_KEY to SYSTEM_ERROR,
        CONNECTOR_REPOSITORY_KEY to SOURCE_DOCKER_REPOSITORY,
        CONNECTOR_COMMAND_KEY to SPEC_COMMAND,
      )

    verify(exactly = 1) {
      jobErrorReportingClient.reportJobFailureReason(null, failureReason, SOURCE_DOCKER_IMAGE, expectedMetadata, null)
    }
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

    jobErrorReporter.reportSpecJobFailure(failureReason, jobContext)

    verify(exactly = 0) { jobErrorReportingClient.reportJobFailureReason(any(), any(), any(), any(), any()) }
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

    jobErrorReporter.reportSpecJobFailure(readFailureReason, jobContext)
    jobErrorReporter.reportSpecJobFailure(discoverFailureReason, jobContext)
    jobErrorReporter.reportSpecJobFailure(checkFailureReason, jobContext)

    verify(exactly = 0) { jobErrorReportingClient.reportJobFailureReason(any(), any(), any(), any(), any()) }
  }

  companion object {
    private val JOB_ID: UUID = UUID.randomUUID()
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val ORGANIZATION_ID: UUID = UUID.randomUUID()
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
    private const val SOURCE_DOCKER_IMAGE: String = "$SOURCE_DOCKER_REPOSITORY:$DOCKER_IMAGE_TAG"
    private val SOURCE_RELEASE_STAGE = ReleaseStage.BETA
    private const val SOURCE_INTERNAL_SUPPORT_LEVEL = 200L
    private val DESTINATION_DEFINITION_ID: UUID = UUID.randomUUID()
    private val DESTINATION_DEFINITION_VERSION_ID: UUID = UUID.randomUUID()
    private const val DESTINATION_DEFINITION_NAME = "snowflake"
    private const val DESTINATION_DOCKER_REPOSITORY = "airbyte/destination-snowflake"
    private const val DESTINATION_DOCKER_IMAGE: String = "$DESTINATION_DOCKER_REPOSITORY:$DOCKER_IMAGE_TAG"
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
    private const val ORGANIZATION_ID_META_KEY = "organization_id"
  }
}
