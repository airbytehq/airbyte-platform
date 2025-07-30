/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter

import com.google.common.collect.ImmutableSet
import io.airbyte.api.client.WebUrlHelper
import io.airbyte.commons.lang.Exceptions
import io.airbyte.config.ActorType
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.FailureReason
import io.airbyte.config.ReleaseStage
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.IOException
import java.util.UUID

/**
 * Report errors from Jobs. Common error information that can be sent to any of the reporting
 * clients that we support.
 */
class JobErrorReporter(
  private val actorDefinitionService: ActorDefinitionService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val workspaceService: WorkspaceService,
  private val airbyteEdition: AirbyteEdition,
  private val airbyteVersion: String,
  private val webUrlHelper: WebUrlHelper,
  private val jobErrorReportingClient: JobErrorReportingClient,
  private val metricClient: MetricClient,
) {
  /**
   * Reports a Sync Job's connector-caused FailureReasons to the JobErrorReportingClient.
   *
   * @param connectionId - connection that had the failure
   * @param failureSummary - final attempt failure summary
   * @param jobContext - sync job reporting context
   */
  fun reportSyncJobFailure(
    connectionId: UUID,
    failureSummary: AttemptFailureSummary,
    jobContext: SyncJobReportingContext,
    attemptConfig: AttemptConfigReportingContext?,
  ) {
    Exceptions.swallow {
      try {
        log.info(
          "{} failures incoming for jobId '{}' connectionId '{}'",
          if (failureSummary.failures == null) 0 else failureSummary.failures.size,
          jobContext.jobId,
          connectionId,
        )
        val traceMessageFailures =
          failureSummary.failures
            .stream()
            .filter { failure: FailureReason ->
              failure.metadata != null &&
                failure.metadata.additionalProperties.containsKey(
                  FROM_TRACE_MESSAGE,
                )
            }.toList()

        val workspace = workspaceService.getStandardWorkspaceFromConnection(connectionId, true)
        val commonMetadata = mutableMapOf<String?, String?>()
        commonMetadata.putAll(java.util.Map.of(JOB_ID_KEY, jobContext.jobId.toString()))
        commonMetadata.putAll(getConnectionMetadata(workspace.workspaceId, connectionId))

        log.info(
          "{} failures to report for jobId '{}' connectionId '{}'",
          traceMessageFailures.size,
          jobContext.jobId,
          connectionId,
        )
        for (failureReason in traceMessageFailures) {
          val failureOrigin = failureReason.failureOrigin
          log.info(
            "Reporting failure for jobId '{}' connectionId '{}' origin '{}'",
            jobContext.jobId,
            connectionId,
            failureOrigin,
          )

          // We only care about the failure origins listed below, i.e. those that come from connectors.
          // The rest are ignored.
          if (failureOrigin == FailureReason.FailureOrigin.SOURCE) {
            val sourceDefinition =
              sourceService.getSourceDefinitionFromConnection(connectionId)
            val sourceVersion =
              actorDefinitionService.getActorDefinitionVersion(jobContext.sourceVersionId!!)
            val dockerImage = ActorDefinitionVersionHelper.getDockerImageName(sourceVersion)
            if (sourceVersion.language != null) {
              commonMetadata[SOURCE_TYPE_META_KEY] = sourceVersion.language
            }
            val metadata =
              commonMetadata +
                getSourceMetadata(sourceDefinition, dockerImage, sourceVersion.releaseStage, sourceVersion.internalSupportLevel)

            reportJobFailureReason(workspace, failureReason, dockerImage, metadata, attemptConfig)
          } else if (failureOrigin == FailureReason.FailureOrigin.DESTINATION) {
            val destinationDefinition =
              destinationService.getDestinationDefinitionFromConnection(connectionId)
            val destinationVersion =
              actorDefinitionService.getActorDefinitionVersion(jobContext.destinationVersionId!!)
            val dockerImage = ActorDefinitionVersionHelper.getDockerImageName(destinationVersion)
            val metadata =
              commonMetadata +
                getDestinationMetadata(
                  destinationDefinition,
                  dockerImage,
                  destinationVersion.releaseStage,
                  destinationVersion.internalSupportLevel,
                )

            reportJobFailureReason(workspace, failureReason, dockerImage, metadata, attemptConfig)
          }
        }
      } catch (e: Exception) {
        log.error(
          "Failed to report status for jobId '{}' connectionId '{}': {}",
          jobContext.jobId,
          connectionId,
          e,
        )
        throw e
      }
    }
  }

  /**
   * Reports a FailureReason from a connector Check job for a Source to the JobErrorReportingClient.
   *
   * @param workspaceId - workspace for which the check failed
   * @param failureReason - failure reason from the check connection job
   * @param jobContext - connector job reporting context
   */
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun reportSourceCheckJobFailure(
    sourceDefinitionId: UUID,
    workspaceId: UUID?,
    failureReason: FailureReason,
    jobContext: ConnectorJobReportingContext,
  ) {
    if (failureReason.failureOrigin != FailureReason.FailureOrigin.SOURCE) {
      return
    }
    val workspace = if (workspaceId != null) workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) else null
    val sourceDefinition = sourceService.getStandardSourceDefinition(sourceDefinitionId)
    val metadata =
      getSourceMetadata(sourceDefinition, jobContext.dockerImage, jobContext.releaseStage, jobContext.internalSupportLevel) +
        java.util.Map.of(JOB_ID_KEY, jobContext.jobId.toString())
    reportJobFailureReason(workspace, failureReason.withFailureOrigin(FailureReason.FailureOrigin.SOURCE), jobContext.dockerImage, metadata, null)
  }

  /**
   * Reports a FailureReason from a connector Check job for a Destination to the
   * JobErrorReportingClient.
   *
   * @param workspaceId - workspace for which the check failed
   * @param failureReason - failure reason from the check connection job
   * @param jobContext - connector job reporting context
   */
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun reportDestinationCheckJobFailure(
    destinationDefinitionId: UUID,
    workspaceId: UUID?,
    failureReason: FailureReason,
    jobContext: ConnectorJobReportingContext,
  ) {
    if (failureReason.failureOrigin != FailureReason.FailureOrigin.DESTINATION) {
      return
    }
    val workspace = if (workspaceId != null) workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) else null
    val destinationDefinition = destinationService.getStandardDestinationDefinition(destinationDefinitionId)
    val metadata =
      getDestinationMetadata(destinationDefinition, jobContext.dockerImage, jobContext.releaseStage, jobContext.internalSupportLevel) +
        java.util.Map.of(JOB_ID_KEY, jobContext.jobId.toString())
    reportJobFailureReason(
      workspace,
      failureReason.withFailureOrigin(FailureReason.FailureOrigin.DESTINATION),
      jobContext.dockerImage,
      metadata,
      null,
    )
  }

  /**
   * Reports a FailureReason from a connector Deploy job for a Source to the JobErrorReportingClient.
   *
   * @param workspaceId - workspace for which the Discover job failed
   * @param failureReason - failure reason from the Discover job
   * @param jobContext - connector job reporting context
   */
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun reportDiscoverJobFailure(
    actorDefinitionId: UUID,
    actorType: ActorType,
    workspaceId: UUID?,
    failureReason: FailureReason,
    jobContext: ConnectorJobReportingContext,
  ) {
    if (failureReason.failureOrigin != FailureReason.FailureOrigin.SOURCE &&
      failureReason.failureOrigin != FailureReason.FailureOrigin.DESTINATION
    ) {
      return
    }
    val workspace = if (workspaceId != null) workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) else null
    val actorDefMetadata =
      if (actorType == ActorType.DESTINATION) {
        getDestinationMetadata(
          destinationService.getStandardDestinationDefinition(actorDefinitionId),
          jobContext.dockerImage,
          jobContext.releaseStage,
          jobContext.internalSupportLevel,
        )
      } else {
        getSourceMetadata(
          sourceService.getStandardSourceDefinition(actorDefinitionId),
          jobContext.dockerImage,
          jobContext.releaseStage,
          jobContext.internalSupportLevel,
        )
      }
    val metadata =
      actorDefMetadata +
        java.util.Map.of(JOB_ID_KEY, jobContext.jobId.toString())
    reportJobFailureReason(workspace, failureReason, jobContext.dockerImage, metadata, null)
  }

  /**
   * Reports a FailureReason from a connector Spec job to the JobErrorReportingClient.
   *
   * @param failureReason - failure reason from the Deploy job
   * @param jobContext - connector job reporting context
   */
  fun reportSpecJobFailure(
    failureReason: FailureReason,
    jobContext: ConnectorJobReportingContext,
  ) {
    if (failureReason.failureOrigin != FailureReason.FailureOrigin.SOURCE &&
      failureReason.failureOrigin != FailureReason.FailureOrigin.DESTINATION
    ) {
      return
    }
    val dockerImage = jobContext.dockerImage
    val connectorRepository = dockerImage.split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()[0]
    val metadata =
      java.util.Map.of(
        JOB_ID_KEY,
        jobContext.jobId.toString(),
        CONNECTOR_REPOSITORY_META_KEY,
        connectorRepository,
      )
    reportJobFailureReason(null, failureReason, dockerImage, metadata, null)
  }

  private fun getConnectionMetadata(
    workspaceId: UUID,
    connectionId: UUID,
  ): Map<String?, String?> {
    val connectionUrl = webUrlHelper.getConnectionUrl(workspaceId, connectionId)
    return java.util.Map.ofEntries(
      java.util.Map.entry(CONNECTION_ID_META_KEY, connectionId.toString()),
      java.util.Map.entry(CONNECTION_URL_META_KEY, connectionUrl),
    )
  }

  private fun getDestinationMetadata(
    destinationDefinition: StandardDestinationDefinition,
    dockerImage: String,
    releaseStage: ReleaseStage?,
    internalSupportLevel: Long?,
  ): Map<String?, String?> {
    val connectorRepository = dockerImage.split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()[0]

    val metadata: MutableMap<String?, String?> =
      HashMap(
        java.util.Map.ofEntries(
          java.util.Map.entry(CONNECTOR_DEFINITION_ID_META_KEY, destinationDefinition.destinationDefinitionId.toString()),
          java.util.Map.entry(CONNECTOR_NAME_META_KEY, destinationDefinition.name),
          java.util.Map.entry(CONNECTOR_REPOSITORY_META_KEY, connectorRepository),
        ),
      )
    if (releaseStage != null) {
      metadata[CONNECTOR_RELEASE_STAGE_META_KEY] = releaseStage.value()
    }
    if (internalSupportLevel != null) {
      metadata[CONNECTOR_INTERNAL_SUPPORT_LEVEL_META_KEY] = internalSupportLevel.toString()
    }

    return metadata
  }

  private fun getSourceMetadata(
    sourceDefinition: StandardSourceDefinition,
    dockerImage: String,
    releaseStage: ReleaseStage?,
    internalSupportLevel: Long?,
  ): Map<String?, String?> {
    val connectorRepository = dockerImage.split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()[0]
    val metadata: MutableMap<String?, String?> =
      HashMap(
        java.util.Map.ofEntries(
          java.util.Map.entry(CONNECTOR_DEFINITION_ID_META_KEY, sourceDefinition.sourceDefinitionId.toString()),
          java.util.Map.entry(CONNECTOR_NAME_META_KEY, sourceDefinition.name),
          java.util.Map.entry(CONNECTOR_REPOSITORY_META_KEY, connectorRepository),
        ),
      )
    if (releaseStage != null) {
      metadata[CONNECTOR_RELEASE_STAGE_META_KEY] = releaseStage.value()
    }
    if (internalSupportLevel != null) {
      metadata[CONNECTOR_INTERNAL_SUPPORT_LEVEL_META_KEY] = internalSupportLevel.toString()
    }

    return metadata
  }

  private fun getFailureReasonMetadata(failureReason: FailureReason): Map<String?, String?> {
    val failureReasonAdditionalProps =
      if (failureReason.metadata != null) failureReason.metadata.additionalProperties else java.util.Map.of()
    val outMetadata: MutableMap<String?, String?> = HashMap()

    if (failureReasonAdditionalProps.containsKey(CONNECTOR_COMMAND_META_KEY) &&
      failureReasonAdditionalProps[CONNECTOR_COMMAND_META_KEY] != null
    ) {
      outMetadata[CONNECTOR_COMMAND_META_KEY] =
        failureReasonAdditionalProps[CONNECTOR_COMMAND_META_KEY].toString()
    }

    if (failureReason.failureOrigin != null) {
      outMetadata[FAILURE_ORIGIN_META_KEY] = failureReason.failureOrigin.value()
    }

    if (failureReason.failureType != null) {
      outMetadata[FAILURE_TYPE_META_KEY] = failureReason.failureType.value()
    }

    return outMetadata
  }

  private fun getWorkspaceMetadata(workspaceId: UUID): Map<String?, String?> {
    val workspaceUrl = webUrlHelper.getWorkspaceUrl(workspaceId)
    return java.util.Map.ofEntries(
      java.util.Map.entry(WORKSPACE_ID_META_KEY, workspaceId.toString()),
      java.util.Map.entry(WORKSPACE_URL_META_KEY, workspaceUrl),
    )
  }

  private fun reportJobFailureReason(
    workspace: StandardWorkspace?,
    failureReason: FailureReason,
    dockerImage: String,
    metadata: Map<String?, String?>,
    attemptConfig: AttemptConfigReportingContext?,
  ) {
    // Failure types associated with a config-error or a manual-cancellation should NOT be reported.
    if (UNSUPPORTED_FAILURETYPES.contains(failureReason.failureType)) {
      metricClient.count(
        OssMetricsRegistry.ERROR_REPORTING_EVENT_COUNT,
        1,
        MetricAttribute(MetricTags.STATUS_TAG, MetricTags.SKIPPED),
        MetricAttribute(MetricTags.FAILURE_TYPE, failureReason.failureType.value()),
      )
      return
    }

    val commonMetadata: MutableMap<String?, String?> =
      HashMap(
        java.util.Map.ofEntries(
          java.util.Map.entry(AIRBYTE_VERSION_META_KEY, airbyteVersion),
          java.util.Map.entry(AIRBYTE_EDITION_META_KEY, airbyteEdition.name),
        ),
      )

    if (workspace != null) {
      commonMetadata.putAll(getWorkspaceMetadata(workspace.workspaceId))
    }

    val allMetadata =
      commonMetadata +
        getFailureReasonMetadata(failureReason) +
        metadata

    try {
      jobErrorReportingClient.reportJobFailureReason(workspace, failureReason, dockerImage, allMetadata, attemptConfig)
      metricClient.count(
        OssMetricsRegistry.ERROR_REPORTING_EVENT_COUNT,
        1,
        MetricAttribute(MetricTags.STATUS_TAG, MetricTags.SUCCESS),
        MetricAttribute(MetricTags.FAILURE_TYPE, failureReason.failureType.value()),
      )
    } catch (e: Exception) {
      metricClient.count(
        OssMetricsRegistry.ERROR_REPORTING_EVENT_COUNT,
        1,
        MetricAttribute(MetricTags.STATUS_TAG, MetricTags.FAILURE),
        MetricAttribute(MetricTags.FAILURE_TYPE, failureReason.failureType.value()),
      )
      log.error(e) { "Error when reporting job failure reason: $failureReason" }
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
    const val FROM_TRACE_MESSAGE = "from_trace_message"
    const val AIRBYTE_EDITION_META_KEY: String = "airbyte_edition"
    const val AIRBYTE_VERSION_META_KEY: String = "airbyte_version"
    const val FAILURE_ORIGIN_META_KEY: String = "failure_origin"
    const val FAILURE_TYPE_META_KEY: String = "failure_type"
    const val WORKSPACE_ID_META_KEY: String = "workspace_id"
    const val WORKSPACE_URL_META_KEY: String = "workspace_url"
    const val CONNECTION_ID_META_KEY: String = "connection_id"
    const val CONNECTION_URL_META_KEY: String = "connection_url"
    const val CONNECTOR_NAME_META_KEY: String = "connector_name"
    const val CONNECTOR_REPOSITORY_META_KEY: String = "connector_repository"
    const val CONNECTOR_DEFINITION_ID_META_KEY: String = "connector_definition_id"
    const val CONNECTOR_RELEASE_STAGE_META_KEY: String = "connector_release_stage"
    private const val CONNECTOR_INTERNAL_SUPPORT_LEVEL_META_KEY = "connector_internal_support_level"
    private const val CONNECTOR_COMMAND_META_KEY = "connector_command"
    const val JOB_ID_KEY: String = "job_id"
    const val SOURCE_TYPE_META_KEY: String = "source_type"

    private val UNSUPPORTED_FAILURETYPES: Set<FailureReason.FailureType> =
      ImmutableSet.of(
        FailureReason.FailureType.CONFIG_ERROR,
        FailureReason.FailureType.MANUAL_CANCELLATION,
        FailureReason.FailureType.TRANSIENT_ERROR,
      )
  }
}
