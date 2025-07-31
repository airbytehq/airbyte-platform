/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.pod.Metadata
import io.airbyte.workers.sync.WorkloadClient
import io.airbyte.workers.workload.DataplaneGroupResolver
import io.airbyte.workers.workload.WorkloadIdGenerator
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.api.domain.WorkloadLabel
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path

@Singleton
class CheckCommand(
  @Named("workspaceRoot") private val workspaceRoot: Path,
  airbyteApiClient: AirbyteApiClient,
  workloadClient: WorkloadClient,
  private val workloadIdGenerator: WorkloadIdGenerator,
  private val logClientManager: LogClientManager,
  private val metricClient: MetricClient,
  private val dataplaneGroupResolver: DataplaneGroupResolver,
) : WorkloadCommandBase<CheckConnectionInput>(
    airbyteApiClient = airbyteApiClient,
    workloadClient = workloadClient,
  ) {
  override val name: String = "check"

  override fun buildWorkloadCreateRequest(
    input: CheckConnectionInput,
    signalPayload: String?,
  ): WorkloadCreateRequest {
    val jobId = input.jobRunConfig.jobId
    val attemptNumber = if (input.jobRunConfig.attemptId == null) 0 else Math.toIntExact(input.jobRunConfig.attemptId)
    val workloadId: String =
      workloadIdGenerator.generateCheckWorkloadId(
        input.checkConnectionInput.actorContext.actorDefinitionId,
        jobId,
        attemptNumber,
      )
    val serializedInput = Jsons.serialize(input)

    val workspaceId = input.checkConnectionInput.actorContext.workspaceId
    val organizationId = input.checkConnectionInput.actorContext.organizationId
    val dataplaneGroup =
      dataplaneGroupResolver.resolveForCheck(
        organizationId = organizationId,
        workspaceId = workspaceId,
        actorId = input.checkConnectionInput.actorContext.actorId,
      )

    return WorkloadCreateRequest(
      workloadId = workloadId,
      labels =
        listOfNotNull(
          WorkloadLabel(Metadata.JOB_LABEL_KEY, jobId),
          WorkloadLabel(Metadata.ATTEMPT_LABEL_KEY, attemptNumber.toString()),
          WorkloadLabel(Metadata.WORKSPACE_LABEL_KEY, workspaceId.toString()),
          WorkloadLabel(Metadata.ACTOR_TYPE, input.checkConnectionInput.actorType.toString()),
          // Can be null if this is the first check that gets run
          input.checkConnectionInput.actorId?.let { WorkloadLabel(Metadata.ACTOR_ID_LABEL_KEY, it.toString()) },
        ),
      workloadInput = serializedInput,
      workspaceId = workspaceId,
      organizationId = organizationId,
      logPath = logClientManager.fullLogPath(TemporalUtils.getJobRoot(workspaceRoot, jobId, attemptNumber.toLong())),
      type = WorkloadType.CHECK,
      priority = WorkloadPriority.fromValue(input.launcherConfig.priority.toString())!!,
      signalInput = signalPayload,
      dataplaneGroup = dataplaneGroup,
    )
  }

  override fun getOutput(id: String): ConnectorJobOutput {
    val output =
      workloadClient.getConnectorJobOutput(
        id,
      ) { failureReason: FailureReason ->
        ConnectorJobOutput()
          .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
          .withCheckConnection(
            StandardCheckConnectionOutput()
              .withStatus(StandardCheckConnectionOutput.Status.FAILED)
              .withMessage(failureReason.externalMessage),
          ).withFailureReason(failureReason)
      }

    metricClient.count(
      metric = OssMetricsRegistry.SIDECAR_CHECK,
      attributes =
        arrayOf(
          MetricAttribute(
            MetricTags.STATUS,
            if (output.checkConnection.status == StandardCheckConnectionOutput.Status.FAILED) "failed" else "success",
          ),
        ),
    )

    return output
  }
}
