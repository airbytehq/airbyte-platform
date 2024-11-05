package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.Geography
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.pod.Metadata
import io.airbyte.workers.sync.WorkloadClient
import io.airbyte.workers.workload.WorkloadIdGenerator
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest
import io.airbyte.workload.api.client.model.generated.WorkloadLabel
import io.airbyte.workload.api.client.model.generated.WorkloadPriority.Companion.decode
import io.airbyte.workload.api.client.model.generated.WorkloadType
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path
import java.util.UUID

@Singleton
class CheckCommand(
  @Named("workspaceRoot") private val workspaceRoot: Path,
  private val airbyteApiClient: AirbyteApiClient,
  private val workloadClient: WorkloadClient,
  private val workloadIdGenerator: WorkloadIdGenerator,
  private val logClientManager: LogClientManager,
  private val metricClient: MetricClient,
) {
  fun buildWorkloadCreateRequest(input: CheckConnectionInput): WorkloadCreateRequest {
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
    val geo: Geography = getGeography(input.launcherConfig.connectionId, workspaceId)

    return WorkloadCreateRequest(
      workloadId = workloadId,
      labels =
        listOf(
          WorkloadLabel(Metadata.JOB_LABEL_KEY, jobId),
          WorkloadLabel(Metadata.ATTEMPT_LABEL_KEY, attemptNumber.toString()),
          WorkloadLabel(Metadata.WORKSPACE_LABEL_KEY, workspaceId.toString()),
          WorkloadLabel(Metadata.ACTOR_TYPE, input.checkConnectionInput.actorType.toString()),
        ),
      workloadInput = serializedInput,
      logPath = logClientManager.fullLogPath(TemporalUtils.getJobRoot(workspaceRoot, jobId, attemptNumber.toLong())),
      geography = geo.value,
      type = WorkloadType.CHECK,
      priority = decode(input.launcherConfig.priority.toString())!!,
      // TODO
      signalInput = null,
    )
  }

  fun start(input: CheckConnectionInput): String {
    val workloadCreateRequest = buildWorkloadCreateRequest(input)
    workloadClient.createWorkload(workloadCreateRequest)
    return workloadCreateRequest.workloadId
  }

  fun isTerminal(workloadId: String): Boolean = workloadClient.isTerminal(workloadId)

  fun getOutput(workloadId: String): ConnectorJobOutput {
    val output =
      workloadClient.getConnectorJobOutput(
        workloadId,
      ) { failureReason: FailureReason ->
        ConnectorJobOutput()
          .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
          .withCheckConnection(
            StandardCheckConnectionOutput()
              .withStatus(StandardCheckConnectionOutput.Status.FAILED)
              .withMessage(failureReason.externalMessage),
          )
          .withFailureReason(failureReason)
      }

    metricClient.count(
      OssMetricsRegistry.SIDECAR_CHECK,
      1,
      MetricAttribute(MetricTags.STATUS, if (output.checkConnection.status == StandardCheckConnectionOutput.Status.FAILED) "failed" else "success"),
    )

    return output
  }

  fun cancel(): Nothing = TODO()

  fun getGeography(
    connectionId: UUID?,
    workspaceId: UUID?,
  ): Geography {
    try {
      return connectionId?.let {
        airbyteApiClient.connectionApi.getConnection(ConnectionIdRequestBody(it)).geography
      } ?: workspaceId?.let {
        airbyteApiClient.workspaceApi.getWorkspace(WorkspaceIdRequestBody(workspaceId, false)).defaultGeography
      } ?: Geography.AUTO
    } catch (e: Exception) {
      throw WorkerException("Unable to find geography of connection $connectionId", e)
    }
  }
}
