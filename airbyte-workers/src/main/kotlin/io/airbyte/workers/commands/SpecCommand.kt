/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.pod.Metadata
import io.airbyte.workers.sync.WorkloadClient
import io.airbyte.workers.workload.DataplaneGroupResolver
import io.airbyte.workers.workload.WorkloadIdGenerator
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.api.domain.WorkloadLabel
import jakarta.inject.Singleton
import java.nio.file.Path

@Singleton
class SpecCommand(
  airbyteApiClient: AirbyteApiClient,
  workloadClient: WorkloadClient,
  private val workloadIdGenerator: WorkloadIdGenerator,
  private val logClientManager: LogClientManager,
  private val dataplaneGroupResolver: DataplaneGroupResolver,
) : WorkloadCommandBase<SpecInput>(
    airbyteApiClient = airbyteApiClient,
    workloadClient = workloadClient,
  ) {
  override val name: String = "spec"

  override fun buildWorkloadCreateRequest(
    input: SpecInput,
    signalPayload: String?,
  ): WorkloadCreateRequest {
    val jobId = input.jobRunConfig.jobId
    val workloadId = workloadIdGenerator.generateSpecWorkloadId(jobId)
    val serializedInput = Jsons.serialize(input)
    val workspaceId = input.launcherConfig.workspaceId
    // todo: add org id to the input or fetch it and pipe through
    val organizationId = null

    val dataplaneGroup =
      dataplaneGroupResolver.resolveForSpec(
        organizationId = null,
        workspaceId = workspaceId,
      )

    return WorkloadCreateRequest(
      workloadId = workloadId,
      labels = listOf(WorkloadLabel(Metadata.JOB_LABEL_KEY, jobId)),
      workloadInput = serializedInput,
      workspaceId = workspaceId,
      organizationId = organizationId,
      logPath = logClientManager.fullLogPath(Path.of(workloadId)),
      type = WorkloadType.SPEC,
      priority = WorkloadPriority.HIGH,
      signalInput = signalPayload,
      dataplaneGroup = dataplaneGroup,
    )
  }

  override fun getOutput(id: String): ConnectorJobOutput =
    workloadClient.getConnectorJobOutput(workloadId = id) { failureReason: FailureReason ->
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.SPEC)
        .withSpec(null)
        .withFailureReason(failureReason)
    }
}
