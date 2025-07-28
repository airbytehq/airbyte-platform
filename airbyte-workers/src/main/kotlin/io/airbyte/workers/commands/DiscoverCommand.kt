/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.workers.input.isReset
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.pod.Metadata
import io.airbyte.workers.sync.WorkloadClient
import io.airbyte.workers.workload.DataplaneGroupResolver
import io.airbyte.workers.workload.WorkloadIdGenerator
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest
import io.airbyte.workload.api.client.model.generated.WorkloadLabel
import io.airbyte.workload.api.client.model.generated.WorkloadPriority.Companion.decode
import io.airbyte.workload.api.client.model.generated.WorkloadType
import io.micronaut.context.annotation.Property
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

@Singleton
class DiscoverCommand(
  @Named("workspaceRoot") private val workspaceRoot: Path,
  airbyteApiClient: AirbyteApiClient,
  workloadClient: WorkloadClient,
  @Property(name = "airbyte.worker.discover.auto-refresh-window") discoverAutoRefreshWindowMinutes: Int,
  private val workloadIdGenerator: WorkloadIdGenerator,
  private val logClientManager: LogClientManager,
  private val dataplaneGroupResolver: DataplaneGroupResolver,
) : WorkloadCommandBase<DiscoverCatalogInput>(
    airbyteApiClient = airbyteApiClient,
    workloadClient = workloadClient,
  ) {
  companion object {
    val NOOP_DISCOVER_PLACEHOLDER_ID = "auto-refresh-disabled"
  }

  private val discoverAutoRefreshWindow: Duration =
    if (discoverAutoRefreshWindowMinutes > 0) discoverAutoRefreshWindowMinutes.minutes else Duration.INFINITE

  override val name: String = "discover"

  override fun start(
    input: DiscoverCatalogInput,
    signalPayload: String?,
  ): String {
    if (input.launcherConfig.isReset() || (isAutoRefresh(input) && discoverAutoRefreshWindow == Duration.INFINITE)) {
      return NOOP_DISCOVER_PLACEHOLDER_ID
    }
    return super.start(input, signalPayload)
  }

  override fun isTerminal(id: String): Boolean {
    if (isNoopDiscover(id)) {
      return true
    }
    return super.isTerminal(id)
  }

  override fun cancel(id: String) {
    if (isNoopDiscover(id)) {
      return
    }
    super.cancel(id)
  }

  override fun buildWorkloadCreateRequest(
    input: DiscoverCatalogInput,
    signalPayload: String?,
  ): WorkloadCreateRequest {
    val jobId = input.jobRunConfig.jobId
    val attemptNumber = if (input.jobRunConfig.attemptId == null) 0 else Math.toIntExact(input.jobRunConfig.attemptId)
    val workloadId =
      if (input.discoverCatalogInput.manual
      ) {
        workloadIdGenerator.generateDiscoverWorkloadId(
          input.discoverCatalogInput.actorContext.actorDefinitionId,
          jobId,
          attemptNumber,
        )
      } else {
        workloadIdGenerator.generateDiscoverWorkloadIdV2WithSnap(
          input.discoverCatalogInput.actorContext.actorId,
          System.currentTimeMillis(),
          discoverAutoRefreshWindow.inWholeMilliseconds,
        )
      }

    val serializedInput = Jsons.serialize(input)

    val workspaceId = input.discoverCatalogInput.actorContext.workspaceId
    val organizationId = input.discoverCatalogInput.actorContext.organizationId
    val dataplaneGroup =
      dataplaneGroupResolver.resolveForDiscover(
        organizationId = organizationId,
        workspaceId = workspaceId,
        actorId = input.discoverCatalogInput.actorContext.actorId,
      )

    return WorkloadCreateRequest(
      workloadId = workloadId,
      labels =
        listOf(
          WorkloadLabel(Metadata.JOB_LABEL_KEY, jobId),
          WorkloadLabel(Metadata.ATTEMPT_LABEL_KEY, attemptNumber.toString()),
          WorkloadLabel(Metadata.WORKSPACE_LABEL_KEY, workspaceId.toString()),
          WorkloadLabel(Metadata.ACTOR_TYPE, ActorType.SOURCE.toString()),
          WorkloadLabel(
            Metadata.ACTOR_ID_LABEL_KEY,
            input.discoverCatalogInput.actorContext.actorId
              .toString(),
          ),
        ),
      workloadInput = serializedInput,
      workspaceId = workspaceId,
      organizationId = organizationId,
      logPath = logClientManager.fullLogPath(TemporalUtils.getJobRoot(workspaceRoot, jobId, attemptNumber.toLong())),
      type = WorkloadType.DISCOVER,
      priority = decode(input.launcherConfig.priority.toString())!!,
      signalInput = signalPayload,
      dataplaneGroup = dataplaneGroup,
    )
  }

  override fun getOutput(id: String): ConnectorJobOutput {
    if (isNoopDiscover(id)) {
      return ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)
        .withDiscoverCatalogId(null)
    }
    return workloadClient.getConnectorJobOutput(id) { failureReason ->
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)
        .withDiscoverCatalogId(null)
        .withFailureReason(failureReason)
    }
  }

  private fun isAutoRefresh(input: DiscoverCatalogInput): Boolean = !input.discoverCatalogInput.manual

  private fun isNoopDiscover(id: String): Boolean = NOOP_DISCOVER_PLACEHOLDER_ID == id
}
