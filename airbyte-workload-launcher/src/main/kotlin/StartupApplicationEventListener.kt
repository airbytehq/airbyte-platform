/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import com.google.common.annotations.VisibleForTesting
import io.airbyte.workload.api.client2.model.generated.Workload
import io.airbyte.workload.api.client2.model.generated.WorkloadListRequest
import io.airbyte.workload.api.client2.model.generated.WorkloadListResponse
import io.airbyte.workload.api.client2.model.generated.WorkloadStatus
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.pipeline.LaunchPipeline
import io.airbyte.workload.launcher.pipeline.LauncherInput
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.discovery.event.ServiceReadyEvent
import io.temporal.worker.WorkerFactory
import jakarta.inject.Singleton
import kotlin.concurrent.thread

private val logger = KotlinLogging.logger {}

@Singleton
class StartupApplicationEventListener(
  private val apiClient: WorkloadApiClient,
  private val pipe: LaunchPipeline,
  private val workerFactory: WorkerFactory,
  @Value("\${airbyte.data-plane-id}") private val dataplaneId: String,
) :
  ApplicationEventListener<ServiceReadyEvent> {
  override fun onApplicationEvent(event: ServiceReadyEvent?) {
    // TODO this might slowdown start quite a bit, should be reworked
    thread {
      rehydrateAndProcessClaimed()
    }.join()

    workerFactory.start()
  }

  @VisibleForTesting
  fun rehydrateAndProcessClaimed() {
    val workloadListRequest =
      WorkloadListRequest(
        listOf(dataplaneId),
        listOf(WorkloadStatus.cLAIMED),
      )

    val workloadList: WorkloadListResponse =
      apiClient.workloadList(workloadListRequest)

    workloadList.workloads.forEach {
      pipe.accept(convertToInputMessage(it))
    }
  }

  @VisibleForTesting
  fun convertToInputMessage(workload: Workload): LauncherInput {
    // TODO(Subodh): Add proper input once the format is decided
    return LauncherInput(workload.id, "workload-input")
  }
}
