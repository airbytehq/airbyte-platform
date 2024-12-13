package io.airbyte.workers.temporal.sync

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.temporal.utils.PayloadChecker
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.workers.input.ReplicationInputMapper
import io.airbyte.workers.storage.activities.OutputStorageClient
import io.airbyte.workers.sync.WorkloadClient
import io.airbyte.workers.workload.JobOutputDocStore
import io.airbyte.workers.workload.WorkloadIdGenerator
import io.airbyte.workload.api.client.WorkloadApiClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.nio.file.Path

class ReplicationActivityTest {
  val replicationInputMapper: ReplicationInputMapper = mockk()
  val workspaceRoot: Path = mockk()
  val airbyteVersion: String = "version"
  val airbyteApiClient: AirbyteApiClient = mockk()
  val workloadApiClient: WorkloadApiClient = mockk()
  val workloadClient: WorkloadClient = mockk()
  val jobOutputDocStore: JobOutputDocStore = mockk()
  val workloadIdGenerator: WorkloadIdGenerator = mockk()
  val metricClient: MetricClient = mockk()
  val featureFlagClient: FeatureFlagClient = mockk()
  val payloadChecker: PayloadChecker = mockk()
  val catalogStorageClient: OutputStorageClient<ConfiguredAirbyteCatalog> = mockk()
  val logClientManager: LogClientManager = mockk()

  val replicationActivity =
    spyk(
      ReplicationActivityImpl(
        replicationInputMapper,
        workspaceRoot,
        airbyteVersion,
        jobOutputDocStore,
        workloadApiClient,
        workloadClient,
        workloadIdGenerator,
        metricClient,
        featureFlagClient,
        payloadChecker,
        catalogStorageClient,
        logClientManager,
      ),
    )

  @Test
  fun `test get workload worker`() {
    every { replicationInputMapper.toReplicationInput(any()) } returns mockk()

    replicationActivity.getWorkerAndReplicationInput(mockk())

    verify { replicationInputMapper.toReplicationInput(any()) }
  }
}
