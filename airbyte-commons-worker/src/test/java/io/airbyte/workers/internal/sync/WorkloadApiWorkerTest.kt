package io.airbyte.workers.internal.sync

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.WorkloadApiClient
import io.airbyte.api.client.generated.ConnectionApi
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.Geography
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.exception.DestinationException
import io.airbyte.workers.internal.exception.SourceException
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.storage.StorageClient
import io.airbyte.workers.sync.WorkloadApiWorker
import io.airbyte.workers.sync.WorkloadClient
import io.airbyte.workers.workload.JobOutputDocStore
import io.airbyte.workers.workload.WorkloadIdGenerator
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadStatus
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openapitools.client.infrastructure.ServerException
import java.nio.file.Path
import java.util.Optional
import java.util.UUID
import java.util.concurrent.CancellationException

internal class WorkloadApiWorkerTest {
  private var workloadIdGenerator: WorkloadIdGenerator = mockk()
  private var storageClient: StorageClient = mockk()
  private var apiClient: AirbyteApiClient = mockk()
  private var connectionApi: ConnectionApi = mockk()
  private var workloadApi: WorkloadApi = mockk()
  private var workloadApiClient: WorkloadApiClient = mockk()
  private var featureFlagClient: FeatureFlagClient = mockk()
  private var jobOutputDocStore: JobOutputDocStore = mockk()
  private lateinit var replicationActivityInput: ReplicationActivityInput
  private lateinit var replicationInput: ReplicationInput
  private lateinit var workloadApiWorker: WorkloadApiWorker
  private lateinit var jobRoot: Path

  @BeforeEach
  fun beforeEach() {
    every { apiClient.connectionApi } returns connectionApi
    every { workloadApiClient.workloadApi } returns workloadApi
    featureFlagClient = TestClient()
    jobRoot = Path.of("test", "path")
    replicationActivityInput = ReplicationActivityInput()
    replicationInput = ReplicationInput()
    workloadApiWorker =
      WorkloadApiWorker(
        jobOutputDocStore,
        apiClient,
        workloadApiClient,
        WorkloadClient(workloadApiClient, jobOutputDocStore),
        workloadIdGenerator,
        replicationActivityInput,
        featureFlagClient,
      )
  }

  @Test
  fun testSuccessfulReplication() {
    val jobId = 13L
    val attemptNumber = 37
    val workloadId = "my-workload"
    val expectedOutput =
      ReplicationOutput()
        .withReplicationAttemptSummary(ReplicationAttemptSummary().withStatus(StandardSyncSummary.ReplicationStatus.COMPLETED))
    initializeReplicationInput(jobId, attemptNumber)

    every { workloadIdGenerator.generateSyncWorkloadId(replicationInput.connectionId, jobId, attemptNumber) } returns workloadId

    every {
      connectionApi.getConnection(any())
    } returns
      ConnectionRead(
        connectionId = replicationInput.connectionId,
        name = "name",
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        syncCatalog = AirbyteCatalog(listOf()),
        status = ConnectionStatus.ACTIVE,
        breakingChange = false,
        geography = Geography.US,
      )
    every { workloadApi.workloadCreate(any()) } returns Unit
    every { workloadApi.workloadGet(workloadId) } returns mockWorkload(WorkloadStatus.SUCCESS)

    every { jobOutputDocStore.readSyncOutput(workloadId) } returns Optional.of(expectedOutput)

    val output = workloadApiWorker.run(replicationInput, jobRoot)
    assertEquals(expectedOutput, output)
  }

  @Test
  fun testFailedReplicationWithOutput() {
    val jobId = 13L
    val attemptNumber = 37
    val workloadId = "my-workload"
    val expectedOutput =
      ReplicationOutput()
        .withReplicationAttemptSummary(ReplicationAttemptSummary().withStatus(StandardSyncSummary.ReplicationStatus.COMPLETED))
    initializeReplicationInput(jobId, attemptNumber)

    every { workloadIdGenerator.generateSyncWorkloadId(replicationInput.connectionId, jobId, attemptNumber) } returns workloadId

    every {
      connectionApi.getConnection(any())
    } returns
      ConnectionRead(
        connectionId = replicationInput.connectionId,
        name = "name",
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        syncCatalog = AirbyteCatalog(listOf()),
        status = ConnectionStatus.ACTIVE,
        breakingChange = false,
        geography = Geography.US,
      )
    every { workloadApi.workloadCreate(any()) } returns Unit
    every { workloadApi.workloadGet(workloadId) } returns mockWorkload(WorkloadStatus.FAILURE)

    every { jobOutputDocStore.readSyncOutput(workloadId) } returns Optional.of(expectedOutput)

    val output = workloadApiWorker.run(replicationInput, jobRoot)
    // We expect the output to be returned if it exists, even on a failure
    assertEquals(expectedOutput, output)
  }

  @Test
  fun testResumeReplicationThatAlreadyStarted() {
    val jobId = 313L
    val attemptNumber = 37
    val workloadId = "my-workload"
    val expectedOutput =
      ReplicationOutput()
        .withReplicationAttemptSummary(ReplicationAttemptSummary().withStatus(StandardSyncSummary.ReplicationStatus.COMPLETED))
    initializeReplicationInput(jobId, attemptNumber)

    every { workloadIdGenerator.generateSyncWorkloadId(replicationInput.connectionId, jobId, attemptNumber) } returns workloadId

    every {
      connectionApi.getConnection(any())
    } returns
      ConnectionRead(
        connectionId = replicationInput.connectionId,
        name = "name",
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        syncCatalog = AirbyteCatalog(listOf()),
        status = ConnectionStatus.ACTIVE,
        breakingChange = false,
        geography = Geography.US,
      )
    every { workloadApi.workloadCreate(any()) } throws ServerException(statusCode = 409)
    every { workloadApi.workloadGet(workloadId) } returns mockWorkload(WorkloadStatus.SUCCESS)

    every { jobOutputDocStore.readSyncOutput(workloadId) } returns Optional.of(expectedOutput)

    val output = workloadApiWorker.run(replicationInput, jobRoot)
    assertEquals(expectedOutput, output)
  }

  @Test
  fun testReplicationWithMissingOutput() {
    val jobId = 42L
    val attemptNumber = 1
    val workloadId = "my-failed-workload"
    val expectedDocPrefix = "testNs/orchestrator-repl-job-$jobId-attempt-$attemptNumber"
    initializeReplicationInput(jobId, attemptNumber)

    every { workloadIdGenerator.generateSyncWorkloadId(replicationInput.connectionId, jobId, attemptNumber) } returns workloadId

    every {
      connectionApi.getConnection(any())
    } returns
      ConnectionRead(
        connectionId = replicationInput.connectionId,
        name = "name",
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        syncCatalog = AirbyteCatalog(listOf()),
        status = ConnectionStatus.ACTIVE,
        breakingChange = false,
        geography = Geography.US,
      )
    every { workloadApi.workloadCreate(any()) } returns Unit
    every { workloadApi.workloadGet(workloadId) } returns mockWorkload(WorkloadStatus.SUCCESS)
    every { storageClient.read("$expectedDocPrefix/SUCCEEDED") } returns null

    assertThrows<WorkerException> { workloadApiWorker.run(replicationInput, jobRoot) }
  }

  @Test
  fun testCancelledReplication() {
    val jobId = 42L
    val attemptNumber = 1
    val workloadId = "my-failed-workload"
    initializeReplicationInput(jobId, attemptNumber)

    every { workloadIdGenerator.generateSyncWorkloadId(replicationInput.connectionId, jobId, attemptNumber) } returns workloadId

    every {
      connectionApi.getConnection(any())
    } returns
      ConnectionRead(
        connectionId = replicationInput.connectionId,
        name = "name",
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        syncCatalog = AirbyteCatalog(listOf()),
        status = ConnectionStatus.ACTIVE,
        breakingChange = false,
        geography = Geography.US,
      )
    every { workloadApi.workloadCreate(any()) } returns Unit
    every { workloadApi.workloadGet(workloadId) } returns
      mockWorkload(
        WorkloadStatus.CANCELLED,
        terminationSource = "user",
        terminationReason = "Oops... user cancelled",
      )

    assertThrows<CancellationException> { workloadApiWorker.run(replicationInput, jobRoot) }
  }

  @Test
  fun testFailedReplicationWithPlatformFailure() {
    val jobId = 42L
    val attemptNumber = 1
    val workloadId = "my-failed-workload"
    initializeReplicationInput(jobId, attemptNumber)

    every { workloadIdGenerator.generateSyncWorkloadId(replicationInput.connectionId, jobId, attemptNumber) } returns workloadId

    every {
      connectionApi.getConnection(any())
    } returns
      ConnectionRead(
        connectionId = replicationInput.connectionId,
        name = "name",
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        syncCatalog = AirbyteCatalog(listOf()),
        status = ConnectionStatus.ACTIVE,
        breakingChange = false,
        geography = Geography.US,
      )
    every { workloadApi.workloadCreate(any()) } returns Unit
    every { workloadApi.workloadGet(workloadId) } returns
      mockWorkload(
        WorkloadStatus.FAILURE,
        terminationSource = "airbyte_platform",
        terminationReason = "Oops... platform died",
      )

    assertThrows<WorkerException> { workloadApiWorker.run(replicationInput, jobRoot) }
  }

  @Test
  fun testFailedReplicationWithSourceFailure() {
    val jobId = 43L
    val attemptNumber = 1
    val workloadId = "my-failed-workload"
    initializeReplicationInput(jobId, attemptNumber)

    every { workloadIdGenerator.generateSyncWorkloadId(replicationInput.connectionId, jobId, attemptNumber) } returns workloadId

    every {
      connectionApi.getConnection(any())
    } returns
      ConnectionRead(
        connectionId = replicationInput.connectionId,
        name = "name",
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        syncCatalog = AirbyteCatalog(listOf()),
        status = ConnectionStatus.ACTIVE,
        breakingChange = false,
        geography = Geography.US,
      )
    every { workloadApi.workloadCreate(any()) } returns Unit
    every { workloadApi.workloadGet(workloadId) } returns
      mockWorkload(
        WorkloadStatus.FAILURE,
        terminationSource = "source",
        terminationReason = "Oops... source died",
      )

    assertThrows<SourceException> { workloadApiWorker.run(replicationInput, jobRoot) }
  }

  @Test
  fun testFailedReplicationWithDestinationFailure() {
    val jobId = 44L
    val attemptNumber = 1
    val workloadId = "my-failed-workload"
    initializeReplicationInput(jobId, attemptNumber)

    every { workloadIdGenerator.generateSyncWorkloadId(replicationInput.connectionId, jobId, attemptNumber) } returns workloadId

    every {
      connectionApi.getConnection(any())
    } returns
      ConnectionRead(
        connectionId = replicationInput.connectionId,
        name = "name",
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        syncCatalog = AirbyteCatalog(listOf()),
        status = ConnectionStatus.ACTIVE,
        breakingChange = false,
        geography = Geography.US,
      )
    every { workloadApi.workloadCreate(any()) } returns Unit
    every { workloadApi.workloadGet(workloadId) } returns
      mockWorkload(
        WorkloadStatus.FAILURE,
        terminationSource = "destination",
        terminationReason = "Oops... destination died",
      )

    assertThrows<DestinationException> { workloadApiWorker.run(replicationInput, jobRoot) }
  }

  private fun initializeReplicationInput(
    jobId: Long,
    attemptNumber: Int,
  ) {
    val workspaceId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val jobRunConfig = JobRunConfig().withJobId(jobId.toString()).withAttemptId(attemptNumber.toLong())

    replicationInput.apply {
      this.workspaceId = workspaceId
      this.connectionId = connectionId
      this.jobRunConfig = jobRunConfig
    }

    replicationActivityInput.apply {
      this.workspaceId = workspaceId
      this.connectionId = connectionId
      this.sourceId = sourceId
      this.destinationId = destinationId
      this.jobRunConfig = jobRunConfig
    }
  }

  private fun mockWorkload(
    status: WorkloadStatus,
    terminationSource: String? = null,
    terminationReason: String? = null,
  ): Workload {
    val w = mockk<Workload>()
    every { w.status } returns status
    every { w.terminationReason } returns terminationReason
    every { w.terminationSource } returns terminationSource
    return w
  }
}
