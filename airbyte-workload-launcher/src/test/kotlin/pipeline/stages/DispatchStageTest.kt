/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import fixtures.RecordFixtures
import io.airbyte.config.ConnectInput
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.WorkloadType
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.exception.WorkloadLauncherException
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workload.launcher.pipeline.dispatch.DirectCheckEligibility
import io.airbyte.workload.launcher.pipeline.dispatch.DirectCheckRouter
import io.airbyte.workload.launcher.pipeline.dispatch.DirectJobRunnerClient
import io.airbyte.workload.launcher.pipeline.stages.model.CheckPayload
import io.airbyte.workload.launcher.pipeline.stages.model.ConnectPayload
import io.airbyte.workload.launcher.pipeline.stages.model.DiscoverCatalogPayload
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.pods.KubePodClient
import io.mockk.Called
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class DispatchStageTest {
  private val directJobRunnerClient = mockk<DirectJobRunnerClient>(relaxed = true)
  private val directCheckRouter =
    mockk<DirectCheckRouter>(relaxed = true) {
      every { eligibility(any()) } returns DirectCheckEligibility.WORKSPACE_FLAG_OFF
    }

  @Test
  fun `resolves sync and reset before creating their pods`() {
    for (reset in listOf(false, true)) {
      val launcher = mockk<KubePodClient>(relaxed = true)
      val resolver = mockk<FusionLaunchResolver>(relaxed = true)
      val payload = SyncPayload(ReplicationInput().withIsReset(reset))
      val msg = RecordFixtures.launcherInput("workload")
      DispatchStage(launcher, mockk(), resolver, directJobRunnerClient, directCheckRouter).applyStage(LaunchStageIO(msg, payload = payload))
      verifyOrder {
        resolver.resolve(payload, "workload")
        if (reset) launcher.launchReset(payload, msg) else launcher.launchReplication(payload, msg)
      }
    }
  }

  @Test
  fun `resolver failure prevents sync pod creation`() {
    val launcher = mockk<KubePodClient>(relaxed = true)
    val resolver = mockk<FusionLaunchResolver>()
    every { resolver.resolve(any(), any()) } throws IllegalStateException("Config API failed")
    assertThrows<IllegalStateException> {
      DispatchStage(launcher, mockk(), resolver, directJobRunnerClient, directCheckRouter)
        .applyStage(LaunchStageIO(RecordFixtures.launcherInput("workload"), payload = SyncPayload(ReplicationInput())))
    }
    verify(exactly = 0) {
      launcher.launchReplication(any(), any())
      launcher.launchReset(any(), any())
    }
  }

  @Test
  fun `launches replication`() {
    val replInput = ReplicationInput()
    val payload = SyncPayload(replInput)

    val launcher: KubePodClient = mockk()
    every { launcher.launchReplication(any(), any()) } returns Unit

    val stage = DispatchStage(launcher, mockk(), mockk(relaxed = true), directJobRunnerClient, directCheckRouter)
    val workloadId = UUID.randomUUID().toString()
    val msg = RecordFixtures.launcherInput(workloadId)
    val io = LaunchStageIO(msg = msg, payload = payload)

    val result = stage.applyStage(io)

    verify {
      launcher.launchReplication(payload, msg)
    }

    assertEquals(payload, result.payload)
  }

  @Test
  fun `launches reset`() {
    val replInput = ReplicationInput().withIsReset(true)
    val payload = SyncPayload(replInput)

    val launcher: KubePodClient = mockk()
    every { launcher.launchReset(any(), any()) } returns Unit

    val stage = DispatchStage(launcher, mockk(), mockk(relaxed = true), directJobRunnerClient, directCheckRouter)
    val workloadId = UUID.randomUUID().toString()
    val msg = RecordFixtures.launcherInput(workloadId)
    val io = LaunchStageIO(msg = msg, payload = payload)

    val result = stage.applyStage(io)

    verify {
      launcher.launchReset(payload, msg)
    }

    assertEquals(payload, result.payload)
  }

  @Test
  fun `launches check`() {
    val launcherConfig: IntegrationLauncherConfig = mockk()
    val checkConnectionInput: StandardCheckConnectionInput = mockk()
    val checkInput =
      CheckConnectionInput(
        jobRunConfig = JobRunConfig(),
        launcherConfig = launcherConfig,
        checkConnectionInput = checkConnectionInput,
      )
    val payload = CheckPayload(checkInput)

    val launcher: KubePodClient = mockk()
    every { launcher.launchCheck(any(), any()) } returns Unit

    val stage = DispatchStage(launcher, mockk(relaxed = true), mockk(relaxed = true), directJobRunnerClient, directCheckRouter)
    val workloadId = UUID.randomUUID().toString()
    val msg = RecordFixtures.launcherInput(workloadId)
    val io = LaunchStageIO(msg = msg, payload = payload)

    val result = stage.applyStage(io)

    verify {
      launcher.launchCheck(checkInput, msg)
    }

    assertEquals(payload, result.payload)
  }

  @Test
  fun `launches discover`() {
    val discoverInput =
      DiscoverCatalogInput(
        jobRunConfig = mockk(),
        launcherConfig = mockk(),
        discoverCatalogInput = mockk(),
      )
    val payload = DiscoverCatalogPayload(discoverInput)

    val launcher: KubePodClient = mockk()
    every { launcher.launchDiscover(any(), any()) } returns Unit

    val stage = DispatchStage(launcher, mockk(), mockk(relaxed = true), directJobRunnerClient, directCheckRouter)
    val workloadId = UUID.randomUUID().toString()
    val msg = RecordFixtures.launcherInput(workloadId)
    val io = LaunchStageIO(msg = msg, payload = payload)

    val result = stage.applyStage(io)

    verify {
      launcher.launchDiscover(discoverInput, msg)
    }

    assertEquals(payload, result.payload)
  }

  @Test
  fun `hands connect workloads to the direct job runner without launching a pod`() {
    val launcher: KubePodClient = mockk()
    val stage = DispatchStage(launcher, mockk(), mockk(), directJobRunnerClient, directCheckRouter)
    val connectInput =
      ConnectInput(host = "db.example.com", port = 5432, workspaceId = UUID.randomUUID(), organizationId = UUID.randomUUID())
    val msg = RecordFixtures.launcherInput(workloadType = WorkloadType.CONNECT)
    val payload = ConnectPayload(connectInput)

    val result = stage.applyStage(LaunchStageIO(msg = msg, payload = payload))

    verify(exactly = 1) { directJobRunnerClient.submit(msg) }
    verify { launcher wasNot Called }
    assertEquals(payload, result.payload)
  }

  @Test
  fun `a direct job runner failure propagates`() {
    val launcher: KubePodClient = mockk()
    every { directJobRunnerClient.submit(any()) } throws WorkloadLauncherException("Direct job runner unavailable in this data plane")
    val stage = DispatchStage(launcher, mockk(), mockk(), directJobRunnerClient, directCheckRouter)
    val connectInput =
      ConnectInput(host = "db.example.com", port = 5432, workspaceId = UUID.randomUUID(), organizationId = UUID.randomUUID())
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadType = WorkloadType.CONNECT), payload = ConnectPayload(connectInput))

    val error = assertThrows<WorkloadLauncherException> { stage.applyStage(io) }

    assertEquals("Direct job runner unavailable in this data plane", error.message)
    verify { launcher wasNot Called }
  }

  @Test
  fun `pod workloads never reach the direct job runner`() {
    val launcher: KubePodClient = mockk(relaxed = true)
    val stage = DispatchStage(launcher, mockk(), mockk(relaxed = true), directJobRunnerClient, directCheckRouter)

    stage.applyStage(LaunchStageIO(msg = RecordFixtures.launcherInput(), payload = SyncPayload(ReplicationInput())))

    verify { directJobRunnerClient wasNot Called }
  }

  @Test
  fun `an eligible check the runner takes gets no pod`() {
    val launcher: KubePodClient = mockk(relaxed = true)
    val metricClient = mockk<MetricClient>(relaxed = true)
    val checkInput = CheckConnectionInput(jobRunConfig = JobRunConfig(), launcherConfig = mockk(), checkConnectionInput = mockk())
    val msg = RecordFixtures.launcherInput(workloadType = WorkloadType.CHECK)
    every { directCheckRouter.eligibility(checkInput) } returns DirectCheckEligibility.ELIGIBLE
    every { directCheckRouter.dispatch(msg) } returns true

    DispatchStage(launcher, metricClient, mockk(relaxed = true), directJobRunnerClient, directCheckRouter)
      .applyStage(LaunchStageIO(msg = msg, payload = CheckPayload(checkInput)))

    verify { launcher wasNot Called }
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.DIRECT_CHECK_ROUTE,
        1,
        MetricAttribute("destination", "runner"),
        MetricAttribute("reason", "eligible"),
      )
    }
  }

  @Test
  fun `an eligible check the runner doesn't take launches its pod`() {
    val launcher: KubePodClient = mockk(relaxed = true)
    val metricClient = mockk<MetricClient>(relaxed = true)
    val checkInput = CheckConnectionInput(jobRunConfig = JobRunConfig(), launcherConfig = mockk(), checkConnectionInput = mockk())
    val msg = RecordFixtures.launcherInput(workloadType = WorkloadType.CHECK)
    every { directCheckRouter.eligibility(checkInput) } returns DirectCheckEligibility.ELIGIBLE
    every { directCheckRouter.dispatch(msg) } returns false

    DispatchStage(launcher, metricClient, mockk(relaxed = true), directJobRunnerClient, directCheckRouter)
      .applyStage(LaunchStageIO(msg = msg, payload = CheckPayload(checkInput)))

    verify(exactly = 1) { launcher.launchCheck(checkInput, msg) }
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.DIRECT_CHECK_ROUTE,
        1,
        MetricAttribute("destination", "pod"),
        MetricAttribute("reason", "runner_rejected"),
      )
    }
  }

  @Test
  fun `a check that isn't eligible launches its pod without trying the runner`() {
    val launcher: KubePodClient = mockk(relaxed = true)
    val metricClient = mockk<MetricClient>(relaxed = true)
    val checkInput = CheckConnectionInput(jobRunConfig = JobRunConfig(), launcherConfig = mockk(), checkConnectionInput = mockk())
    val msg = RecordFixtures.launcherInput(workloadType = WorkloadType.CHECK)
    every { directCheckRouter.eligibility(checkInput) } returns DirectCheckEligibility.WORKSPACE_FLAG_OFF

    DispatchStage(launcher, metricClient, mockk(relaxed = true), directJobRunnerClient, directCheckRouter)
      .applyStage(LaunchStageIO(msg = msg, payload = CheckPayload(checkInput)))

    verify(exactly = 0) { directCheckRouter.dispatch(any()) }
    verify(exactly = 1) { launcher.launchCheck(checkInput, msg) }
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.DIRECT_CHECK_ROUTE,
        1,
        MetricAttribute("destination", "pod"),
        MetricAttribute("reason", "workspace_flag_off"),
      )
    }
  }
}
