package io.airbyte.workers.temporal.workflow

import io.airbyte.config.CatalogDiff
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.PostprocessCatalogInput
import io.airbyte.workers.models.PostprocessCatalogOutput
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogActivity
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogHelperActivity
import io.airbyte.workers.temporal.workflows.DiscoverCatalogAndAutoPropagateWorkflowImpl
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class DiscoverCatalogAndAutoPropagateWorkflowTest {
  val discoverCatalogId = UUID.randomUUID()
  val connectionId = UUID.randomUUID()
  val workspaceId = UUID.randomUUID()
  val catalogDiff = CatalogDiff().withAdditionalProperty("test", "value")

  val activity: DiscoverCatalogActivity = mockk()
  val activityReport: DiscoverCatalogHelperActivity = mockk()
  val jobRunConfig =
    JobRunConfig()
      .withJobId("1")
      .withAttemptId(1)
  val launcherConfig =
    IntegrationLauncherConfig()
      .withWorkspaceId(workspaceId)
      .withConnectionId(connectionId)
      .withDockerImage("image")
  val config = StandardDiscoverCatalogInput()

  @Test
  fun `Test success`() {
    val discoverCatalogAndAutoPropagateWorkflow = DiscoverCatalogAndAutoPropagateWorkflowImpl(activity, activityReport)

    every {
      activity.runWithWorkload(
        DiscoverCatalogInput(
          jobRunConfig,
          launcherConfig,
          config,
        ),
      )
    } returns ConnectorJobOutput().withDiscoverCatalogId(discoverCatalogId)
    every {
      activityReport.postprocess(
        PostprocessCatalogInput(
          discoverCatalogId,
          connectionId,
        ),
      )
    } returns PostprocessCatalogOutput.success(catalogDiff)
    every { activityReport.reportSuccess() } returns Unit

    val result = discoverCatalogAndAutoPropagateWorkflow.run(jobRunConfig, launcherConfig, config)

    verify { activityReport.reportSuccess() }
    assertEquals(catalogDiff, result.appliedDiff)
  }

  @Test
  fun `Test failure discover`() {
    val discoverCatalogAndAutoPropagateWorkflow = DiscoverCatalogAndAutoPropagateWorkflowImpl(activity, activityReport)

    every {
      activity.runWithWorkload(
        DiscoverCatalogInput(
          jobRunConfig,
          launcherConfig,
          config,
        ),
      )
    } returns ConnectorJobOutput().withDiscoverCatalogId(null)
    every { activityReport.reportFailure() } returns Unit

    val result = discoverCatalogAndAutoPropagateWorkflow.run(jobRunConfig, launcherConfig, config)

    verify { activityReport.reportFailure() }
    assertEquals(null, result.appliedDiff)
  }

  @Test
  fun `Test failure post process`() {
    val discoverCatalogAndAutoPropagateWorkflow = DiscoverCatalogAndAutoPropagateWorkflowImpl(activity, activityReport)

    every {
      activity.runWithWorkload(
        DiscoverCatalogInput(
          jobRunConfig,
          launcherConfig,
          config,
        ),
      )
    } returns ConnectorJobOutput().withDiscoverCatalogId(discoverCatalogId)
    every {
      activityReport.postprocess(
        PostprocessCatalogInput(
          discoverCatalogId,
          connectionId,
        ),
      )
    } returns PostprocessCatalogOutput.failure(RuntimeException())
    every { activityReport.reportFailure() } returns Unit

    val result = discoverCatalogAndAutoPropagateWorkflow.run(jobRunConfig, launcherConfig, config)

    verify { activityReport.reportFailure() }
    assertEquals(null, result.appliedDiff)
  }
}
