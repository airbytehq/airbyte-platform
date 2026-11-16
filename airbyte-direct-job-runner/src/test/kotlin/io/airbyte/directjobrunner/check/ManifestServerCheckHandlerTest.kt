/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.check

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorContext
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.directjobrunner.connector.ManifestServerCheckRunner
import io.airbyte.directjobrunner.connector.SourceCheckResult
import io.airbyte.directjobrunner.core.LauncherDataplaneGroup
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.airbyte.protocol.models.v0.AirbyteErrorTraceMessage
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workers.workload.WorkloadOutputWriter
import io.airbyte.workload.api.domain.LogDeliveryMode
import io.airbyte.workload.api.domain.Workload
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.util.UUID

private const val MANIFEST_ONLY_IMAGE = "airbyte/source-sendgrid:1.3.51"
private val DISPATCHED_FROM = LauncherDataplaneGroup.Known(UUID.randomUUID())

class ManifestServerCheckHandlerTest {
  private val workloadId = "workload-1"
  private val hydratedConfig = Jsons.deserialize("""{"api_key":"hydrated"}""")
  private val secretsHydrator = mockk<ConnectorSecretsHydrator> { every { hydrateConfig(any(), any()) } returns hydratedConfig }
  private val manifestServer =
    mockk<ManifestServerCheckRunner> {
      every { runs(any()) } returns false
      every { runs(MANIFEST_ONLY_IMAGE) } returns true
      every { checkTimeout } returns Duration.ofSeconds(70)
    }
  private val outputWriter = mockk<WorkloadOutputWriter>(relaxed = true)
  private val metricClient = mockk<MetricClient>(relaxed = true)
  private val handler = ManifestServerCheckHandler(PayloadDeserializer(), secretsHydrator, manifestServer, outputWriter, metricClient)

  private fun payload(image: String = MANIFEST_ONLY_IMAGE) =
    Jsons.serialize(
      CheckConnectionInput(
        jobRunConfig = JobRunConfig(),
        launcherConfig = IntegrationLauncherConfig().withDockerImage(image),
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(ActorType.SOURCE)
            .withConnectionConfiguration(Jsons.deserialize("""{"api_key":"secret-ref"}"""))
            .withActorContext(ActorContext().withOrganizationId(UUID.randomUUID()).withWorkspaceId(UUID.randomUUID())),
      ),
    )

  private fun runAndCaptureOutput(result: SourceCheckResult): ConnectorJobOutput {
    every { manifestServer.check(MANIFEST_ONLY_IMAGE, hydratedConfig, workloadId) } returns result
    val output = slot<ConnectorJobOutput>()
    every { outputWriter.write(workloadId, capture(output)) } returns Unit

    handler.run(workloadId, payload(), "/logs/workload-1.log")

    return output.captured
  }

  @Test
  fun `accepts only images the manifest server runs, from any launcher`() {
    assertTrue(handler.accepts(Workload(inputPayload = payload()), DISPATCHED_FROM))
    assertTrue(handler.accepts(Workload(inputPayload = payload()), LauncherDataplaneGroup.Unknown))
    assertFalse(handler.accepts(Workload(inputPayload = payload("airbyte/source-postgres:3.8.10")), DISPATCHED_FROM))
  }

  @Test
  fun `rejects FLEX log delivery without asking the manifest server`() {
    assertFalse(handler.accepts(Workload(inputPayload = payload(), logDeliveryMode = LogDeliveryMode.FLEX), DISPATCHED_FROM))

    verify(exactly = 0) { manifestServer.runs(any()) }
  }

  @Test
  fun `the running deadline is the manifest server call's timeout plus time to write the result`() {
    assertEquals(Duration.ofSeconds(100), handler.runningDeadline)
  }

  @Test
  fun `the check runs on the manifest server with the hydrated config, and a failure is a normal FAILED result`() {
    val output =
      runAndCaptureOutput(
        SourceCheckResult.Reported(AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.FAILED).withMessage("bad token")),
      )

    assertEquals(
      StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.FAILED).withMessage("bad token"),
      output.checkConnection,
    )
    assertEquals(null, output.failureReason, "$output")
  }

  @Test
  fun `a connector error from the manifest server fails the check with its message, like the sidecar`() {
    val output = runAndCaptureOutput(SourceCheckResult.ConnectorError(AirbyteErrorTraceMessage().withMessage("Invalid manifest")))

    assertEquals(
      StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.FAILED).withMessage("Invalid manifest"),
      output.checkConnection,
    )
    assertEquals("Invalid manifest", output.failureReason.externalMessage, "$output")
    assertEquals("check", output.failureReason.metadata.additionalProperties["connector_command"], "$output")
  }

  @Test
  fun `the manifest server's time is recorded by image and result`() {
    runAndCaptureOutput(SourceCheckResult.TimedOut)

    verify(exactly = 1) {
      metricClient.distribution(
        OssMetricsRegistry.DIRECT_CHECK_CONNECTOR_DURATION,
        any(),
        MetricAttribute("connector_image", MANIFEST_ONLY_IMAGE),
        MetricAttribute("result", "TimedOut"),
      )
    }
  }

  @Test
  fun `a manifest server failure propagates as a platform error and nothing is written`() {
    every { manifestServer.check(MANIFEST_ONLY_IMAGE, hydratedConfig, workloadId) } throws
      IllegalStateException("The manifest server refused the runner's check call with 401")

    assertThrows<IllegalStateException> { handler.run(workloadId, payload(), "/logs/workload-1.log") }
    verify(exactly = 0) { outputWriter.write(any(), any()) }
  }
}
