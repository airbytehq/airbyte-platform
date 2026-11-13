/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.check

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorContext
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.directjobrunner.connector.ConnectorJarPrefetcher
import io.airbyte.directjobrunner.connector.DirectCheckAllowlist
import io.airbyte.directjobrunner.connector.DirectCheckRun
import io.airbyte.directjobrunner.connector.DirectCheckRunner
import io.airbyte.directjobrunner.connector.JarSource
import io.airbyte.directjobrunner.connector.SourceCheckResult
import io.airbyte.directjobrunner.core.LauncherDataplaneGroup
import io.airbyte.featureflag.DataplaneGroup
import io.airbyte.featureflag.DirectCheckImages
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

private const val BAKED_IMAGE = "airbyte/source-postgres:3.8.10"
private val DATAPLANE_GROUP_ID: UUID = UUID.randomUUID()
private val DISPATCHED_FROM = LauncherDataplaneGroup.Known(DATAPLANE_GROUP_ID)

class CheckJobHandlerTest {
  private val workloadId = "workload-1"
  private val hydratedConfig = Jsons.deserialize("""{"host":"db.example.com","password":"hydrated"}""")
  private val secretsHydrator = mockk<ConnectorSecretsHydrator> { every { hydrateConfig(any(), any()) } returns hydratedConfig }
  private val directCheckRunner =
    mockk<DirectCheckRunner> {
      every { canRun(any()) } returns true
      every { checkTimeout } returns Duration.ofSeconds(60)
      every { jarDownloadAllowance } returns Duration.ofSeconds(120)
    }
  private val prefetcher = mockk<ConnectorJarPrefetcher>(relaxed = true)
  private val outputWriter = mockk<WorkloadOutputWriter>(relaxed = true)
  private val metricClient = mockk<MetricClient>(relaxed = true)
  private val handler =
    CheckJobHandler(
      PayloadDeserializer(),
      secretsHydrator,
      directCheckRunner,
      DirectCheckAllowlist(mockk { every { stringVariation(DirectCheckImages, DataplaneGroup(DATAPLANE_GROUP_ID)) } returns "source-postgres" }),
      prefetcher,
      outputWriter,
      metricClient,
    )

  private fun payload(image: String = BAKED_IMAGE) =
    Jsons.serialize(
      CheckConnectionInput(
        jobRunConfig = JobRunConfig(),
        launcherConfig = IntegrationLauncherConfig().withDockerImage(image),
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(ActorType.SOURCE)
            .withConnectionConfiguration(Jsons.deserialize("""{"host":"db.example.com"}"""))
            .withActorContext(ActorContext().withOrganizationId(UUID.randomUUID()).withWorkspaceId(UUID.randomUUID())),
      ),
    )

  private fun runAndCaptureOutput(
    result: SourceCheckResult,
    processMs: Long = 2_000,
  ): ConnectorJobOutput {
    every { directCheckRunner.check(BAKED_IMAGE, hydratedConfig, workloadId) } returns
      DirectCheckRun(result, JarSource.DOWNLOADED, jarMs = 5_000, processMs = processMs)
    val output = slot<ConnectorJobOutput>()
    every { outputWriter.write(workloadId, capture(output)) } returns Unit

    handler.run(workloadId, payload(), "/logs/workload-1.log")

    return output.captured
  }

  @Test
  fun `accepts any version of an allowlisted connector, and doesn't look up the jar for other connectors`() {
    assertTrue(handler.accepts(Workload(inputPayload = payload(BAKED_IMAGE)), DISPATCHED_FROM))
    assertTrue(handler.accepts(Workload(inputPayload = payload("airbyte/source-postgres:3.8.9")), DISPATCHED_FROM))
    assertFalse(handler.accepts(Workload(inputPayload = payload("airbyte/source-mysql:3.8.10")), DISPATCHED_FROM))

    verify(exactly = 1) { directCheckRunner.canRun(BAKED_IMAGE) }
    verify(exactly = 1) { directCheckRunner.canRun("airbyte/source-postgres:3.8.9") }
    verify(exactly = 0) { directCheckRunner.canRun("airbyte/source-mysql:3.8.10") }
  }

  @Test
  fun `turns down an allowlisted image whose jar isn't in the jar repository, so it runs in a pod`() {
    every { directCheckRunner.canRun(BAKED_IMAGE) } returns false

    assertFalse(handler.accepts(Workload(inputPayload = payload(BAKED_IMAGE)), DISPATCHED_FROM))
  }

  @Test
  fun `turns down every check from a launcher that sent no data plane group`() {
    assertFalse(handler.accepts(Workload(inputPayload = payload(BAKED_IMAGE)), LauncherDataplaneGroup.Unknown))

    verify(exactly = 0) { prefetcher.prefetchOnce(any()) }
  }

  @Test
  fun `a check from a known data plane group starts that group's prefetch`() {
    handler.accepts(Workload(inputPayload = payload(BAKED_IMAGE)), DISPATCHED_FROM)

    verify(exactly = 1) { prefetcher.prefetchOnce(DATAPLANE_GROUP_ID) }
  }

  @Test
  fun `rejects FLEX log delivery, which only the check pod's sidecar can upload`() {
    assertFalse(handler.accepts(Workload(inputPayload = payload(), logDeliveryMode = LogDeliveryMode.FLEX), DISPATCHED_FROM))
    assertTrue(handler.accepts(Workload(inputPayload = payload(), logDeliveryMode = LogDeliveryMode.STANDARD), DISPATCHED_FROM))
  }

  @Test
  fun `the running deadline covers a jar download, the connector timeout and writing the result`() {
    assertEquals(Duration.ofSeconds(210), handler.runningDeadline)
  }

  @Test
  fun `a reported status is written as the check result`() {
    val output = runAndCaptureOutput(SourceCheckResult.Reported(AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.SUCCEEDED)))

    assertEquals(ConnectorJobOutput.OutputType.CHECK_CONNECTION, output.outputType)
    assertEquals(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED), output.checkConnection)
    assertEquals(null, output.failureReason)
  }

  @Test
  fun `a reported failure is a normal FAILED result, not a failure reason`() {
    val output =
      runAndCaptureOutput(
        SourceCheckResult.Reported(AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.FAILED).withMessage("bad password")),
      )

    assertEquals(
      StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.FAILED).withMessage("bad password"),
      output.checkConnection,
    )
    assertEquals(null, output.failureReason)
  }

  @Test
  fun `a connector error trace fails the check with the connector's message, like the sidecar`() {
    val output = runAndCaptureOutput(SourceCheckResult.ConnectorError(AirbyteErrorTraceMessage().withMessage("no route to host")))

    assertEquals(
      StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.FAILED).withMessage("no route to host"),
      output.checkConnection,
    )
    // The sidecar builds this with FailureHelper.connectorCommandFailure too, so the reason has the same shape.
    assertEquals("no route to host", output.failureReason.externalMessage)
    assertEquals("check", output.failureReason.metadata.additionalProperties["connector_command"])
  }

  @Test
  fun `a crash and a timeout are system errors from the source`() {
    val crashed = runAndCaptureOutput(SourceCheckResult.Crashed(137))
    assertEquals(FailureReason.FailureType.SYSTEM_ERROR, crashed.failureReason.failureType)
    assertEquals(FailureReason.FailureOrigin.SOURCE, crashed.failureReason.failureOrigin)
    assertEquals("The connector running check exited with 137 exit code", crashed.checkConnection.message)

    val timedOut = runAndCaptureOutput(SourceCheckResult.TimedOut)
    assertEquals(FailureReason.FailureType.SYSTEM_ERROR, timedOut.failureReason.failureType)
    assertEquals("The connector check timed out", timedOut.checkConnection.message)
  }

  @Test
  fun `the connector's run time is recorded by image and result`() {
    runAndCaptureOutput(SourceCheckResult.TimedOut)

    verify(exactly = 1) {
      metricClient.distribution(
        OssMetricsRegistry.DIRECT_CHECK_CONNECTOR_DURATION,
        2_000.0,
        MetricAttribute("connector_image", BAKED_IMAGE),
        MetricAttribute("result", "TimedOut"),
      )
    }
  }

  @Test
  fun `a hydration failure propagates as a platform error and nothing is written`() {
    every { secretsHydrator.hydrateConfig(any(), any()) } throws IllegalStateException("secret store down")

    assertThrows<IllegalStateException> { handler.run(workloadId, payload(), "/logs/workload-1.log") }

    verify(exactly = 0) {
      directCheckRunner.check(any(), any(), any())
      outputWriter.write(any(), any())
    }
  }
}
