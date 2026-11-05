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
import io.airbyte.directjobrunner.connector.DirectCheckRunner
import io.airbyte.directjobrunner.connector.SourceCheckResult
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

class CheckJobHandlerTest {
  private val workloadId = "workload-1"
  private val hydratedConfig = Jsons.deserialize("""{"host":"db.example.com","password":"hydrated"}""")
  private val secretsHydrator = mockk<ConnectorSecretsHydrator> { every { hydrateConfig(any(), any()) } returns hydratedConfig }
  private val directCheckRunner =
    mockk<DirectCheckRunner> {
      every { image } returns BAKED_IMAGE
      every { checkTimeout } returns Duration.ofSeconds(60)
    }
  private val outputWriter = mockk<WorkloadOutputWriter>(relaxed = true)
  private val metricClient = mockk<MetricClient>(relaxed = true)
  private val handler =
    CheckJobHandler(PayloadDeserializer(), secretsHydrator, directCheckRunner, outputWriter, metricClient)

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

  private fun runAndCaptureOutput(result: SourceCheckResult): ConnectorJobOutput {
    every { directCheckRunner.check(hydratedConfig, workloadId) } returns result
    val output = slot<ConnectorJobOutput>()
    every { outputWriter.write(workloadId, capture(output)) } returns Unit

    handler.run(workloadId, payload(), "/logs/workload-1.log")

    return output.captured
  }

  @Test
  fun `accepts only the image baked into the runner`() {
    assertTrue(handler.accepts(Workload(inputPayload = payload(BAKED_IMAGE))))
    assertFalse(handler.accepts(Workload(inputPayload = payload("airbyte/source-postgres:3.8.9"))))
    assertFalse(handler.accepts(Workload(inputPayload = payload("airbyte/source-mysql:3.8.10"))))
  }

  @Test
  fun `rejects FLEX log delivery, which only the check pod's sidecar can upload`() {
    assertFalse(handler.accepts(Workload(inputPayload = payload(), logDeliveryMode = LogDeliveryMode.FLEX)))
    assertTrue(handler.accepts(Workload(inputPayload = payload(), logDeliveryMode = LogDeliveryMode.STANDARD)))
  }

  @Test
  fun `the running deadline is the connector timeout plus time to write the result`() {
    assertEquals(Duration.ofSeconds(90), handler.runningDeadline)
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
        any(),
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
      directCheckRunner.check(any(), any())
      outputWriter.write(any(), any())
    }
  }
}
