/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.dispatch

import fixtures.RecordFixtures
import io.airbyte.config.WorkloadType
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.micronaut.runtime.AirbyteWorkloadLauncherConfig
import io.airbyte.micronaut.runtime.AirbyteWorkloadLauncherConfig.AirbyteWorkloadLauncherDirectJobRunnerConfig
import io.airbyte.workers.exception.WorkloadLauncherException
import io.mockk.mockk
import io.mockk.verify
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.net.ServerSocket
import java.util.concurrent.TimeUnit

class DirectJobRunnerClientTest {
  private val workloadId = "workload-1"
  private val msg = RecordFixtures.launcherInput(workloadId, workloadType = WorkloadType.CONNECT)
  private val metricClient = mockk<MetricClient>(relaxed = true)
  private val server = MockWebServer()

  @BeforeEach
  fun setUp() {
    server.start()
  }

  @AfterEach
  fun tearDown() {
    server.shutdown()
  }

  private fun client(url: String = server.url("/").toString()) =
    DirectJobRunnerClient(
      OkHttpClient
        .Builder()
        .connectTimeout(1, TimeUnit.SECONDS)
        .readTimeout(2, TimeUnit.SECONDS)
        .build(),
      AirbyteWorkloadLauncherConfig(directJobRunner = AirbyteWorkloadLauncherDirectJobRunnerConfig(url = url)),
      metricClient,
    )

  private fun verifyResult(result: String) =
    verify {
      metricClient.count(
        OssMetricsRegistry.DIRECT_JOB_DISPATCH,
        1,
        MetricAttribute("workload_type", "connect"),
        MetricAttribute("result", result),
      )
    }

  @Test
  fun `202 is accepted and posts the workload id to the jobs endpoint`() {
    server.enqueue(MockResponse().setResponseCode(202))

    client().submit(msg)

    val request = server.takeRequest()
    assertEquals("POST", request.method)
    assertEquals("/v1/jobs", request.path)
    assertEquals("""{"workloadId":"$workloadId"}""", request.body.readUtf8())
    verifyResult("accepted")
  }

  @Test
  fun `a trailing slash on the url is ignored`() {
    server.enqueue(MockResponse().setResponseCode(202))

    client(server.url("/").toString().trimEnd('/') + "/").submit(msg)

    assertEquals("/v1/jobs", server.takeRequest().path)
  }

  @Test
  fun `409 is treated as already handled`() {
    server.enqueue(MockResponse().setResponseCode(409))

    client().submit(msg)

    assertEquals(1, server.requestCount)
    verifyResult("duplicate")
  }

  @Test
  fun `5xx is retried until accepted`() {
    server.enqueue(MockResponse().setResponseCode(503))
    server.enqueue(MockResponse().setResponseCode(202))

    client().submit(msg)

    assertEquals(2, server.requestCount)
    verifyResult("accepted")
  }

  @Test
  fun `exhausted 5xx retries fail with a user safe message`() {
    repeat(4) { server.enqueue(MockResponse().setResponseCode(503)) }

    val e = assertThrows<WorkloadLauncherException> { client().submit(msg) }

    assertEquals(DIRECT_JOB_RUNNER_UNAVAILABLE_MESSAGE, e.message)
    assertEquals(4, server.requestCount)
    verifyResult("failed")
  }

  @Test
  fun `400 fails without retrying`() {
    server.enqueue(MockResponse().setResponseCode(400))

    val e = assertThrows<WorkloadLauncherException> { client().submit(msg) }

    assertEquals(DIRECT_JOB_RUNNER_UNAVAILABLE_MESSAGE, e.message)
    assertEquals(1, server.requestCount)
    verifyResult("failed")
  }

  @Test
  fun `429 is not retried`() {
    server.enqueue(MockResponse().setResponseCode(429))

    assertThrows<WorkloadLauncherException> { client().submit(msg) }

    assertEquals(1, server.requestCount)
    verifyResult("failed")
  }

  @Test
  fun `connection errors fail with a user safe message`() {
    val unusedPort = ServerSocket(0).use { it.localPort }

    val e = assertThrows<WorkloadLauncherException> { client("http://localhost:$unusedPort").submit(msg) }

    assertEquals(DIRECT_JOB_RUNNER_UNAVAILABLE_MESSAGE, e.message)
    verifyResult("failed")
  }

  @Test
  fun `blank url fails without a request`() {
    val e = assertThrows<WorkloadLauncherException> { client("").submit(msg) }

    assertEquals(DIRECT_JOB_RUNNER_UNAVAILABLE_MESSAGE, e.message)
    assertEquals(0, server.requestCount)
    verifyResult("failed")
  }
}
