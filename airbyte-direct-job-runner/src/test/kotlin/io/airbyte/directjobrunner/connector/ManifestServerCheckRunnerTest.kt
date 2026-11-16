/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.version.Version
import io.airbyte.directjobrunner.config.ManifestServerCheckConfig
import io.airbyte.directjobrunner.manifest.ManifestOnlyConnectorRegistry
import io.airbyte.directjobrunner.manifest.RegistryConnector
import io.airbyte.manifestserver.api.client.ManifestServerApiClient
import io.airbyte.manifestserver.api.client.generated.HealthApi
import io.airbyte.manifestserver.api.client.model.generated.CheckRequest
import io.airbyte.manifestserver.api.client.model.generated.CheckResponse
import io.airbyte.micronaut.runtime.AirbyteManifestServerApiClientConfig
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.airbyte.protocol.models.v0.AirbyteErrorTraceMessage
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openapitools.client.infrastructure.ClientException
import org.openapitools.client.infrastructure.ServerException
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration

private const val IMAGE = "airbyte/source-sendgrid:1.3.50"

class ManifestServerCheckRunnerTest {
  private val manifest = Jsons.deserialize("""{"version":"6.7.0","check":{"type":"CheckStream","stream_names":["bounces"]}}""")
  private val config = Jsons.deserialize("""{"api_key":"hydrated","start_date":"2024-01-01T00:00:00Z"}""")

  private val registry =
    mockk<ManifestOnlyConnectorRegistry> { every { lookup(IMAGE) } returns RegistryConnector.ManifestOnly(Version("7.45.0"), manifest) }
  private val client = mockk<ManifestServerApiClient>()
  private val healthApi = mockk<HealthApi> { every { healthHealthGet() } returns mapOf("status" to "ok") }

  private fun runner(
    serverCdkVersion: String = "7.45.0",
    signatureSecret: String = "jwt-secret",
    healthCacheTtl: Duration = Duration.ofSeconds(5),
  ) = ManifestServerCheckRunner(
    client,
    healthApi,
    registry,
    ManifestServerCheckConfig(serverCdkVersion = serverCdkVersion, healthCacheTtl = healthCacheTtl),
    AirbyteManifestServerApiClientConfig(connectTimeoutSeconds = 10, readTimeoutSeconds = 60, signatureSecret = signatureSecret),
  )

  @Test
  fun `the check timeout is the HTTP client's connect and read timeouts`() {
    assertEquals(Duration.ofSeconds(70), runner().checkTimeout)
  }

  @Test
  fun `runs a manifest-only connector on the server's CDK version`() {
    assertTrue(runner().runs(IMAGE), "$IMAGE on CDK 7.45.0 should run on a 7.45.0 server")
  }

  @Test
  fun `runs a manifest-only connector on an older CDK of the server's major version`() {
    assertTrue(runner(serverCdkVersion = "7.47.0").runs(IMAGE), "$IMAGE on CDK 7.45.0 should run on a 7.47.0 server")
  }

  @Test
  fun `doesn't run a connector on a newer CDK than the server`() {
    assertFalse(runner(serverCdkVersion = "7.28.2").runs(IMAGE), "$IMAGE on CDK 7.45.0 shouldn't run on an older 7.28.2 server")
  }

  @Test
  fun `doesn't run a connector on another major CDK version`() {
    every { registry.lookup(IMAGE) } returns RegistryConnector.ManifestOnly(Version("6.61.6"), manifest)

    assertFalse(runner().runs(IMAGE), "$IMAGE on CDK 6.61.6 shouldn't run on a 7.45.0 server")
    assertFalse(runner(serverCdkVersion = "8.0.0").runs(IMAGE), "$IMAGE on CDK 6.61.6 shouldn't run on an 8.0.0 server")
  }

  @Test
  fun `runs nothing without a server CDK version`() {
    assertFalse(runner(serverCdkVersion = "").runs(IMAGE), "a blank server CDK version should run nothing")
    assertFalse(runner(serverCdkVersion = "latest").runs(IMAGE), "a server CDK version that isn't a release (latest) should run nothing")
    verify(exactly = 0) { registry.lookup(any()) }
  }

  @Test
  fun `runs nothing without the JWT signature secret the server checks calls against`() {
    assertFalse(runner(signatureSecret = "").runs(IMAGE), "an empty signature secret should run nothing")
    assertFalse(runner(signatureSecret = " ").runs(IMAGE), "a blank signature secret should run nothing")
    verify(exactly = 0) { registry.lookup(any()) }
  }

  @Test
  fun `doesn't run a connector the registry doesn't have as manifest-only`() {
    every { registry.lookup(IMAGE) } returns RegistryConnector.NotManifestOnly("it has custom Python components")

    assertFalse(runner().runs(IMAGE), "$IMAGE isn't manifest-only in the registry, so it shouldn't run")
  }

  @Test
  fun `runs nothing while the registry can't be reached`() {
    every { registry.lookup(IMAGE) } throws IOException("connection refused")

    assertFalse(runner().runs(IMAGE), "$IMAGE shouldn't run while the registry is unreachable")
  }

  @Test
  fun `runs nothing while the manifest server is down or failing`() {
    every { healthApi.healthHealthGet() } throws IOException("connection refused")
    assertFalse(runner().runs(IMAGE), "$IMAGE shouldn't run while the manifest server is unreachable")

    every { healthApi.healthHealthGet() } throws ServerException("Server error : 503", 503)
    assertFalse(runner().runs(IMAGE), "$IMAGE shouldn't run while the manifest server answers 503")
  }

  @Test
  fun `one health answer serves the checks right after it`() {
    val runner = runner()
    assertTrue(runner.runs(IMAGE), "$IMAGE should run on a healthy server (first check)")
    assertTrue(runner.runs(IMAGE), "$IMAGE should run on the cached healthy answer (second check)")
    verify(exactly = 1) { healthApi.healthHealthGet() }

    // A hung server costs one health timeout, not one per check.
    val downRunner = runner()
    every { healthApi.healthHealthGet() } throws SocketTimeoutException("timeout")
    assertFalse(downRunner.runs(IMAGE), "$IMAGE shouldn't run while the health call times out")
    every { healthApi.healthHealthGet() } returns mapOf("status" to "ok")
    assertFalse(downRunner.runs(IMAGE), "$IMAGE shouldn't run on the cached unhealthy answer, even after the server recovers")
    verify(exactly = 2) { healthApi.healthHealthGet() }
  }

  @Test
  fun `an expired health answer is checked again`() {
    val runner = runner(healthCacheTtl = Duration.ZERO)
    every { healthApi.healthHealthGet() } throws SocketTimeoutException("timeout")
    assertFalse(runner.runs(IMAGE), "$IMAGE shouldn't run while the health call times out")

    every { healthApi.healthHealthGet() } returns mapOf("status" to "ok")
    assertTrue(runner.runs(IMAGE), "$IMAGE should run once an expired health answer is checked again and the server is healthy")
    verify(exactly = 2) { healthApi.healthHealthGet() }
  }

  @Test
  fun `the server gets the manifest and the hydrated config as they are`() {
    val request = slot<CheckRequest>()
    every { client.manifestApi.check(capture(request)) } returns CheckResponse(success = true)

    runner().check(IMAGE, config, "workload-1")

    assertEquals(CheckRequest(manifest = manifest, config = config), request.captured)
  }

  @Test
  fun `the server's answer is the reported status`() {
    every { client.manifestApi.check(any()) } returns CheckResponse(success = true)
    assertEquals(
      SourceCheckResult.Reported(AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.SUCCEEDED)),
      runner().check(IMAGE, config, "workload-1"),
    )

    every { client.manifestApi.check(any()) } returns CheckResponse(success = false, message = "401 Unauthorized")
    assertEquals(
      SourceCheckResult.Reported(AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.FAILED).withMessage("401 Unauthorized")),
      runner().check(IMAGE, config, "workload-1"),
    )
  }

  @Test
  fun `a server that can't build or check the source reports the connector's error`() {
    every { client.manifestApi.check(any()) } throws ClientException("Client error : 400 Error checking connection: bad manifest", 400)

    assertEquals(
      SourceCheckResult.ConnectorError(AirbyteErrorTraceMessage().withMessage("Client error : 400 Error checking connection: bad manifest")),
      runner().check(IMAGE, config, "workload-1"),
    )
  }

  @Test
  fun `a server that refuses the check call is a platform error, not the user's`() {
    // 401/403: no shared JWT secret; 404/405: wrong base path or an older server; 413/422: a request it can't take.
    for (status in listOf(401, 403, 404, 405, 413, 422)) {
      every { client.manifestApi.check(any()) } throws ClientException("Client error : $status", status)

      assertThrows<IllegalStateException> { runner().check(IMAGE, config, "workload-1") }
    }
  }

  @Test
  fun `a check that needs custom code is a platform error, not the user's failure`() {
    every { client.manifestApi.check(any()) } returns
      CheckResponse(
        success = false,
        message = "Encountered an error while discovering streams. Error: Custom connector code is not permitted in this environment.",
      )
    assertThrows<IllegalStateException> { runner().check(IMAGE, config, "workload-1") }

    every { client.manifestApi.check(any()) } throws
      ClientException("Client error : 400 Error checking connection: Custom connector code is not permitted in this environment.", 400)
    assertThrows<IllegalStateException> { runner().check(IMAGE, config, "workload-1") }
  }

  @Test
  fun `a server that doesn't answer in time times the check out`() {
    every { client.manifestApi.check(any()) } throws SocketTimeoutException("timeout")

    assertEquals(SourceCheckResult.TimedOut, runner().check(IMAGE, config, "workload-1"))
  }

  @Test
  fun `a server that can't be connected to in time is a platform error, not the user's timeout`() {
    every { client.manifestApi.check(any()) } throws SocketTimeoutException("Connect timed out")

    assertThrows<IllegalStateException> { runner().check(IMAGE, config, "workload-1") }
  }

  @Test
  fun `a server failure propagates as a platform error`() {
    every { client.manifestApi.check(any()) } throws ServerException("Server error : 500", 500)

    assertThrows<ServerException> { runner().check(IMAGE, config, "workload-1") }
  }

  @Test
  fun `checking a connector the server can't run is a platform error`() {
    every { registry.lookup(IMAGE) } returns RegistryConnector.NotManifestOnly("it isn't manifest-only")

    assertThrows<IllegalStateException> { runner().check(IMAGE, config, "workload-1") }
  }
}
