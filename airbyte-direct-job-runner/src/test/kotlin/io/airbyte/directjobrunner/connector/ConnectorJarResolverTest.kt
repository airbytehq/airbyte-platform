/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

import io.airbyte.directjobrunner.config.CheckConnectorConfig
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.mockk.mockk
import io.mockk.verify
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import okio.Buffer
import org.eclipse.aether.resolution.ArtifactResolutionException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

private const val COORDINATES = "io.airbyte.connectors:source-postgres:3.8.10"
private const val JAR_PATH = "/io/airbyte/connectors/source-postgres/3.8.10/source-postgres-3.8.10.jar"

class ConnectorJarResolverTest {
  @TempDir
  lateinit var cacheDir: Path

  private val server = MockWebServer()
  private val metricClient = mockk<MetricClient>(relaxed = true)
  private val jarBytes = "not really a jar".toByteArray()

  @AfterEach
  fun tearDown() {
    server.shutdown()
  }

  // Serves the jar and its checksums; every other path is a 404. [sha1] lets a test publish a wrong checksum.
  private fun dispatcher(sha1: String = hex("SHA-1", jarBytes)) =
    object : Dispatcher() {
      override fun dispatch(request: RecordedRequest): MockResponse =
        when (request.path) {
          JAR_PATH -> MockResponse().setBody(Buffer().write(jarBytes))
          "$JAR_PATH.sha1" -> MockResponse().setBody(sha1)
          "$JAR_PATH.md5" -> MockResponse().setBody(hex("MD5", jarBytes))
          else -> MockResponse().setResponseCode(404)
        }
    }

  private fun serve(sha1: String = hex("SHA-1", jarBytes)) {
    server.dispatcher = dispatcher(sha1 = sha1)
    server.start()
  }

  private fun resolver(
    repositoryUrl: String = server.url("/").toString(),
    localRepository: Path = cacheDir,
  ) = ConnectorJarResolver(
    CheckConnectorConfig(jarRepositoryUrl = repositoryUrl, jarCacheDir = localRepository.toString()),
    metricClient,
  )

  private fun verifyPublishedCheck(
    image: String,
    result: String,
  ) {
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.DIRECT_CHECK_JAR_PUBLISHED_CHECK,
        1,
        MetricAttribute(MetricTags.CONNECTOR_IMAGE, image),
        MetricAttribute(MetricTags.RESULT_TAG, result),
      )
    }
    verify(exactly = 1) {
      metricClient.distribution(OssMetricsRegistry.DIRECT_CHECK_JAR_PUBLISHED_CHECK_DURATION, any(), MetricAttribute(MetricTags.RESULT_TAG, result))
    }
  }

  @Test
  fun `the jar for the image's version is downloaded into the cache`() {
    serve()

    val jar = resolver().resolve(COORDINATES)

    assertEquals(cacheDir.resolve(JAR_PATH.removePrefix("/")), jar)
    assertArrayEquals(jarBytes, Files.readAllBytes(jar))
  }

  @Test
  fun `a cached jar is used without asking the repository again`() {
    serve()
    val first = resolver().resolve(COORDINATES)
    val repositoryUrl = server.url("/").toString()
    server.shutdown()

    assertEquals(first, resolver(repositoryUrl).resolve(COORDINATES))
  }

  @Test
  fun `a download that doesn't match the published checksum fails and isn't cached`() {
    serve(sha1 = "0".repeat(40))

    assertThrows<ArtifactResolutionException> { resolver().resolve(COORDINATES) }
    assertFalse(Files.exists(cacheDir.resolve(JAR_PATH.removePrefix("/"))), "the mismatched jar was cached")
  }

  @Test
  fun `a published jar is found with a HEAD request, without downloading it`() {
    serve()

    assertTrue(resolver().isPublished(COORDINATES))
    verifyPublishedCheck("airbyte/source-postgres:3.8.10", "published")
    val request = server.takeRequest()
    assertEquals("HEAD $JAR_PATH", "${request.method} ${request.path}")
    assertEquals(1, server.requestCount)
    assertFalse(Files.exists(cacheDir.resolve(JAR_PATH.removePrefix("/"))), "the existence check downloaded the jar")
  }

  @Test
  fun `a jar that isn't published, an error and an unreachable repository all count as not published`() {
    serve()
    assertFalse(resolver().isPublished("io.airbyte.connectors:source-postgres:9.9.9"))
    verifyPublishedCheck("airbyte/source-postgres:9.9.9", "not_published")

    server.dispatcher =
      object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse = MockResponse().setResponseCode(500)
      }
    assertFalse(resolver().isPublished(COORDINATES))

    val repositoryUrl = server.url("/").toString()
    server.shutdown()
    assertFalse(resolver(repositoryUrl).isPublished(COORDINATES))
    // The 500 and the unreachable repository.
    verify(exactly = 2) {
      metricClient.count(
        OssMetricsRegistry.DIRECT_CHECK_JAR_PUBLISHED_CHECK,
        1,
        MetricAttribute(MetricTags.CONNECTOR_IMAGE, "airbyte/source-postgres:3.8.10"),
        MetricAttribute(MetricTags.RESULT_TAG, "error"),
      )
    }
  }

  @Test
  fun `a failed download is classified as not found, a checksum mismatch, or another error`() {
    serve(sha1 = "0".repeat(40))
    val resolver = resolver()

    val notFound = assertThrows<ArtifactResolutionException> { resolver.resolve("io.airbyte.connectors:source-postgres:9.9.9") }
    val checksum = assertThrows<ArtifactResolutionException> { resolver.resolve(COORDINATES) }
    val repositoryUrl = server.url("/").toString()
    server.shutdown()
    val unreachable =
      assertThrows<ArtifactResolutionException> { resolver(repositoryUrl, cacheDir.resolve("other")).resolve(COORDINATES) }

    assertEquals(
      listOf("not_found", "checksum_mismatch", "error"),
      listOf(notFound, checksum, unreachable).map { downloadFailureResult(it) },
    )
  }

  @Test
  fun `a version that isn't published fails`() {
    serve()

    assertThrows<ArtifactResolutionException> { resolver().resolve("io.airbyte.connectors:source-postgres:9.9.9") }
  }

  private fun hex(
    algorithm: String,
    bytes: ByteArray,
  ): String = MessageDigest.getInstance(algorithm).digest(bytes).joinToString("") { "%02x".format(it) }
}
