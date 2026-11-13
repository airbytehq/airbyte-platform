/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

import io.airbyte.directjobrunner.config.CheckConnectorConfig
import io.airbyte.featureflag.DataplaneGroup
import io.airbyte.featureflag.DirectCheckImages
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.eclipse.aether.resolution.ArtifactResolutionException
import org.eclipse.aether.resolution.VersionRangeRequest
import org.eclipse.aether.resolution.VersionRangeResolutionException
import org.eclipse.aether.resolution.VersionRangeResult
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.time.Duration
import java.util.UUID

class ConnectorJarPrefetcherTest {
  private val config = CheckConnectorConfig()

  private val resolver =
    mockk<ConnectorJarResolver> {
      every { releasedVersions("source-postgres") } returns listOf("3.8.10", "3.8.11")
      every { releasedVersions("source-mysql") } returns listOf("3.50.0")
      every { resolve(any()) } answers { Path.of("/cache/${firstArg<String>()}.jar") }
    }
  private val metricClient = mockk<MetricClient>(relaxed = true)
  private val cache = ConnectorJarCache(resolver, metricClient)

  private val dataplaneGroupId = UUID.randomUUID()

  private fun prefetcher(vararg connectors: String): ConnectorJarPrefetcher {
    val featureFlagClient =
      mockk<FeatureFlagClient> {
        every { stringVariation(DirectCheckImages, DataplaneGroup(dataplaneGroupId)) } returns connectors.joinToString(",")
      }
    return ConnectorJarPrefetcher(DirectCheckAllowlist(featureFlagClient), config, resolver, cache, metricClient)
  }

  // The prefetches run on background threads; wait until the expected jars are cached.
  private fun awaitCached(expected: Set<String>) {
    val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos()
    while (cache.cached() != expected && System.nanoTime() < deadline) {
      Thread.sleep(10)
    }
  }

  @Test
  fun `the first prefetch for a data plane group downloads the latest released jar of every connector in its flag`() {
    prefetcher("source-postgres", "source-mysql").prefetchOnce(dataplaneGroupId)

    val expected = setOf("io.airbyte.connectors:source-postgres:3.8.11", "io.airbyte.connectors:source-mysql:3.50.0")
    awaitCached(expected)
    assertEquals(expected, cache.cached())
  }

  @Test
  fun `a data plane group is prefetched only once`() {
    val prefetcher = prefetcher("source-postgres")

    repeat(3) { prefetcher.prefetchOnce(dataplaneGroupId) }
    awaitCached(setOf("io.airbyte.connectors:source-postgres:3.8.11"))

    verify(exactly = 1) { resolver.releasedVersions("source-postgres") }
  }

  @Test
  fun `a flag listing more connectors than the cache holds prefetches only as many as it holds, in the flag's order`() {
    val connectors = (1..ConnectorJarCache.MAX_JARS + 2).map { "source-c$it" }
    val prefetched = connectors.take(ConnectorJarCache.MAX_JARS)
    every { resolver.releasedVersions(any()) } returns listOf("1.0.0")
    val prefetcher = prefetcher(*connectors.toTypedArray())

    assertEquals(prefetched, prefetcher.connectorsToPrefetch(dataplaneGroupId))

    prefetcher.prefetchOnce(dataplaneGroupId)
    val expected = prefetched.map { "io.airbyte.connectors:$it:1.0.0" }.toSet()
    awaitCached(expected)
    assertEquals(expected, cache.cached())
    connectors.drop(ConnectorJarCache.MAX_JARS).forEach { verify(exactly = 0) { resolver.releasedVersions(it) } }
  }

  @Test
  fun `a flag listing as many connectors as the cache holds prefetches them all`() {
    val connectors = (1..ConnectorJarCache.MAX_JARS).map { "source-c$it" }

    assertEquals(connectors, prefetcher(*connectors.toTypedArray()).connectorsToPrefetch(dataplaneGroupId))
  }

  @Test
  fun `a connector with no released jar is skipped`() {
    every { resolver.releasedVersions("source-postgres") } returns emptyList()

    prefetcher("source-postgres").prefetch("source-postgres")

    assertEquals(emptySet<String>(), cache.cached())
    verify(exactly = 0) { resolver.resolve(any()) }
  }

  @Test
  fun `a version list or jar that can't be read is skipped, not thrown, and other connectors still prefetch`() {
    every { resolver.releasedVersions("source-postgres") } throws VersionRangeResolutionException(VersionRangeResult(VersionRangeRequest()))
    every { resolver.resolve("io.airbyte.connectors:source-mysql:3.50.0") } throws ArtifactResolutionException(emptyList(), "not found")
    val prefetcher = prefetcher("source-postgres", "source-mysql", "source-mssql")
    every { resolver.releasedVersions("source-mssql") } returns listOf("4.1.0")

    prefetcher.prefetch("source-postgres")
    prefetcher.prefetch("source-mysql")
    prefetcher.prefetch("source-mssql")

    assertEquals(setOf("io.airbyte.connectors:source-mssql:4.1.0"), cache.cached())
  }

  private fun verifyPrefetch(
    connector: String,
    result: String,
  ) = verify(exactly = 1) {
    metricClient.count(
      OssMetricsRegistry.DIRECT_CHECK_JAR_PREFETCH,
      1,
      MetricAttribute(MetricTags.CONNECTOR_NAME, connector),
      MetricAttribute(MetricTags.RESULT_TAG, result),
    )
  }

  @Test
  fun `each connector's prefetch result is recorded`() {
    every { resolver.releasedVersions("source-mssql") } returns emptyList()
    every { resolver.releasedVersions("source-oracle") } throws VersionRangeResolutionException(VersionRangeResult(VersionRangeRequest()))
    every { resolver.releasedVersions("source-db2") } returns listOf("1.0.0")
    every { resolver.resolve("io.airbyte.connectors:source-db2:1.0.0") } throws ArtifactResolutionException(emptyList(), "not found")
    val prefetcher = prefetcher()

    listOf("source-postgres", "source-mssql", "source-oracle", "source-db2").forEach { prefetcher.prefetch(it) }

    verifyPrefetch("source-postgres", "cached")
    verifyPrefetch("source-mssql", "no_release")
    verifyPrefetch("source-oracle", "version_list_failed")
    verifyPrefetch("source-db2", "download_failed")
  }

  @Test
  fun `connectors past the cache size are recorded as over the cache limit`() {
    val connectors = (1..ConnectorJarCache.MAX_JARS + 2).map { "source-c$it" }

    prefetcher(*connectors.toTypedArray()).connectorsToPrefetch(dataplaneGroupId)

    connectors.drop(ConnectorJarCache.MAX_JARS).forEach { verifyPrefetch(it, "over_cache_limit") }
    verify(exactly = 2) { metricClient.count(OssMetricsRegistry.DIRECT_CHECK_JAR_PREFETCH, 1, *anyVararg()) }
  }

  @Test
  fun `a jar that is already cached is not downloaded again`() {
    val prefetcher = prefetcher("source-postgres")

    prefetcher.prefetch("source-postgres")
    prefetcher.prefetch("source-postgres")

    verify(exactly = 1) { resolver.resolve(any()) }
  }
}
