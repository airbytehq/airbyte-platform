/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

import io.airbyte.directjobrunner.config.CheckConnectorConfig
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.eclipse.aether.resolution.ArtifactResolutionException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.ToDoubleFunction

class ConnectorJarCacheTest {
  @TempDir
  lateinit var repoDir: Path

  // Each coordinate resolves to a real file in its own version directory, like the local Maven repository.
  private val metricClient = mockk<MetricClient>(relaxed = true)

  private val resolver =
    mockk<ConnectorJarResolver> {
      every { resolve(any()) } answers { jarFor(firstArg()) }
    }

  private fun jarFor(coordinates: String): Path {
    val (_, artifact, version) = coordinates.split(':')
    val jar = repoDir.resolve("$artifact/$version/$artifact-$version.jar")
    Files.createDirectories(jar.parent)
    if (!Files.exists(jar)) {
      Files.writeString(jar, coordinates)
    }
    return jar
  }

  private fun cache(
    maxJars: Int = 50,
    evictedJarRetention: Duration = Duration.ZERO,
  ) = ConnectorJarCache(resolver, metricClient, repoDir, maxJars, evictedJarRetention)

  // Gets the jar [times] times, as that many checks would; the first downloads it.
  private fun ConnectorJarCache.use(
    coordinates: String,
    times: Int = 1,
  ) {
    repeat(times) { assertEquals(jarFor(coordinates), getOrDownload(coordinates)) }
  }

  // Evicted jars are deleted on a background thread; wait for it.
  private fun awaitDeleted(path: Path) {
    val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos()
    while (Files.exists(path) && System.nanoTime() < deadline) {
      Thread.sleep(10)
    }
  }

  @Test
  fun `getOrDownload downloads a jar once, caches it, and later calls and get return it`() {
    val cache = cache()

    assertEquals(jarFor("g:source-postgres:3.8.10"), cache.getOrDownload("g:source-postgres:3.8.10"))
    assertEquals(jarFor("g:source-postgres:3.8.10"), cache.getOrDownload("g:source-postgres:3.8.10"))

    assertEquals(CachedJar.Hit(jarFor("g:source-postgres:3.8.10")), cache.get("g:source-postgres:3.8.10"))
    assertEquals(setOf("g:source-postgres:3.8.10"), cache.cached())
    verify(exactly = 1) { resolver.resolve("g:source-postgres:3.8.10") }
  }

  private fun verifyDownload(
    image: String,
    result: String,
  ) {
    val tags =
      arrayOf(
        MetricAttribute(MetricTags.CONNECTOR_IMAGE, image),
        MetricAttribute(MetricTags.RESULT_TAG, result),
      )
    verify(exactly = 1) { metricClient.count(OssMetricsRegistry.DIRECT_CHECK_JAR_DOWNLOAD, 1, *tags) }
    verify(exactly = 1) { metricClient.distribution(OssMetricsRegistry.DIRECT_CHECK_JAR_DOWNLOAD_DURATION, any(), *tags) }
  }

  @Test
  fun `a download is recorded with its result, and getting a cached jar records nothing`() {
    every { resolver.resolve("io.airbyte.connectors:source-mysql:3.50.0") } throws ArtifactResolutionException(emptyList(), "repository down")
    val cache = cache()

    cache.getOrDownload("io.airbyte.connectors:source-postgres:3.8.11")
    cache.getOrDownload("io.airbyte.connectors:source-postgres:3.8.11")
    assertThrows<ArtifactResolutionException> { cache.getOrDownload("io.airbyte.connectors:source-mysql:3.50.0") }

    verifyDownload("airbyte/source-postgres:3.8.11", "success")
    verifyDownload("airbyte/source-mysql:3.50.0", "error")
  }

  @Test
  fun `the cache size gauge reports how many jars are cached`() {
    val gauge = slot<ToDoubleFunction<ConnectorJarCache>>()
    every { metricClient.gauge(OssMetricsRegistry.DIRECT_CHECK_JAR_CACHE_SIZE, any<ConnectorJarCache>(), capture(gauge)) } answers { secondArg() }
    val cache = cache()
    cache.use("g:a:1")
    cache.use("g:b:1")

    assertEquals(2.0, gauge.captured.applyAsDouble(cache))
  }

  @Test
  fun `getting a jar that isn't cached is a miss`() {
    assertEquals(CachedJar.Miss, cache().get("g:source-postgres:3.8.10"))
  }

  @Test
  fun `the cap is 15 jars`() {
    val cache = ConnectorJarCache(resolver, CheckConnectorConfig(jarCacheDir = repoDir.toString()), metricClient)
    (1..16).forEach { cache.use("g:c$it:1") }

    assertEquals(15, cache.cached().size, "${cache.cached()}")
  }

  @Test
  fun `a failed download throws, caches nothing, and the next call tries again`() {
    every { resolver.resolve("g:a:1") } throws ArtifactResolutionException(emptyList(), "repository down") andThenAnswer { jarFor(firstArg()) }
    val cache = cache()

    assertThrows<ArtifactResolutionException> { cache.getOrDownload("g:a:1") }
    assertEquals(emptySet<String>(), cache.cached())

    assertEquals(jarFor("g:a:1"), cache.getOrDownload("g:a:1"))
    assertEquals(setOf("g:a:1"), cache.cached())
  }

  @Test
  fun `concurrent checks of an uncached jar share one download`() {
    val downloading = CountDownLatch(1)
    val release = CountDownLatch(1)
    every { resolver.resolve(any()) } answers {
      downloading.countDown()
      release.await(10, TimeUnit.SECONDS)
      jarFor(firstArg())
    }
    val cache = cache()
    val pool = Executors.newFixedThreadPool(5)

    val results = (1..5).map { pool.submit<Path> { cache.getOrDownload("g:source-postgres:3.8.10") } }
    assertTrue(downloading.await(10, TimeUnit.SECONDS))
    release.countDown()

    assertEquals(List(5) { jarFor("g:source-postgres:3.8.10") }, results.map { it.get(10, TimeUnit.SECONDS) })
    verify(exactly = 1) { resolver.resolve(any()) }
    pool.shutdown()
  }

  @Test
  fun `caching a jar past the cap evicts the least recently used one and deletes its files`() {
    val cache = cache(maxJars = 3)
    cache.use("g:a:1")
    cache.use("g:b:1")
    cache.use("g:c:1")
    cache.use("g:a:1")

    cache.use("g:d:1")

    assertEquals(setOf("g:a:1", "g:c:1", "g:d:1"), cache.cached())
    awaitDeleted(repoDir.resolve("b/1"))
    assertFalse(Files.exists(repoDir.resolve("b/1")), "the evicted jar's files are still there")
    assertTrue(Files.exists(repoDir.resolve("a/1/a-1.jar")))
  }

  @Test
  fun `a jar used often but not lately is evicted before a newer jar used once`() {
    // A connector upgrade: checks move from 1.0.0 to 1.1.0, and 1.0.0's earlier uses don't keep it cached.
    val cache = cache(maxJars = 2)
    cache.use("g:a:1.0.0", times = 100)
    cache.use("g:a:1.1.0")

    cache.use("g:b:1")

    assertEquals(setOf("g:a:1.1.0", "g:b:1"), cache.cached())
  }

  @Test
  fun `checking whether a jar is cached doesn't count as using it`() {
    val cache = cache(maxJars = 2)
    cache.use("g:a:1")
    cache.use("g:b:1")
    assertTrue(cache.isCached("g:a:1"))
    assertEquals(setOf("g:a:1", "g:b:1"), cache.cached())

    cache.use("g:c:1")

    assertEquals(setOf("g:b:1", "g:c:1"), cache.cached())
  }

  @Test
  fun `an evicted jar's files stay for the retention, so a check that just got it can still start it`() {
    val cache = cache(maxJars = 1, evictedJarRetention = Duration.ofHours(1))
    cache.use("g:a:1")
    val jar = jarFor("g:a:1")

    cache.use("g:b:1")

    assertEquals(setOf("g:b:1"), cache.cached())
    assertTrue(Files.exists(jar), "an evicted jar was deleted before its retention")
  }

  @Test
  fun `a jar cached again before its deletion keeps its files`() {
    val cache = cache(maxJars = 1, evictedJarRetention = Duration.ofMillis(200))
    cache.use("g:a:1")
    cache.use("g:b:1")

    cache.use("g:a:1")
    Thread.sleep(600)

    assertTrue(Files.exists(repoDir.resolve("a/1/a-1.jar")), "a jar cached again was deleted by its earlier eviction")
    assertEquals(CachedJar.Hit(jarFor("g:a:1")), cache.get("g:a:1"))
  }

  @Test
  fun `an evicted jar's delayed delete doesn't remove the files of a new download of the same jar`() {
    val cache = cache(maxJars = 1, evictedJarRetention = Duration.ofMillis(200))
    cache.use("g:a:1")
    cache.use("g:b:1")

    // The download writes its file, then is still running when a's delete from the eviction above comes due.
    every { resolver.resolve("g:a:1") } answers {
      val jar = jarFor(firstArg())
      Thread.sleep(600)
      jar
    }
    val jar = cache.getOrDownload("g:a:1")
    Thread.sleep(300)

    assertEquals(setOf("g:a:1"), cache.cached())
    assertTrue(Files.exists(jar), "the delayed delete removed a jar that is cached")
  }

  @Test
  fun `an evicted jar is downloaded again by the next check`() {
    val cache = cache(maxJars = 1)
    cache.use("g:a:1")
    cache.use("g:b:1")

    cache.use("g:a:1")

    verify(exactly = 2) { resolver.resolve("g:a:1") }
    assertEquals(setOf("g:a:1"), cache.cached())
  }

  @Test
  fun `jars left on the volume by the last runner are removed when the cache starts`() {
    val leftover = jarFor("g:a:1")

    val cache = cache()

    assertFalse(Files.exists(leftover), "a jar the new index doesn't track is still on the volume")
    assertEquals(emptySet<String>(), cache.cached())
  }

  @Test
  fun `a cached jar is available without asking the jar repository, an uncached one asks it`() {
    every { resolver.isPublished("g:b:1") } returns false
    val cache = cache()
    cache.use("g:a:1")

    assertTrue(cache.isAvailable("g:a:1"))
    assertFalse(cache.isAvailable("g:b:1"))

    verify(exactly = 0) { resolver.isPublished("g:a:1") }
    verify(exactly = 1) { resolver.isPublished("g:b:1") }
  }
}
