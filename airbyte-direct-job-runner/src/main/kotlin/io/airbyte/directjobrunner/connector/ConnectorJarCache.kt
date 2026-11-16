/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

import io.airbyte.directjobrunner.config.CheckConnectorConfig
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.eclipse.aether.resolution.ArtifactResolutionException
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.ToDoubleFunction

private val logger = KotlinLogging.logger {}

/** The result of looking a jar up in [ConnectorJarCache]. */
sealed interface CachedJar {
  data class Hit(
    val jar: Path,
  ) : CachedJar

  data object Miss : CachedJar
}

/**
 * The connector jars on this runner, keyed by Maven coordinates. A check runs its jar from here ([get]); the first check
 * that needs a jar downloads it into the cache ([getOrDownload]) for the checks after it.
 *
 * The index is in memory and holds at most [MAX_JARS] jars. Caching one more evicts the least recently used jar, so a
 * version checks have moved off (after a connector upgrade) ages out however often it was used. An evicted jar's files are deleted [EVICTED_JAR_RETENTION] later rather than at
 * once, so a check that got the jar just before the eviction can still start it; once its JVM has opened the jar,
 * deleting the file doesn't affect it.
 */
@Singleton
class ConnectorJarCache internal constructor(
  private val resolver: ConnectorJarResolver,
  private val metricClient: MetricClient,
  cacheDir: Path,
  // Tests set these; the runner always uses the constants, through the @Inject constructor.
  private val maxJars: Int,
  private val evictedJarRetention: Duration,
) {
  @Inject
  constructor(resolver: ConnectorJarResolver, config: CheckConnectorConfig, metricClient: MetricClient) :
    this(resolver, metricClient, Path.of(config.jarCacheDir), MAX_JARS, EVICTED_JAR_RETENTION)

  companion object {
    /** How many jars the cache keeps. Caching one more evicts the least recently used. */
    const val MAX_JARS = 15

    /** How long an evicted jar's files stay on disk, so a check that got the jar just before the eviction can start it. */
    val EVICTED_JAR_RETENTION: Duration = Duration.ofMinutes(5)
  }

  // Each cached jar's path, least recently used first: access order moves a jar to the end on every get. Guarded by
  // this cache's monitor.
  private val entries = LinkedHashMap<String, Path>(maxJars, 0.75f, true)

  // One lock per coordinates, so concurrent checks of an uncached jar share a download while different jars download in
  // parallel, and an evicted jar's delayed delete waits for a download of the same jar. It holds one small object per
  // distinct jar ever cached. Taken before this cache's monitor, never while holding it.
  private val downloadLocks = ConcurrentHashMap<String, Any>()

  private val deletions =
    Executors.newSingleThreadScheduledExecutor { Thread(it, "evicted-jar-deletion").apply { isDaemon = true } }

  init {
    // The jar-cache volume outlives a container restart but the index doesn't, so jars left by the last runner would
    // sit on the volume uncounted against MAX_JARS.
    cacheDir.toFile().deleteRecursively()
    metricClient.gauge(OssMetricsRegistry.DIRECT_CHECK_JAR_CACHE_SIZE, this, ToDoubleFunction { it.size().toDouble() })
  }

  /**
   * The jar for [coordinates], marked as just used, downloaded into the cache first if it isn't cached. Throws
   * [ArtifactResolutionException] if it can't be downloaded; nothing is cached then, so the next call tries again.
   */
  fun getOrDownload(coordinates: String): Path {
    synchronized(downloadLock(coordinates)) {
      // Another check may have downloaded it while this one waited for the lock.
      val cached = get(coordinates)
      if (cached is CachedJar.Hit) {
        return cached.jar
      }

      val start = System.currentTimeMillis()
      val jar =
        try {
          resolver.resolve(coordinates)
        } catch (e: ArtifactResolutionException) {
          recordDownload(coordinates, downloadFailureResult(e), start)
          throw e
        }
      logger.info { "Downloaded $coordinates to $jar in ${System.currentTimeMillis() - start}ms" }
      recordDownload(coordinates, RESULT_SUCCESS, start)

      synchronized(this) {
        evictForNewJar()
        entries[coordinates] = jar
        logger.info { "Cached $coordinates (${entries.size} of $maxJars jars cached)" }
      }
      return jar
    }
  }

  /** The cached jar for [coordinates], marked as just used, or [CachedJar.Miss] if it isn't cached. */
  @Synchronized
  fun get(coordinates: String): CachedJar {
    val jar = entries[coordinates] ?: return CachedJar.Miss
    return CachedJar.Hit(jar)
  }

  @Synchronized
  fun isCached(coordinates: String): Boolean = coordinates in entries

  @Synchronized
  fun size(): Int = entries.size

  /** Whether a check for [coordinates] can get its jar: it is cached, or the jar repository has it. */
  fun isAvailable(coordinates: String): Boolean = isCached(coordinates) || resolver.isPublished(coordinates)

  /** The cached coordinates, for tests and diagnostics. */
  @Synchronized
  fun cached(): Set<String> = entries.keys.toSet()

  private fun recordDownload(
    coordinates: String,
    result: String,
    start: Long,
  ) {
    val tags =
      arrayOf(
        MetricAttribute(MetricTags.CONNECTOR_IMAGE, connectorImageTag(coordinates)),
        MetricAttribute(MetricTags.RESULT_TAG, result),
      )
    metricClient.count(OssMetricsRegistry.DIRECT_CHECK_JAR_DOWNLOAD, 1, *tags)
    metricClient.distribution(OssMetricsRegistry.DIRECT_CHECK_JAR_DOWNLOAD_DURATION, (System.currentTimeMillis() - start).toDouble(), *tags)
  }

  // Called holding this cache's monitor.
  private fun evictForNewJar() {
    while (entries.size >= maxJars) {
      val (coordinates, jar) = entries.entries.first().toPair()
      entries.remove(coordinates)
      logger.info { "Evicted $coordinates, the least recently used jar, to cache a new jar" }
      deletions.schedule({ deleteUnlessCachedAgain(coordinates, jar) }, evictedJarRetention.toMillis(), TimeUnit.MILLISECONDS)
    }
  }

  private fun downloadLock(coordinates: String): Any = downloadLocks.computeIfAbsent(coordinates) { Any() }

  // The jar's directory in the local Maven repository: the jar and the resolver's bookkeeping for that version. A jar
  // put back in the cache since its eviction reuses these files, so they stay. A download of the same jar writes to that
  // directory before the jar is back in the index, so the delete waits for it on the jar's download lock.
  private fun deleteUnlessCachedAgain(
    coordinates: String,
    jar: Path,
  ) {
    synchronized(downloadLock(coordinates)) {
      synchronized(this) {
        if (coordinates !in entries) {
          jar.parent.toFile().deleteRecursively()
          logger.info { "Deleted the files of evicted jar $coordinates" }
        }
      }
    }
  }
}
