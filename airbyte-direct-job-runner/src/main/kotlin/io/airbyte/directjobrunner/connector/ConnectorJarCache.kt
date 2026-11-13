/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

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
 * The prefetched connector jars on this runner, keyed by Maven coordinates. Only [ConnectorJarPrefetcher] fills it
 * ([put]); a check runs a cached jar ([get]) and downloads any other jar for itself without caching it.
 *
 * The index is in memory and holds at most [MAX_JARS] jars. Caching one more evicts the least frequently used jar (the
 * least recently used among equals). An evicted jar's files are deleted [EVICTED_JAR_RETENTION] later rather than at
 * once, so a check that got the jar just before the eviction can still start it; once its JVM has opened the jar,
 * deleting the file doesn't affect it.
 */
@Singleton
class ConnectorJarCache internal constructor(
  private val resolver: ConnectorJarResolver,
  private val metricClient: MetricClient,
  // Tests set these; the runner always uses the constants, through the @Inject constructor.
  private val maxJars: Int,
  private val evictedJarRetention: Duration,
) {
  @Inject
  constructor(resolver: ConnectorJarResolver, metricClient: MetricClient) : this(resolver, metricClient, MAX_JARS, EVICTED_JAR_RETENTION)

  companion object {
    /** How many jars the cache keeps. Caching one more evicts the least frequently used. */
    const val MAX_JARS = 20

    /** How long an evicted jar's files stay on disk, so a check that got the jar just before the eviction can start it. */
    val EVICTED_JAR_RETENTION: Duration = Duration.ofMinutes(5)
  }

  private class Entry(
    val jar: Path,
    var uses: Long,
    var lastUsed: Long,
  )

  // Guarded by this cache's monitor.
  private val entries = HashMap<String, Entry>()
  private var tick = 0L

  // One lock per coordinates, so concurrent puts of an uncached jar share a download while different jars download in
  // parallel. It holds one small object per distinct jar ever cached.
  private val downloadLocks = ConcurrentHashMap<String, Any>()

  private val deletions =
    Executors.newSingleThreadScheduledExecutor { Thread(it, "evicted-jar-deletion").apply { isDaemon = true } }

  init {
    metricClient.gauge(OssMetricsRegistry.DIRECT_CHECK_JAR_CACHE_SIZE, this, ToDoubleFunction { it.size().toDouble() })
  }

  /**
   * Downloads the jar for [coordinates] into the cache unless it is already cached. Returns whether it is cached
   * afterwards: false when it couldn't be downloaded (logged here).
   */
  fun put(coordinates: String): Boolean {
    if (isCached(coordinates)) {
      return true
    }

    synchronized(downloadLocks.computeIfAbsent(coordinates) { Any() }) {
      // Another put may have downloaded it while this one waited for the lock.
      if (isCached(coordinates)) {
        return true
      }

      val start = System.currentTimeMillis()
      val jar =
        try {
          resolver.resolve(coordinates)
        } catch (e: ArtifactResolutionException) {
          logger.warn(e) { "Couldn't download $coordinates" }
          recordDownload(coordinates, downloadFailureResult(e), start)
          return false
        }
      logger.info { "Downloaded $coordinates to $jar in ${System.currentTimeMillis() - start}ms" }
      recordDownload(coordinates, RESULT_SUCCESS, start)

      synchronized(this) {
        evictForNewJar()
        entries[coordinates] = Entry(jar, uses = 0, lastUsed = ++tick)
      }
      return true
    }
  }

  /** The cached jar for [coordinates], counting a use, or [CachedJar.Miss] if it isn't cached. */
  @Synchronized
  fun get(coordinates: String): CachedJar {
    val entry = entries[coordinates] ?: return CachedJar.Miss
    entry.uses++
    entry.lastUsed = ++tick
    return CachedJar.Hit(entry.jar)
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
        MetricAttribute(MetricTags.JAR_DOWNLOAD_PURPOSE_TAG, PURPOSE_PREFETCH),
        MetricAttribute(MetricTags.RESULT_TAG, result),
      )
    metricClient.count(OssMetricsRegistry.DIRECT_CHECK_JAR_DOWNLOAD, 1, *tags)
    metricClient.distribution(OssMetricsRegistry.DIRECT_CHECK_JAR_DOWNLOAD_DURATION, (System.currentTimeMillis() - start).toDouble(), *tags)
  }

  // Called holding this cache's monitor.
  private fun evictForNewJar() {
    while (entries.size >= maxJars) {
      val (coordinates, victim) = entries.entries.minWith(compareBy({ it.value.uses }, { it.value.lastUsed })).toPair()
      entries.remove(coordinates)
      logger.info { "Evicted $coordinates (used ${victim.uses} times) to cache a new jar" }
      deletions.schedule({ deleteUnlessCachedAgain(coordinates, victim.jar) }, evictedJarRetention.toMillis(), TimeUnit.MILLISECONDS)
    }
  }

  // The jar's directory in the local Maven repository: the jar and the resolver's bookkeeping for that version. A jar
  // put back in the cache since its eviction reuses these files, so they stay.
  @Synchronized
  private fun deleteUnlessCachedAgain(
    coordinates: String,
    jar: Path,
  ) {
    if (coordinates !in entries) {
      jar.parent.toFile().deleteRecursively()
    }
  }
}
