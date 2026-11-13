/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

import io.airbyte.directjobrunner.config.CheckConnectorConfig
import io.airbyte.directjobrunner.core.LauncherDataplaneGroup
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import org.eclipse.aether.resolution.VersionRangeResolutionException
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.concurrent.thread

private val logger = KotlinLogging.logger {}

/**
 * Downloads the latest released jar of every allowlisted connector into [ConnectorJarCache], so later checks don't wait
 * on the download. "Latest" is the highest release version in the jar repository; preview and release candidate
 * versions are skipped. A check for another version downloads that version's jar itself.
 *
 * The allowlist is the feature flag for the launcher's data plane group, which the runner first learns from a dispatch,
 * so this runs once per group, in the background, from the first check this runner is offered. Until a jar is cached,
 * checks don't wait for it: a check that arrives during the prefetch, or for a jar that failed here, downloads its own
 * copy for that check. A connector added to the flag later is not prefetched until the next restart.
 */
@Singleton
class ConnectorJarPrefetcher(
  private val allowlist: DirectCheckAllowlist,
  private val config: CheckConnectorConfig,
  private val resolver: ConnectorJarResolver,
  private val jarCache: ConnectorJarCache,
  private val metricClient: MetricClient,
) {
  private val prefetchedGroups = ConcurrentHashMap.newKeySet<UUID>()

  /** Starts prefetching the jars allowlisted for [dataplaneGroupId] in the background, the first time it is seen. */
  fun prefetchOnce(dataplaneGroupId: UUID) {
    if (!prefetchedGroups.add(dataplaneGroupId)) {
      return
    }
    thread(name = "jar-prefetch", isDaemon = true) {
      connectorsToPrefetch(dataplaneGroupId).forEach { prefetch(it) }
    }
  }

  /**
   * The connectors in the flag for [dataplaneGroupId], in the flag's order, up to the cache's [ConnectorJarCache.MAX_JARS]:
   * prefetching more would only evict jars this same prefetch cached. A longer flag is a configuration error, logged with
   * the connectors left out; their checks download their jar themselves.
   */
  fun connectorsToPrefetch(dataplaneGroupId: UUID): List<String> {
    val connectors = allowlist.connectors(LauncherDataplaneGroup.Known(dataplaneGroupId)).toList()
    val notPrefetched = connectors.drop(ConnectorJarCache.MAX_JARS)
    if (notPrefetched.isNotEmpty()) {
      logger.error {
        "platform.direct-check-images lists ${connectors.size} connectors for data plane group $dataplaneGroupId, more than the " +
          "${ConnectorJarCache.MAX_JARS} the jar cache holds. Not prefetching: ${notPrefetched.joinToString()}"
      }
      notPrefetched.forEach { recordPrefetch(it, "over_cache_limit") }
    }
    return connectors.take(ConnectorJarCache.MAX_JARS)
  }

  /** Downloads [connector]'s latest released jar unless it is cached. A jar that can't be found or downloaded is logged, not thrown. */
  fun prefetch(connector: String) {
    val start = System.currentTimeMillis()
    val versions =
      try {
        resolver.releasedVersions(connector)
      } catch (e: VersionRangeResolutionException) {
        logger.warn(e) { "Couldn't read $connector's versions to prefetch; checks download the jar for their version themselves" }
        recordPrefetch(connector, "version_list_failed")
        return
      }
    if (versions.isEmpty()) {
      logger.warn { "No released jar of $connector in the jar repository to prefetch" }
      recordPrefetch(connector, "no_release")
      return
    }

    val coordinates = config.jarCoordinates(connector, versions.last())
    if (jarCache.put(coordinates)) {
      logger.info { "Prefetched $coordinates in ${System.currentTimeMillis() - start}ms" }
      recordPrefetch(connector, "cached")
    } else {
      logger.warn { "Couldn't prefetch $coordinates; checks download the jar for their version themselves" }
      recordPrefetch(connector, "download_failed")
    }
  }

  // Why a download failed (not found, checksum mismatch) is on direct_check_jar_download.
  private fun recordPrefetch(
    connector: String,
    result: String,
  ) = metricClient.count(
    OssMetricsRegistry.DIRECT_CHECK_JAR_PREFETCH,
    1,
    MetricAttribute(MetricTags.CONNECTOR_NAME, connector),
    MetricAttribute(MetricTags.RESULT_TAG, result),
  )
}
