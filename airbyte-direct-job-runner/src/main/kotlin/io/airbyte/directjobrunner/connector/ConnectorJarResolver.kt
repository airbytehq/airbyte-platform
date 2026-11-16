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
import jakarta.inject.Singleton
import org.eclipse.aether.ConfigurationProperties
import org.eclipse.aether.RepositorySystem
import org.eclipse.aether.RepositorySystemSession
import org.eclipse.aether.artifact.DefaultArtifact
import org.eclipse.aether.repository.RemoteRepository
import org.eclipse.aether.repository.RepositoryPolicy
import org.eclipse.aether.resolution.ArtifactRequest
import org.eclipse.aether.supplier.RepositorySystemSupplier
import org.eclipse.aether.supplier.SessionBuilderSupplier
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.time.Duration

private val logger = KotlinLogging.logger {}

// The resolver's own defaults (30s to connect, 30 minutes without data) would hold a check far past its deadline.
private val CONNECT_TIMEOUT = Duration.ofSeconds(10)
private val REQUEST_TIMEOUT = Duration.ofSeconds(60)

// The existence check runs while the launcher waits on its handoff, which times out after 10s and is then retried.
private val PUBLISHED_CHECK_TIMEOUT = Duration.ofSeconds(3)
private const val HTTP_OK = 200
private const val HTTP_NOT_FOUND = 404
private const val PUBLISHED = "published"
private const val NOT_PUBLISHED = "not_published"
private const val PUBLISHED_CHECK_ERROR = "error"

/**
 * Downloads a connector's fat jar from the jar repository into the local Maven repository at
 * [CheckConnectorConfig.jarCacheDir], or finds it there.
 *
 * Only the jar itself is resolved: it carries every dependency, and its POM declares none.
 */
@Singleton
class ConnectorJarResolver(
  private val config: CheckConnectorConfig,
  private val metricClient: MetricClient,
) {
  private val httpClient: HttpClient = HttpClient.newBuilder().connectTimeout(PUBLISHED_CHECK_TIMEOUT).build()

  /**
   * Whether the jar for [coordinates] is in the jar repository, asked with a HEAD request so nothing is downloaded. Any
   * answer but 200, including a timeout or an unreachable repository, counts as not published.
   */
  fun isPublished(coordinates: String): Boolean {
    val start = System.currentTimeMillis()
    val result = publishedCheckResult(coordinates)
    val resultTag = MetricAttribute(MetricTags.RESULT_TAG, result)
    metricClient.count(
      OssMetricsRegistry.DIRECT_CHECK_JAR_PUBLISHED_CHECK,
      1,
      MetricAttribute(MetricTags.CONNECTOR_IMAGE, connectorImageTag(coordinates)),
      resultTag,
    )
    metricClient.distribution(
      OssMetricsRegistry.DIRECT_CHECK_JAR_PUBLISHED_CHECK_DURATION,
      (System.currentTimeMillis() - start).toDouble(),
      resultTag,
    )
    return result == PUBLISHED
  }

  // published (200), not_published (404), or error (any other answer, a timeout or an unreachable repository).
  private fun publishedCheckResult(coordinates: String): String {
    val url = jarUrl(coordinates)
    val request =
      HttpRequest
        .newBuilder(url)
        .method("HEAD", HttpRequest.BodyPublishers.noBody())
        .timeout(PUBLISHED_CHECK_TIMEOUT)
        .build()
    val status =
      try {
        httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode()
      } catch (e: IOException) {
        logger.warn(e) { "Couldn't ask the jar repository for $url" }
        return PUBLISHED_CHECK_ERROR
      }
    return when (status) {
      HTTP_OK -> PUBLISHED
      HTTP_NOT_FOUND -> NOT_PUBLISHED
      else -> {
        logger.warn { "The jar repository answered $status for $url" }
        PUBLISHED_CHECK_ERROR
      }
    }
  }

  /**
   * Downloads the jar for [coordinates] (`group:artifact:version`) into the cache directory, [CheckConnectorConfig.jarCacheDir],
   * or finds it there, and returns its path. Throws [org.eclipse.aether.resolution.ArtifactResolutionException] if it
   * can't be fetched.
   */
  fun resolve(coordinates: String): Path =
    withSession(Path.of(config.jarCacheDir)) { system, session ->
      val request = ArtifactRequest(DefaultArtifact(coordinates), listOf(repository()), null)
      system.resolveArtifact(session, request).artifact.path
    }

  private fun <T> withSession(
    localRepository: Path,
    block: (RepositorySystem, RepositorySystemSession) -> T,
  ): T {
    val system = RepositorySystemSupplier().get()
    try {
      SessionBuilderSupplier(system)
        .get()
        .withLocalRepositoryBaseDirectories(localRepository)
        .setConfigProperty(ConfigurationProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT.toMillis().toInt())
        .setConfigProperty(ConfigurationProperties.REQUEST_TIMEOUT, REQUEST_TIMEOUT.toMillis().toInt())
        .build()
        .use { session -> return block(system, session) }
    } finally {
      system.shutdown()
    }
  }

  // The Maven repository layout: `io/airbyte/connectors/source-postgres/3.8.10/source-postgres-3.8.10.jar`.
  private fun jarUrl(coordinates: String): URI {
    val (groupId, artifactId, version) = coordinates.split(':')
    val path = "${groupId.replace('.', '/')}/$artifactId/$version/$artifactId-$version.jar"
    return URI.create("${config.jarRepositoryUrl.trimEnd('/')}/$path")
  }

  // A published release jar never changes, so a cached copy is never re-checked. A download whose checksum doesn't match
  // the published one fails instead of being run.
  private fun repository(): RemoteRepository =
    RemoteRepository
      .Builder("connector-jars", "default", config.jarRepositoryUrl)
      .setReleasePolicy(RepositoryPolicy(true, RepositoryPolicy.UPDATE_POLICY_NEVER, RepositoryPolicy.CHECKSUM_POLICY_FAIL))
      .setSnapshotPolicy(RepositoryPolicy(false, RepositoryPolicy.UPDATE_POLICY_NEVER, RepositoryPolicy.CHECKSUM_POLICY_FAIL))
      .build()
}
