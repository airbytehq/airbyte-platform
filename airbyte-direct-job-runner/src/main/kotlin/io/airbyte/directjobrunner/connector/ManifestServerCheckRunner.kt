/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.version.Version
import io.airbyte.directjobrunner.config.MANIFEST_SERVER_HEALTH_API
import io.airbyte.directjobrunner.config.ManifestServerCheckConfig
import io.airbyte.directjobrunner.manifest.ManifestOnlyConnectorRegistry
import io.airbyte.directjobrunner.manifest.RegistryConnector
import io.airbyte.manifestserver.api.client.ManifestServerApiClient
import io.airbyte.manifestserver.api.client.generated.HealthApi
import io.airbyte.manifestserver.api.client.model.generated.CheckRequest
import io.airbyte.micronaut.runtime.AirbyteManifestServerApiClientConfig
import io.airbyte.micronaut.runtime.MANIFEST_SERVER_API_PREFIX
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.airbyte.protocol.models.v0.AirbyteErrorTraceMessage
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException
import org.openapitools.client.infrastructure.ServerException
import java.io.IOException
import java.net.HttpURLConnection.HTTP_BAD_REQUEST
import java.net.SocketTimeoutException
import java.time.Duration

private val logger = KotlinLogging.logger {}

private val RELEASE_VERSION = Regex("""\d+\.\d+\.\d+""")

// OkHttp's message when it can't connect in time; a read timeout says "timeout".
private const val CONNECT_TIMED_OUT = "connect timed out"

// The manifest server's answer when building the source needs custom Python components, which it doesn't allow. It
// comes back as a failed check (or a 400), so it would read as the user's failure.
private const val CUSTOM_CODE_NOT_PERMITTED = "Custom connector code is not permitted"

/**
 * Runs checks for Airbyte's manifest-only connectors on the data plane's manifest server, instead of a pod of the
 * connector's own image. The manifest comes from the connector registry ([ManifestOnlyConnectorRegistry]); the
 * hydrated config comes from the check.
 *
 * It takes a connector version only when the server can run it the way the connector's image would: the image's CDK
 * is on the server's major version and no newer (a newer CDK can have manifest components the server doesn't). The rest run in
 * a pod, as does every check while the server or the registry can't be reached.
 */
@Singleton
@Requires(property = "$MANIFEST_SERVER_API_PREFIX.base-path", pattern = ".+")
class ManifestServerCheckRunner(
  private val manifestServerApiClient: ManifestServerApiClient,
  @Named(MANIFEST_SERVER_HEALTH_API) private val healthApi: HealthApi,
  private val registry: ManifestOnlyConnectorRegistry,
  private val config: ManifestServerCheckConfig,
  private val apiConfig: AirbyteManifestServerApiClientConfig,
) {
  // The HTTP client gives up after its connect and read timeouts, so a check can't outlast them.
  val checkTimeout: Duration = Duration.ofSeconds(apiConfig.connectTimeoutSeconds + apiConfig.readTimeoutSeconds)

  // The last health answer and when it was taken; reused for config.healthCacheTtl. Starts expired.
  @Volatile
  private var health: HealthAnswer = HealthAnswer(healthy = false, atNanos = System.nanoTime() - config.healthCacheTtl.toNanos())

  /** Whether the manifest server can run a check for [image], such as `airbyte/source-sendgrid:1.3.51`, right now. */
  fun runs(image: String): Boolean {
    val serverVersion = config.serverCdkVersion.trim()
    if (!RELEASE_VERSION.matches(serverVersion)) {
      return false
    }
    // The server rejects unsigned check calls, and only its check endpoint says so, after a check was accepted.
    if (apiConfig.signatureSecret.isBlank()) {
      return false
    }

    val connector =
      try {
        registry.lookup(image)
      } catch (e: IOException) {
        logger.warn(e) { "Running check for $image in a pod: the connector registry can't be reached" }
        return false
      }
    if (connector !is RegistryConnector.ManifestOnly) {
      return false
    }

    if (!supportsCdk(connector.cdkVersion, Version(serverVersion))) {
      logger.info { "Running check for $image in a pod: its CDK ${connector.cdkVersion.serialize()} isn't supported by $serverVersion" }
      return false
    }

    return serverHealthy(image)
  }

  /** Runs the check for [image] on the manifest server with its hydrated [connectionConfiguration]. */
  fun check(
    image: String,
    connectionConfiguration: JsonNode,
    workloadId: String,
  ): SourceCheckResult {
    val connector =
      registry.lookup(image) as? RegistryConnector.ManifestOnly
        ?: error("$image isn't a manifest-only connector the manifest server can run")

    // The manifest server's own log names neither the check nor the connector, so these two lines tie its requests to them.
    logger.info {
      "Sending check $workloadId for $image (CDK ${connector.cdkVersion.serialize()}) to the manifest server " +
        "(CDK ${config.serverCdkVersion.trim()})"
    }
    val sentAt = System.currentTimeMillis()
    val result =
      try {
        val response = manifestServerApiClient.manifestApi.check(CheckRequest(manifest = connector.manifest, config = connectionConfiguration))
        val status = if (response.success) AirbyteConnectionStatus.Status.SUCCEEDED else AirbyteConnectionStatus.Status.FAILED
        SourceCheckResult.Reported(AirbyteConnectionStatus().withStatus(status).withMessage(response.message))
      } catch (e: ClientException) {
        // The server answers 400 when the source can't be built from the manifest and config, or the check itself
        // throws: the connector's own error, as a pod's error trace would be. Any other 4xx is a platform fault, not the
        // user's: 401/403 when the runner and the server don't share the JWT signature secret, 404/405 from a wrong
        // base path or an older server, 413 or 422 when the server can't take the request.
        if (e.statusCode != HTTP_BAD_REQUEST) {
          throw IllegalStateException("The manifest server refused the runner's check call with ${e.statusCode}", e)
        }
        SourceCheckResult.ConnectorError(AirbyteErrorTraceMessage().withMessage(e.message))
      } catch (e: SocketTimeoutException) {
        // OkHttp throws the same exception when it can't connect in time: the server went away after the check was
        // accepted, a platform fault rather than the user's connector timing out.
        if (e.message.orEmpty().contains(CONNECT_TIMED_OUT, ignoreCase = true)) {
          throw IllegalStateException("The manifest server couldn't be reached for the check", e)
        }
        SourceCheckResult.TimedOut
      }
    logger.info {
      "Manifest server answered check $workloadId for $image in ${System.currentTimeMillis() - sentAt} ms: ${describe(result)}"
    }

    // A connector that needs its custom components (one allowlisted by mistake, or a version whose check started using
    // them) was routed here wrongly: a platform fault, not the user's.
    if (needsCustomCode(result)) {
      throw IllegalStateException("The manifest server can't run $image's check without its custom Python components")
    }
    return result
  }

  // The outcome without the message, which can echo the user's config.
  private fun describe(result: SourceCheckResult): String =
    when (result) {
      is SourceCheckResult.Reported -> "status=${result.status.status}"
      else -> "result=${result::class.simpleName}"
    }

  private fun needsCustomCode(result: SourceCheckResult): Boolean =
    when (result) {
      is SourceCheckResult.Reported ->
        result.status.message
          .orEmpty()
          .contains(CUSTOM_CODE_NOT_PERMITTED)
      is SourceCheckResult.ConnectorError ->
        result.error.message
          .orEmpty()
          .contains(CUSTOM_CODE_NOT_PERMITTED)
      else -> false
    }

  // The image runs a CDK from its base image; the server runs one CDK. Only the server's major version (a different
  // manifest schema otherwise), and nothing newer than it.
  private fun supportsCdk(
    cdkVersion: Version,
    serverVersion: Version,
  ): Boolean = cdkVersion.getMajorVersion() == serverVersion.getMajorVersion() && cdkVersion.lessThanOrEqualTo(serverVersion)

  // While the server is down or failing, checks run in pods. The health endpoint isn't signed, so it can't tell
  // whether the server accepts the runner's calls. One answer serves every check for config.healthCacheTtl.
  private fun serverHealthy(image: String): Boolean {
    val cached = health
    if (System.nanoTime() - cached.atNanos < config.healthCacheTtl.toNanos()) {
      if (!cached.healthy) {
        logger.info { "Running check for $image in a pod: the manifest server was unhealthy at its last health check" }
      }
      return cached.healthy
    }

    val healthy =
      try {
        healthApi.healthHealthGet()
        true
      } catch (e: IOException) {
        logger.warn(e) { "Running check for $image in a pod: the manifest server can't be reached" }
        false
      } catch (e: ServerException) {
        logger.warn(e) { "Running check for $image in a pod: the manifest server failed" }
        false
      }
    health = HealthAnswer(healthy, System.nanoTime())
    return healthy
  }

  private data class HealthAnswer(
    val healthy: Boolean,
    val atNanos: Long,
  )
}
