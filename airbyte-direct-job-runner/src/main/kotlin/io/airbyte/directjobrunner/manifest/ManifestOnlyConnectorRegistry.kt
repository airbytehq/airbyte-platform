/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.manifest

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.version.Version
import io.airbyte.commons.yaml.Yamls
import io.airbyte.directjobrunner.config.ManifestServerCheckConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

private const val MANIFEST_ONLY_TAG = "language:manifest-only"
private const val METADATA_FILE = "metadata.yaml"
private const val MANIFEST_FILE = "manifest.yaml"

// The CDK version in a manifest-only connector's base image, such as
// `docker.io/airbyte/source-declarative-manifest:7.45.0@sha256:...`. The manifest's own `version` field is not it.
private val BASE_IMAGE_CDK_VERSION = Regex("""/source-declarative-manifest:(\d+\.\d+\.\d+)(@|$)""")

// A manifest component implemented in the connector's custom Python (components.py), which the registry doesn't
// publish next to the manifest.
private val CUSTOM_COMPONENT_CLASS = Regex("""class_name:\s*["']?(source_declarative_manifest\.)?components\.""")

// The kinds of rule under a manifest's `spec.config_normalization_rules`.
private val CONFIG_NORMALIZATION_RULES = listOf("config_migrations", "transformations", "validations")

/** What the connector registry says about one connector version, as far as running its check on a manifest server. */
sealed interface RegistryConnector {
  /** A manifest-only connector without custom Python: its [manifest], and the CDK version its image runs ([cdkVersion]). */
  data class ManifestOnly(
    val cdkVersion: Version,
    val manifest: JsonNode,
  ) : RegistryConnector

  /** Anything else; [reason] says why, for the runner's log. */
  data class NotManifestOnly(
    val reason: String,
  ) : RegistryConnector
}

/**
 * Looks connector versions up in the public connector registry, which publishes a `metadata.yaml` and, for
 * manifest-only connectors, a `manifest.yaml` for each version.
 *
 * Only manifest-only connectors without custom Python components are runnable, plus the allowlisted ones whose check
 * doesn't use their components ([ManifestServerCheckConfig.componentsOptionalConnectors]); the manifest server would
 * otherwise need their components.py too, and permission to run it. A manifest with config normalization rules isn't
 * runnable either: the manifest server doesn't apply them. Nor is one whose authenticator rotates a single-use refresh
 * token, since the manifest server doesn't return the new token.
 *
 * A published version never changes, so each lookup is kept for the life of the runner. A registry that can't be
 * reached throws, and a version it doesn't publish yet isn't kept either, so the next check for it tries again.
 */
@Singleton
class ManifestOnlyConnectorRegistry(
  private val config: ManifestServerCheckConfig,
) {
  private val httpClient: HttpClient = HttpClient.newBuilder().connectTimeout(config.registryTimeout).build()
  private val lookups = ConcurrentHashMap<String, RegistryConnector>()

  /** The connector behind [image], such as `airbyte/source-sendgrid:1.3.50`. Throws [IOException] when unreachable. */
  fun lookup(image: String): RegistryConnector {
    lookups[image]?.let { return it }

    // A version the registry doesn't publish yet (its check can arrive before the upload) isn't kept, so it runs here
    // once it is published rather than in pods until the runner restarts.
    val connector =
      try {
        fetchConnectorFromRegistry(image).also { lookups[image] = it }
      } catch (e: NotPublished) {
        RegistryConnector.NotManifestOnly("the connector registry doesn't publish its ${e.file} yet")
      }
    logger.info { "Connector registry: $image is ${describe(connector)}" }
    return connector
  }

  private fun fetchConnectorFromRegistry(image: String): RegistryConnector {
    val repository = image.substringBefore(':')
    val tag = image.substringAfter(':', "")
    if (tag.isEmpty()) {
      return RegistryConnector.NotManifestOnly("the image has no version tag")
    }
    val versionPath = "metadata/$repository/$tag"
    // One budget for both reads, not one each: the lookup runs inside the launcher's handoff call.
    val deadlineNanos = System.nanoTime() + config.registryTimeout.toNanos()

    val metadata = Yamls.deserialize(String(getFileFromConnectorRegistry(versionPath, METADATA_FILE, deadlineNanos))).path("data")
    if (metadata.path("tags").none { it.asText() == MANIFEST_ONLY_TAG }) {
      return RegistryConnector.NotManifestOnly("it isn't manifest-only")
    }
    // Allowlisted connectors ship custom Python components that their check doesn't use.
    val componentsOptional = repository in config.componentsOptionalConnectors
    if (!componentsOptional &&
      metadata
        .path("generated")
        .path("pythonComponents")
        .path("required")
        .asBoolean(false)
    ) {
      return RegistryConnector.NotManifestOnly("it has custom Python components")
    }

    val baseImage = metadata.path("connectorBuildOptions").path("baseImage").asText()
    val cdkVersion =
      BASE_IMAGE_CDK_VERSION.find(baseImage)?.groupValues?.get(1)
        ?: return RegistryConnector.NotManifestOnly("its base image '$baseImage' isn't a source-declarative-manifest release")

    // The metadata flag is the registry's own record; the manifest is what the server will actually build.
    val manifest = String(getFileFromConnectorRegistry(versionPath, MANIFEST_FILE, deadlineNanos))
    if (!componentsOptional && CUSTOM_COMPONENT_CLASS.containsMatchIn(manifest)) {
      return RegistryConnector.NotManifestOnly("its manifest uses custom Python components")
    }

    // The connector migrates, transforms and validates a config by these rules before its check; the manifest server
    // checks the config as given, so an older config that passes in a pod would fail there.
    val parsedManifest = Yamls.deserialize(manifest)
    val normalizationRules = parsedManifest.path("spec").path("config_normalization_rules")
    if (CONFIG_NORMALIZATION_RULES.any { !normalizationRules.path(it).isEmpty }) {
      return RegistryConnector.NotManifestOnly("its manifest normalizes the config before the check")
    }

    // A single-use refresh token is replaced when the check logs in, and the provider revokes the old one. A pod saves
    // the new token to the config; the manifest server's check result doesn't carry it, so the next sync couldn't log in.
    if (parsedManifest.findValue("refresh_token_updater") != null) {
      return RegistryConnector.NotManifestOnly("its authenticator rotates the refresh token")
    }
    return RegistryConnector.ManifestOnly(Version(cdkVersion), parsedManifest)
  }

  // A 404 means the registry doesn't publish the file for this version ([NotPublished]); any other failure is the
  // registry being unreachable ([IOException]), which isn't kept.
  private fun getFileFromConnectorRegistry(
    versionPath: String,
    file: String,
    deadlineNanos: Long,
  ): ByteArray {
    val path = "$versionPath/$file"
    val remaining = Duration.ofNanos(deadlineNanos - System.nanoTime())
    if (remaining.isNegative || remaining.isZero) {
      throw HttpTimeoutException("No time left in the registry lookup to read $path")
    }

    val request =
      HttpRequest
        .newBuilder(URI.create(config.registryBase + path))
        .timeout(remaining)
        .GET()
        .build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray())
    return when (response.statusCode()) {
      200 -> response.body()
      404 -> throw NotPublished(file)
      else -> throw IOException("The connector registry returned ${response.statusCode()} for $path")
    }
  }

  private class NotPublished(
    val file: String,
  ) : Exception("Not published: $file")

  private fun describe(connector: RegistryConnector): String =
    when (connector) {
      is RegistryConnector.ManifestOnly -> "manifest-only on CDK ${connector.cdkVersion.serialize()}"
      is RegistryConnector.NotManifestOnly -> "not runnable on the manifest server: ${connector.reason}"
    }
}
