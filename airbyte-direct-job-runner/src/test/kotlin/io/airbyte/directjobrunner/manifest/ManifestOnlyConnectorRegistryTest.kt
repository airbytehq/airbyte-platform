/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.manifest

import io.airbyte.commons.version.Version
import io.airbyte.commons.yaml.Yamls
import io.airbyte.directjobrunner.config.ManifestServerCheckConfig
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.time.Duration
import java.util.concurrent.TimeUnit

private const val IMAGE = "airbyte/source-sendgrid:1.3.50"
private const val VERSION_PATH = "/metadata/airbyte/source-sendgrid/1.3.50"
private const val MANIFEST = "version: 6.7.0\ncheck:\n  type: CheckStream\n  stream_names:\n    - bounces\n"

class ManifestOnlyConnectorRegistryTest {
  private val files = mutableMapOf<String, MockResponse>()
  private val server =
    MockWebServer().apply {
      dispatcher =
        object : Dispatcher() {
          override fun dispatch(request: RecordedRequest): MockResponse = files[request.path] ?: MockResponse().setResponseCode(404)
        }
    }
  private val registry = ManifestOnlyConnectorRegistry(ManifestServerCheckConfig(registryBaseUrl = server.url("/").toString()))

  @AfterEach
  fun tearDown() = server.shutdown()

  private fun publish(
    file: String,
    body: String,
  ) {
    files["$VERSION_PATH/$file"] = MockResponse().setBody(body)
  }

  private fun metadata(
    tag: String = "language:manifest-only",
    baseImage: String = "docker.io/airbyte/source-declarative-manifest:7.45.0@sha256:a4947a",
    pythonComponentsRequired: Boolean = false,
  ) = """
    data:
      dockerRepository: airbyte/source-sendgrid
      dockerImageTag: 1.3.50
      connectorBuildOptions:
        baseImage: $baseImage
      tags:
        - $tag
      generated:
        pythonComponents:
          required: $pythonComponentsRequired
    """.trimIndent()

  private fun publishManifestOnly(manifest: String = MANIFEST) {
    publish("metadata.yaml", metadata())
    publish("manifest.yaml", manifest)
  }

  @Test
  fun `a manifest-only connector comes with its manifest and the CDK version of its base image`() {
    publishManifestOnly()

    assertEquals(RegistryConnector.ManifestOnly(Version("7.45.0"), Yamls.deserialize(MANIFEST)), registry.lookup(IMAGE))
  }

  @Test
  fun `the CDK version comes from the base image, not the manifest`() {
    publish("metadata.yaml", metadata(baseImage = "docker.io/airbyte/source-declarative-manifest:7.35.0"))
    publish("manifest.yaml", MANIFEST)

    assertEquals(RegistryConnector.ManifestOnly(Version("7.35.0"), Yamls.deserialize(MANIFEST)), registry.lookup(IMAGE))
  }

  @Test
  fun `a Python connector version isn't manifest-only`() {
    publish("metadata.yaml", metadata(tag = "language:python", baseImage = "docker.io/airbyte/python-connector-base:2.0.0"))

    assertEquals(RegistryConnector.NotManifestOnly("it isn't manifest-only"), registry.lookup(IMAGE))
  }

  @Test
  fun `a connector with custom Python components isn't runnable yet`() {
    publish("metadata.yaml", metadata(pythonComponentsRequired = true))

    assertEquals(RegistryConnector.NotManifestOnly("it has custom Python components"), registry.lookup(IMAGE))
    assertEquals(1, server.requestCount)
  }

  @Test
  fun `a manifest that uses custom components isn't runnable, even when its metadata doesn't say so`() {
    for (className in listOf("source_declarative_manifest.components.BingAdsErrorHandler", "components.MyExtractor", "\"components.Quoted\"")) {
      files.clear()
      val registry = ManifestOnlyConnectorRegistry(ManifestServerCheckConfig(registryBaseUrl = server.url("/").toString()))
      publishManifestOnly(MANIFEST + "definitions:\n  handler:\n    type: CustomErrorHandler\n    class_name: $className\n")

      assertEquals(RegistryConnector.NotManifestOnly("its manifest uses custom Python components"), registry.lookup(IMAGE), className)
    }
  }

  @Test
  fun `an allowlisted connector whose check doesn't use its custom components is runnable`() {
    val registry =
      ManifestOnlyConnectorRegistry(
        ManifestServerCheckConfig(registryBaseUrl = server.url("/").toString(), componentsOptionalConnectors = listOf("airbyte/source-sendgrid")),
      )
    val manifest =
      MANIFEST + "definitions:\n  migration:\n    type: CustomRecordTransformation\n    class_name: source_declarative_manifest.components.Fix\n"
    publish("metadata.yaml", metadata(pythonComponentsRequired = true))
    publish("manifest.yaml", manifest)

    assertEquals(RegistryConnector.ManifestOnly(Version("7.45.0"), Yamls.deserialize(manifest)), registry.lookup(IMAGE))
  }

  @Test
  fun `a manifest that normalizes the config before the check isn't runnable`() {
    for (rule in listOf("config_migrations", "transformations", "validations")) {
      files.clear()
      val registry = ManifestOnlyConnectorRegistry(ManifestServerCheckConfig(registryBaseUrl = server.url("/").toString()))
      publishManifestOnly(MANIFEST + "spec:\n  config_normalization_rules:\n    type: ConfigNormalizationRules\n    $rule:\n      - type: Rule\n")

      assertEquals(RegistryConnector.NotManifestOnly("its manifest normalizes the config before the check"), registry.lookup(IMAGE), rule)
    }
  }

  @Test
  fun `an allowlisted connector that migrates its config isn't runnable`() {
    val registry =
      ManifestOnlyConnectorRegistry(
        ManifestServerCheckConfig(registryBaseUrl = server.url("/").toString(), componentsOptionalConnectors = listOf("airbyte/source-sendgrid")),
      )
    publish("metadata.yaml", metadata(pythonComponentsRequired = true))
    publish("manifest.yaml", MANIFEST + "spec:\n  config_normalization_rules:\n    config_migrations:\n      - type: ConfigMigration\n")

    assertEquals(RegistryConnector.NotManifestOnly("its manifest normalizes the config before the check"), registry.lookup(IMAGE))
  }

  @Test
  fun `a manifest whose authenticator rotates its refresh token isn't runnable`() {
    publishManifestOnly(
      MANIFEST +
        "definitions:\n  base_requester:\n    authenticator:\n      type: OAuthAuthenticator\n" +
        "      refresh_token_updater:\n        refresh_token_name: refresh_token\n",
    )

    assertEquals(RegistryConnector.NotManifestOnly("its authenticator rotates the refresh token"), registry.lookup(IMAGE))
  }

  @Test
  fun `config normalization rules without any rules are runnable`() {
    val manifest = MANIFEST + "spec:\n  config_normalization_rules:\n    type: ConfigNormalizationRules\n    config_migrations: []\n"
    publishManifestOnly(manifest)

    assertEquals(RegistryConnector.ManifestOnly(Version("7.45.0"), Yamls.deserialize(manifest)), registry.lookup(IMAGE))
  }

  @Test
  fun `the allowlist only covers the connectors it names`() {
    val registry =
      ManifestOnlyConnectorRegistry(
        ManifestServerCheckConfig(registryBaseUrl = server.url("/").toString(), componentsOptionalConnectors = listOf("airbyte/source-stripe")),
      )
    publish("metadata.yaml", metadata(pythonComponentsRequired = true))

    assertEquals(RegistryConnector.NotManifestOnly("it has custom Python components"), registry.lookup(IMAGE))
  }

  @Test
  fun `a base image that isn't a source-declarative-manifest release isn't runnable`() {
    publish("metadata.yaml", metadata(baseImage = "docker.io/airbyte/source-declarative-manifest:dev"))

    assertEquals(
      RegistryConnector.NotManifestOnly(
        "its base image 'docker.io/airbyte/source-declarative-manifest:dev' isn't a source-declarative-manifest release",
      ),
      registry.lookup(IMAGE),
    )
  }

  @Test
  fun `a version the registry doesn't publish, such as a preview, isn't runnable`() {
    assertEquals(RegistryConnector.NotManifestOnly("the connector registry doesn't publish its metadata.yaml yet"), registry.lookup(IMAGE))
  }

  @Test
  fun `a manifest-only version without a published manifest isn't runnable`() {
    publish("metadata.yaml", metadata())

    assertEquals(RegistryConnector.NotManifestOnly("the connector registry doesn't publish its manifest.yaml yet"), registry.lookup(IMAGE))
  }

  @Test
  fun `a version the registry doesn't publish yet is looked up again once it is`() {
    assertEquals(RegistryConnector.NotManifestOnly("the connector registry doesn't publish its metadata.yaml yet"), registry.lookup(IMAGE))

    publishManifestOnly()
    assertEquals(RegistryConnector.ManifestOnly(Version("7.45.0"), Yamls.deserialize(MANIFEST)), registry.lookup(IMAGE))
  }

  @Test
  fun `an image without a version tag isn't looked up`() {
    assertEquals(RegistryConnector.NotManifestOnly("the image has no version tag"), registry.lookup("airbyte/source-sendgrid"))
    assertEquals(0, server.requestCount)
  }

  @Test
  fun `each version is looked up once`() {
    publishManifestOnly()

    registry.lookup(IMAGE)
    registry.lookup(IMAGE)

    assertEquals(2, server.requestCount)
  }

  @Test
  fun `an unreachable registry throws, and the next lookup tries again`() {
    files["$VERSION_PATH/metadata.yaml"] = MockResponse().setResponseCode(503)

    assertThrows<IOException> { registry.lookup(IMAGE) }

    publishManifestOnly()
    assertEquals(RegistryConnector.ManifestOnly(Version("7.45.0"), Yamls.deserialize(MANIFEST)), registry.lookup(IMAGE))
  }

  @Test
  fun `both reads share one timeout, so a slow registry can't take twice as long`() {
    files["$VERSION_PATH/metadata.yaml"] = MockResponse().setBody(metadata()).setHeadersDelay(700, TimeUnit.MILLISECONDS)
    files["$VERSION_PATH/manifest.yaml"] = MockResponse().setBody(MANIFEST).setHeadersDelay(700, TimeUnit.MILLISECONDS)
    val slowRegistry =
      ManifestOnlyConnectorRegistry(
        ManifestServerCheckConfig(registryBaseUrl = server.url("/").toString(), registryTimeout = Duration.ofSeconds(1)),
      )

    // Each read fits in 1s on its own; the manifest read gets only what the metadata read left.
    assertThrows<IOException> { slowRegistry.lookup(IMAGE) }
    assertEquals(2, server.requestCount)
  }
}
