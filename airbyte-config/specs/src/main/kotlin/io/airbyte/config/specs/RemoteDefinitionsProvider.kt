/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.constants.AirbyteCatalogConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.commons.yaml.Yamls
import io.airbyte.config.ActorType
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.ConnectorRegistry
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteConnectorRegistryConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.cache.annotation.CacheConfig
import io.micronaut.cache.annotation.Cacheable
import io.micronaut.http.HttpHeaders
import io.micronaut.http.MediaType
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.net.MalformedURLException
import java.net.URI
import java.net.URISyntaxException
import java.net.URL
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Optional
import java.util.UUID
import java.util.stream.Collectors
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream

/**
 * This provider pulls the definitions from a remotely hosted connector registry.
 */
@Singleton
@CacheConfig("remote-definitions-provider")
open class RemoteDefinitionsProvider(
  airbyteConfig: AirbyteConfig,
  airbyteConnectorRegistryConfig: AirbyteConnectorRegistryConfig,
) : DefinitionsProvider {
  private val okHttpClient: OkHttpClient

  private val remoteRegistryBaseUrl: URI
  private val airbyteEdition: AirbyteEdition

  private fun parsedRemoteRegistryBaseUrlOrDefault(remoteRegistryBaseUrl: String?): URI {
    try {
      return if (remoteRegistryBaseUrl == null || remoteRegistryBaseUrl.isEmpty()) {
        URI(AirbyteCatalogConstants.REMOTE_REGISTRY_BASE_URL)
      } else {
        URI(remoteRegistryBaseUrl)
      }
    } catch (e: URISyntaxException) {
      log.error(e) { "Invalid remote registry base URL: $remoteRegistryBaseUrl" }
      throw IllegalArgumentException("Remote connector registry base URL must be a valid URI.")
    }
  }

  init {
    val remoteRegistryBaseUrlUri = parsedRemoteRegistryBaseUrlOrDefault(airbyteConnectorRegistryConfig.remote.baseUrl)

    this.remoteRegistryBaseUrl = remoteRegistryBaseUrlUri
    this.airbyteEdition = airbyteConfig.edition
    this.okHttpClient =
      OkHttpClient
        .Builder()
        .callTimeout(Duration.ofMillis(airbyteConnectorRegistryConfig.remote.timeoutMs))
        .build()
    log.info { "Created remote definitions provider for URL '$remoteRegistryBaseUrlUri' and registry '${getRegistryName(airbyteEdition)}'..." }
  }

  private fun getSourceDefinitionsMap(): Map<UUID, ConnectorRegistrySourceDefinition> {
    val registry = getRemoteConnectorRegistry()
    return registry.sources.stream().collect(
      Collectors.toMap(
        { obj: ConnectorRegistrySourceDefinition -> obj.sourceDefinitionId },
        { source: ConnectorRegistrySourceDefinition ->
          source.withProtocolVersion(
            AirbyteProtocolVersion.getWithDefault(if (source.spec != null) source.spec.protocolVersion else null).serialize(),
          )
        },
      ),
    )
  }

  private fun getDestinationDefinitionsMap(): Map<UUID, ConnectorRegistryDestinationDefinition> {
    val catalog = getRemoteConnectorRegistry()
    return catalog.destinations.stream().collect(
      Collectors.toMap(
        { obj: ConnectorRegistryDestinationDefinition -> obj.destinationDefinitionId },
        { destination: ConnectorRegistryDestinationDefinition ->
          destination.withProtocolVersion(
            AirbyteProtocolVersion
              .getWithDefault(if (destination.spec != null) destination.spec.protocolVersion else null)
              .serialize(),
          )
        },
      ),
    )
  }

  override fun getSourceDefinition(definitionId: UUID): ConnectorRegistrySourceDefinition {
    val definition =
      getSourceDefinitionsMap()[definitionId]
        ?: throw RegistryDefinitionNotFoundException(ActorType.SOURCE, definitionId)
    return definition
  }

  /**
   * Gets the registry entry for a given source connector version. If no entry exists for the given
   * connector or version, an empty optional will be returned.
   */
  fun getSourceDefinitionByVersion(
    connectorRepository: String?,
    version: String?,
  ): Optional<ConnectorRegistrySourceDefinition> {
    val registryEntryJson = getConnectorRegistryEntryJson(connectorRepository, version)
    return registryEntryJson.map { jsonNode: JsonNode ->
      Jsons.`object`(
        jsonNode,
        ConnectorRegistrySourceDefinition::class.java,
      )
    }
  }

  override fun getSourceDefinitions(): List<ConnectorRegistrySourceDefinition> = ArrayList(getSourceDefinitionsMap().values)

  override fun getDestinationDefinition(definitionId: UUID): ConnectorRegistryDestinationDefinition {
    val definition =
      getDestinationDefinitionsMap()[definitionId]
        ?: throw RegistryDefinitionNotFoundException(ActorType.DESTINATION, definitionId)
    return definition
  }

  /**
   * Gets the registry entry for a given destination connector version. If no entry exists for the
   * given connector or version, an empty optional will be returned.
   */
  fun getDestinationDefinitionByVersion(
    connectorRepository: String?,
    version: String?,
  ): Optional<ConnectorRegistryDestinationDefinition> {
    val registryEntryJson = getConnectorRegistryEntryJson(connectorRepository, version)
    return registryEntryJson.map { jsonNode: JsonNode ->
      Jsons.`object`(
        jsonNode,
        ConnectorRegistryDestinationDefinition::class.java,
      )
    }
  }

  override fun getDestinationDefinitions(): List<ConnectorRegistryDestinationDefinition> = ArrayList(getDestinationDefinitionsMap().values)

  @InternalForTesting
  fun getRemoteRegistryUrlForPath(path: String): URL {
    try {
      return remoteRegistryBaseUrl.resolve(path).toURL()
    } catch (e: MalformedURLException) {
      throw RuntimeException("Invalid URL format", e)
    }
  }

  @get:InternalForTesting
  val registryPath: String
    get() = String.format("registries/v0/%s_registry.json", getRegistryName(airbyteEdition))

  @InternalForTesting
  fun getRegistryEntryPath(
    connectorRepository: String?,
    version: String?,
  ): String = String.format("metadata/%s/%s/%s.json", connectorRepository, version, getRegistryName(airbyteEdition))

  /**
   * Get remote connector registry.
   *
   * @return ConnectorRegistry
   */
  @Cacheable
  open fun getRemoteConnectorRegistry(): ConnectorRegistry {
    val registryUrl = getRemoteRegistryUrlForPath(registryPath)
    val request =
      Request
        .Builder()
        .url(registryUrl)
        .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
        .build()

    try {
      okHttpClient.newCall(request).execute().use { response ->
        if (response.isSuccessful && response.body != null) {
          val responseBody = response.body!!.string()
          log.info { "Fetched latest remote definitions (${responseBody.hashCode()})" }
          return Jsons.deserialize(responseBody, ConnectorRegistry::class.java)
        } else {
          throw IOException(formatStatusCodeException("getRemoteConnectorRegistry", response))
        }
      }
    } catch (e: Exception) {
      throw RuntimeException("Failed to fetch remote connector registry", e)
    }
  }

  @InternalForTesting
  fun getConnectorRegistryEntryJson(
    connectorName: String?,
    version: String?,
  ): Optional<JsonNode> {
    val registryEntryPath = getRemoteRegistryUrlForPath(getRegistryEntryPath(connectorName, version))
    val request =
      Request
        .Builder()
        .url(registryEntryPath)
        .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
        .build()

    try {
      okHttpClient.newCall(request).execute().use { response ->
        return if (response.code == NOT_FOUND) {
          Optional.empty()
        } else if (response.isSuccessful && response.body != null) {
          Optional.of(
            Jsons.deserialize(
              response.body!!.string(),
            ),
          )
        } else {
          throw IOException(formatStatusCodeException("getConnectorRegistryEntryJson", response))
        }
      }
    } catch (e: IOException) {
      throw RuntimeException(String.format("Failed to fetch connector registry entry for %s:%s", connectorName, version), e)
    }
  }

  /**
   * Retrieves the live documentation for a connector directly from GitHub.
   * This fetches the latest docs from the airbyte repo's master branch, which is the source
   * of truth for docs.airbyte.com. This ensures users see the most up-to-date documentation
   * even if the connector hasn't been republished.
   *
   * The documentationUrl field from ActorDefinitionVersion is used as the primary source
   * to derive the GitHub path. This is more reliable than parsing docker repository names
   * since it uses the same URL that docs.airbyte.com uses.
   *
   * @param documentationUrl The documentation URL from ActorDefinitionVersion (e.g., "https://docs.airbyte.com/integrations/sources/postgres")
   * @return Optional containing the live doc if it can be found, or empty otherwise.
   */
  fun getLiveConnectorDocumentation(documentationUrl: String?): Optional<String> {
    if (documentationUrl == null) {
      return Optional.empty()
    }

    val relativePath = extractPathFromDocumentationUrl(documentationUrl)
    if (relativePath == null) {
      log.debug { "Could not extract path from documentationUrl: $documentationUrl" }
      return Optional.empty()
    }

    val githubDocUrl = "$GITHUB_DOCS_BASE_URL/$relativePath.md"
    return fetchDocFromUrl(githubDocUrl)
  }

  /**
   * Fetches documentation from a GitHub URL.
   * Returns Optional.empty() on 404 or any error (to allow fallback to next path or registry).
   */
  private fun fetchDocFromUrl(githubDocUrl: String): Optional<String> {
    val request =
      Request
        .Builder()
        .url(githubDocUrl)
        .header(HttpHeaders.ACCEPT, MediaType.TEXT_PLAIN)
        .build()

    try {
      okHttpClient.newCall(request).execute().use { response ->
        return if (response.code == NOT_FOUND) {
          log.debug { "Live documentation not found at $githubDocUrl" }
          Optional.empty()
        } else if (response.isSuccessful && response.body != null) {
          log.info { "Successfully fetched live documentation from $githubDocUrl" }
          Optional.of(response.body!!.string())
        } else {
          log.warn { "Failed to fetch live documentation from $githubDocUrl: ${response.code} ${response.message}" }
          Optional.empty()
        }
      }
    } catch (e: IOException) {
      log.warn(e) { "Error fetching live documentation from $githubDocUrl" }
      return Optional.empty()
    }
  }

  /**
   * Retrieves the full or inapp documentation for the specified connector repo and version.
   *
   * @return Optional containing the connector doc if it can be found, or empty otherwise.
   */
  fun getConnectorDocumentation(
    connectorRepository: String?,
    version: String?,
  ): Optional<String> {
    val docUrl = getRemoteRegistryUrlForPath(getDocPath(connectorRepository, version))
    val request =
      Request
        .Builder()
        .url(docUrl)
        .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
        .build()

    try {
      okHttpClient.newCall(request).execute().use { response ->
        return if (response.code == NOT_FOUND) {
          Optional.empty()
        } else if (response.isSuccessful && response.body != null) {
          Optional.of(response.body!!.string())
        } else {
          throw IOException(formatStatusCodeException("getConnectorDocumentation", response))
        }
      }
    } catch (e: IOException) {
      throw RuntimeException(
        String.format("Failed to fetch connector documentation for %s:%s", connectorRepository, version),
        e,
      )
    }
  }

  /**
   * Retrieves the manifest for the specified connector repo and version.
   *
   * @return Optional containing the connector manifest if it can be found, or empty otherwise.
   */
  fun getConnectorManifest(
    connectorRepository: String?,
    version: String?,
  ): Optional<JsonNode> {
    val manifestUrl = getRemoteRegistryUrlForPath(getManifestPath(connectorRepository, version))
    val request =
      Request
        .Builder()
        .url(manifestUrl)
        .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
        .build()

    try {
      okHttpClient.newCall(request).execute().use { response ->
        if (response.code == NOT_FOUND) {
          return Optional.empty()
        } else if (response.isSuccessful && response.body != null) {
          val manifestYamlContent = response.body!!.string()
          return Optional.of(Yamls.deserialize(manifestYamlContent))
        } else {
          throw IOException(formatStatusCodeException("getConnectorManifest", response))
        }
      }
    } catch (e: IOException) {
      throw RuntimeException(
        String.format("Failed to fetch manifest file for %s:%s", connectorRepository, version),
        e,
      )
    }
  }

  /**
   * Retrieves the (optional) components.py content for the specified connector repo and version.
   *
   * @return Optional containing the connector manifest if it can be found, or empty otherwise.
   */
  fun getConnectorCustomComponents(
    connectorRepository: String?,
    version: String?,
  ): Optional<String> {
    val manifestUrl = getRemoteRegistryUrlForPath(getComponentsZipPath(connectorRepository, version))
    val request =
      Request
        .Builder()
        .url(manifestUrl)
        .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
        .build()

    try {
      okHttpClient.newCall(request).execute().use { response ->
        return if (response.code == NOT_FOUND) {
          Optional.empty()
        } else if (response.isSuccessful && response.body != null) {
          extractFileContentFromZip(response.body!!.bytes(), CUSTOM_COMPONENTS_FILE_NAME)
        } else {
          throw IOException(formatStatusCodeException("getConnectorCustomComponents", response))
        }
      }
    } catch (e: IOException) {
      throw RuntimeException(
        String.format("Failed to fetch custom components file for %s:%s", connectorRepository, version),
        e,
      )
    }
  }

  /**
   * Extracts a specific file from a zip file byte array.
   *
   * @param zipBytes The byte array containing the zip file
   * @param fileName The name of the file to extract
   * @return Optional containing the file contents if found, empty otherwise
   * @throws IOException if there is an error reading the zip file
   */
  private fun extractFileContentFromZip(
    zipBytes: ByteArray,
    fileName: String,
  ): Optional<String> {
    ZipInputStream(ByteArrayInputStream(zipBytes)).use { zipStream ->
      return findAndExtractFile(zipStream, fileName)
    }
  }

  /**
   * Finds and extracts a specific file from a ZipInputStream.
   */
  private fun findAndExtractFile(
    zipStream: ZipInputStream,
    fileName: String,
  ): Optional<String> {
    var entry: ZipEntry?
    while ((zipStream.nextEntry.also { entry = it }) != null) {
      if (entry!!.name == fileName) {
        return Optional.of(readStreamToString(zipStream))
      }
    }
    // Log a warning if the file was not found
    log.warn { "File $fileName not found in zip stream" }
    return Optional.empty()
  }

  /**
   * Reads a stream into a String using UTF-8 encoding.
   */
  private fun readStreamToString(inputStream: InputStream): String {
    val outputStream = ByteArrayOutputStream()
    inputStream.transferTo(outputStream)
    return outputStream.toString(StandardCharsets.UTF_8)
  }

  private fun formatStatusCodeException(
    operationName: String,
    response: Response,
  ): String = String.format("%s request ran into status code error: %d with message: %s", operationName, response.code, response.message)

  companion object {
    private val log = KotlinLogging.logger {}

    private const val NOT_FOUND = 404
    private const val CUSTOM_COMPONENTS_FILE_NAME = "components.py"
    private const val GITHUB_DOCS_BASE_URL = "https://raw.githubusercontent.com/airbytehq/airbyte/master/docs/integrations"

    /**
     * Extracts the relative path from a docs.airbyte.com documentation URL.
     * For example:
     * - "https://docs.airbyte.com/integrations/sources/postgres" -> "sources/postgres"
     * - "https://docs.airbyte.com/integrations/enterprise-connectors/source-oracle-enterprise" -> "enterprise-connectors/source-oracle-enterprise"
     *
     * Returns null if the URL is not a valid docs.airbyte.com integrations URL.
     */
    @JvmStatic
    @InternalForTesting
    fun extractPathFromDocumentationUrl(documentationUrl: String): String? =
      try {
        val uri = java.net.URI(documentationUrl)
        if (uri.host != "docs.airbyte.com") {
          null
        } else {
          val rawPath = uri.path.removePrefix("/")
          if (!rawPath.startsWith("integrations/")) {
            null
          } else {
            // "integrations/sources/postgres" -> "sources/postgres"
            rawPath.removePrefix("integrations/")
          }
        }
      } catch (e: java.net.URISyntaxException) {
        log.warn(e) { "Invalid documentationUrl: $documentationUrl" }
        null
      }

    @JvmStatic
    fun getRegistryName(airbyteEdition: AirbyteEdition): String =
      when (airbyteEdition) {
        AirbyteEdition.COMMUNITY, AirbyteEdition.ENTERPRISE -> "oss"
        AirbyteEdition.CLOUD -> "cloud"
      }

    @JvmStatic
    @InternalForTesting
    fun getDocPath(
      connectorRepository: String?,
      version: String?,
    ): String = String.format("metadata/%s/%s/doc.md", connectorRepository, version)

    @JvmStatic
    @InternalForTesting
    fun getManifestPath(
      connectorRepository: String?,
      version: String?,
    ): String = String.format("metadata/%s/%s/manifest.yaml", connectorRepository, version)

    @InternalForTesting
    fun getComponentsZipPath(
      connectorRepository: String?,
      version: String?,
    ): String = String.format("metadata/%s/%s/components.zip", connectorRepository, version)
  }
}
