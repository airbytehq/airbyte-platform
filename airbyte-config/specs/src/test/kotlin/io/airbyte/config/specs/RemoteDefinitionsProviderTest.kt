/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.io.Resources
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.yaml.Yamls
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.SupportLevel
import io.airbyte.config.specs.RemoteDefinitionsProvider.Companion.getDocPath
import io.airbyte.config.specs.RemoteDefinitionsProvider.Companion.getManifestPath
import io.airbyte.config.specs.RemoteDefinitionsProvider.Companion.getRegistryName
import io.airbyte.protocol.models.v0.ConnectorSpecification
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okio.Buffer
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InterruptedIOException
import java.net.URI
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

internal class RemoteDefinitionsProviderTest {
  private var webServer: MockWebServer? = null
  private var validCatalogResponse: MockResponse? = null
  private var baseUrl: String? = null
  private var jsonCatalog: JsonNode? = null

  @BeforeEach
  @Throws(IOException::class)
  fun setup() {
    webServer = MockWebServer()
    baseUrl = webServer!!.url("/").toString()

    val testCatalog = Resources.getResource("connector_catalog.json")
    val jsonBody = Resources.toString(testCatalog, Charset.defaultCharset())
    jsonCatalog = Jsons.deserialize(jsonBody)
    validCatalogResponse =
      MockResponse()
        .setResponseCode(200)
        .addHeader("Content-Type", "application/json; charset=utf-8")
        .addHeader("Cache-Control", "no-cache")
        .setBody(jsonBody)
  }

  // Helper method to create a test zip file with specified content
  @Throws(IOException::class)
  private fun createTestZip(
    filename: String,
    content: String,
  ): ByteArray {
    val baos = ByteArrayOutputStream()
    ZipOutputStream(baos).use { zos ->
      val entry = ZipEntry(filename)
      zos.putNextEntry(entry)
      zos.write(content.toByteArray(StandardCharsets.UTF_8))
      zos.closeEntry()
    }
    return baos.toByteArray()
  }

  @Test
  @Throws(Exception::class)
  fun testGetSourceDefinition() {
    webServer!!.enqueue(validCatalogResponse!!)
    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val stripeSourceId = UUID.fromString("e094cb9a-26de-4645-8761-65c0c425d1de")
    val stripeSource = remoteDefinitionsProvider.getSourceDefinition(stripeSourceId)
    Assertions.assertEquals(stripeSourceId, stripeSource.sourceDefinitionId)
    Assertions.assertEquals("Stripe", stripeSource.name)
    Assertions.assertEquals("airbyte/source-stripe", stripeSource.dockerRepository)
    Assertions.assertEquals("https://docs.airbyte.io/integrations/sources/stripe", stripeSource.documentationUrl)
    Assertions.assertEquals("stripe.svg", stripeSource.icon)
    Assertions.assertEquals(URI.create("https://docs.airbyte.io/integrations/sources/stripe"), stripeSource.spec.documentationUrl)
    Assertions.assertEquals(false, stripeSource.tombstone)
    Assertions.assertEquals("0.2.1", stripeSource.protocolVersion)
    Assertions.assertEquals(SupportLevel.COMMUNITY, stripeSource.supportLevel)
    Assertions.assertEquals(300L, stripeSource.abInternal.sl)
  }

  @Test
  @Throws(Exception::class)
  fun testGetDestinationDefinition() {
    webServer!!.enqueue(validCatalogResponse!!)
    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val s3DestinationId = UUID.fromString("4816b78f-1489-44c1-9060-4b19d5fa9362")
    val s3Destination =
      remoteDefinitionsProvider
        .getDestinationDefinition(s3DestinationId)
    Assertions.assertEquals(s3DestinationId, s3Destination.destinationDefinitionId)
    Assertions.assertEquals("S3", s3Destination.name)
    Assertions.assertEquals("airbyte/destination-s3", s3Destination.dockerRepository)
    Assertions.assertEquals("https://docs.airbyte.io/integrations/destinations/s3", s3Destination.documentationUrl)
    Assertions.assertEquals(URI.create("https://docs.airbyte.io/integrations/destinations/s3"), s3Destination.spec.documentationUrl)
    Assertions.assertEquals(false, s3Destination.tombstone)
    Assertions.assertEquals("0.2.2", s3Destination.protocolVersion)
    Assertions.assertEquals(SupportLevel.COMMUNITY, s3Destination.supportLevel)
    Assertions.assertEquals(300L, s3Destination.abInternal.sl)
  }

  @Test
  fun testGetInvalidDefinitionId() {
    webServer!!.enqueue(validCatalogResponse!!)
    webServer!!.enqueue(validCatalogResponse!!)

    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val invalidDefinitionId = UUID.fromString("1a7c360c-1289-4b96-a171-2ac1c86fb7ca")

    Assertions.assertThrows(
      RegistryDefinitionNotFoundException::class.java,
    ) { remoteDefinitionsProvider.getSourceDefinition(invalidDefinitionId) }
    Assertions.assertThrows(
      RegistryDefinitionNotFoundException::class.java,
    ) { remoteDefinitionsProvider.getDestinationDefinition(invalidDefinitionId) }
  }

  @Test
  fun testGetSourceDefinitions() {
    webServer!!.enqueue(validCatalogResponse!!)
    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val sourceDefinitions = remoteDefinitionsProvider.getSourceDefinitions()
    val expectedNumberOfSources =
      jsonCatalog!!["sources"]
        .elements()
        .asSequence()
        .toList()
        .size
    Assertions.assertEquals(expectedNumberOfSources, sourceDefinitions.size)
    Assertions.assertTrue(
      sourceDefinitions.stream().allMatch { sourceDef: ConnectorRegistrySourceDefinition -> sourceDef.protocolVersion.length > 0 },
    )
  }

  @Test
  fun testGetDestinationDefinitions() {
    webServer!!.enqueue(validCatalogResponse!!)
    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val destinationDefinitions = remoteDefinitionsProvider.getDestinationDefinitions()
    val expectedNumberOfDestinations =
      jsonCatalog!!["destinations"]
        .elements()
        .asSequence()
        .toList()
        .size
    Assertions.assertEquals(expectedNumberOfDestinations, destinationDefinitions.size)
    Assertions.assertTrue(
      destinationDefinitions.stream().allMatch { destDef: ConnectorRegistryDestinationDefinition -> destDef.protocolVersion.length > 0 },
    )

    val destinationDefinitionWithFileTransfer =
      getDestinationDefinitionById(destinationDefinitions, UUID.fromString("0eeee7fb-518f-4045-bacc-9619e31c43ea"))
    Assertions.assertTrue(destinationDefinitionWithFileTransfer.supportsFileTransfer)

    val destinationDefinitionWithNoFileTransfer =
      getDestinationDefinitionById(destinationDefinitions, UUID.fromString("b4c5d105-31fd-4817-96b6-cb923bfc04cb"))
    Assertions.assertFalse(destinationDefinitionWithNoFileTransfer.supportsFileTransfer)

    val destinationDefinitionWithoutFileTransfer =
      getDestinationDefinitionById(destinationDefinitions, UUID.fromString("22f6c74f-5699-40ff-833c-4a879ea40133"))
    Assertions.assertFalse(destinationDefinitionWithoutFileTransfer.supportsFileTransfer)
  }

  private fun getDestinationDefinitionById(
    destDefs: List<ConnectorRegistryDestinationDefinition>,
    destDefId: UUID,
  ): ConnectorRegistryDestinationDefinition =
    destDefs
      .stream()
      .filter { destDef: ConnectorRegistryDestinationDefinition -> destDef.destinationDefinitionId == destDefId }
      .findFirst()
      .orElseThrow()

  @Test
  fun testBadResponseStatus() {
    webServer!!.enqueue(MockResponse().setResponseCode(404))
    val ex =
      Assertions.assertThrows(
        RuntimeException::class.java,
      ) {
        RemoteDefinitionsProvider(
          baseUrl,
          AIRBYTE_EDITION,
          TimeUnit.SECONDS.toMillis(1),
        ).getDestinationDefinitions()
      }

    Assertions.assertTrue(ex.message!!.contains("Failed to fetch remote connector registry"))
    Assertions.assertInstanceOf(IOException::class.java, ex.cause)
  }

  @Test
  fun testTimeOut() {
    // No request enqueued -> Timeout
    val ex =
      Assertions.assertThrows(
        RuntimeException::class.java,
      ) {
        RemoteDefinitionsProvider(
          baseUrl,
          AIRBYTE_EDITION,
          TimeUnit.SECONDS.toMillis(1),
        ).getDestinationDefinitions()
      }

    Assertions.assertTrue(ex.message!!.contains("Failed to fetch remote connector registry"))
    Assertions.assertEquals(ex.cause!!.javaClass, InterruptedIOException::class.java)
  }

  @Test
  fun testNonJson() {
    val notJson = makeResponse(200, "not json")
    webServer!!.enqueue(notJson)
    Assertions.assertThrows(
      RuntimeException::class.java,
    ) {
      RemoteDefinitionsProvider(
        baseUrl,
        AIRBYTE_EDITION,
        TimeUnit.SECONDS.toMillis(1),
      ).getDestinationDefinitions()
    }
  }

  @ParameterizedTest
  @EnumSource(AirbyteEdition::class)
  fun testGetPaths(airbyteEdition: AirbyteEdition) {
    val definitionsProvider =
      RemoteDefinitionsProvider(baseUrl, airbyteEdition, TimeUnit.SECONDS.toMillis(1))
    val connectorName = "airbyte/source-github"
    val version = "1.0.0"

    val registryPath = definitionsProvider.registryPath
    Assertions.assertEquals(
      String.format(
        "registries/v0/%s_registry.json",
        getRegistryName(airbyteEdition),
      ),
      registryPath,
    )

    val metadataPath = definitionsProvider.getRegistryEntryPath(connectorName, version)
    Assertions.assertEquals(
      String.format(
        "metadata/airbyte/source-github/1.0.0/%s.json",
        getRegistryName(airbyteEdition),
      ),
      metadataPath,
    )

    val docsPath = getDocPath(connectorName, version)
    Assertions.assertEquals("metadata/airbyte/source-github/1.0.0/doc.md", docsPath)

    val manifestPath = getManifestPath(connectorName, version)
    Assertions.assertEquals("metadata/airbyte/source-github/1.0.0/manifest.yaml", manifestPath)
  }

  @Test
  fun getRemoteRegistryUrlForPath() {
    val baseUrl = "https://connectors.airbyte.com/files/"
    val definitionsProvider =
      RemoteDefinitionsProvider(
        baseUrl,
        AIRBYTE_EDITION,
        TimeUnit.SECONDS.toMillis(1),
      )
    val registryPath = "registries/v0/oss_registry.json"

    val registryUrl = definitionsProvider.getRemoteRegistryUrlForPath(registryPath)
    Assertions.assertEquals(
      "https://connectors.airbyte.com/files/registries/v0/oss_registry.json",
      registryUrl.toString(),
    )
  }

  @Test
  fun testGetMissingEntryByVersion() {
    webServer!!.enqueue(makeResponse(404, "not found"))
    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val definition = remoteDefinitionsProvider.getConnectorRegistryEntryJson(CONNECTOR_REPOSITORY, CONNECTOR_VERSION)
    Assertions.assertTrue(definition.isEmpty)
  }

  @Test
  fun testGetEntryByVersion() {
    val sourceDef =
      ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("source")
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withSpec(ConnectorSpecification().withConnectionSpecification(Jsons.emptyObject()))

    val validResponse = makeResponse(200, Jsons.serialize(sourceDef))

    webServer!!.enqueue(validResponse)
    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val definition = remoteDefinitionsProvider.getConnectorRegistryEntryJson(CONNECTOR_REPOSITORY, CONNECTOR_VERSION)
    Assertions.assertTrue(definition.isPresent)
    Assertions.assertEquals(Jsons.jsonNode(sourceDef), definition.get())
  }

  @Test
  fun testGetSourceDefinitionByVersion() {
    val sourceDef =
      ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("source")
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withSpec(ConnectorSpecification().withConnectionSpecification(Jsons.emptyObject()))

    val validResponse = makeResponse(200, Jsons.serialize(sourceDef))
    webServer!!.enqueue(validResponse)

    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val definition =
      remoteDefinitionsProvider.getSourceDefinitionByVersion(CONNECTOR_REPOSITORY, CONNECTOR_VERSION)
    Assertions.assertTrue(definition.isPresent)
    Assertions.assertEquals(sourceDef, definition.get())
  }

  @Test
  fun testGetDestinationDefinitionByVersion() {
    val destinationDef =
      ConnectorRegistryDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withName("destination")
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withSpec(ConnectorSpecification().withConnectionSpecification(Jsons.emptyObject()))

    val validResponse = makeResponse(200, Jsons.serialize(destinationDef))
    webServer!!.enqueue(validResponse)

    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val definition =
      remoteDefinitionsProvider.getDestinationDefinitionByVersion(CONNECTOR_REPOSITORY, CONNECTOR_VERSION)
    Assertions.assertTrue(definition.isPresent)
    Assertions.assertEquals(destinationDef, definition.get())
  }

  @Test
  fun testGetConnectorDocumentation() {
    val connectorDocumentationBody = "The documentation contents"

    val validResponse = makeResponse(200, connectorDocumentationBody)
    webServer!!.enqueue(validResponse)

    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val documentationResult = remoteDefinitionsProvider.getConnectorDocumentation(CONNECTOR_REPOSITORY, CONNECTOR_VERSION)
    Assertions.assertTrue(documentationResult.isPresent)
    Assertions.assertEquals(documentationResult.get(), connectorDocumentationBody)
  }

  @Test
  fun testGetMissingConnectorDocumentation() {
    webServer!!.enqueue(makeResponse(404, "not found"))
    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val documentationResult = remoteDefinitionsProvider.getConnectorDocumentation(CONNECTOR_REPOSITORY, CONNECTOR_VERSION)
    Assertions.assertTrue(documentationResult.isEmpty)
  }

  @Test
  fun testGetConnectorManifest() {
    val connectorManifestBody = "key: value"

    val validResponse = makeResponse(200, connectorManifestBody)
    webServer!!.enqueue(validResponse)

    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val manifestResult = remoteDefinitionsProvider.getConnectorManifest(CONNECTOR_REPOSITORY, CONNECTOR_VERSION)
    Assertions.assertTrue(manifestResult.isPresent)
    Assertions.assertEquals(manifestResult.get(), Yamls.deserialize(connectorManifestBody))
  }

  @Test
  @Throws(IOException::class)
  fun testGetConnectorCustomComponents() {
    // Create a zip file containing components.py with test content
    val expectedContent = "def test_function():\n    return 'test'"
    val zipBytes = createTestZip("components.py", expectedContent)

    val validResponse =
      MockResponse()
        .setResponseCode(200)
        .addHeader("Content-Type", "application/zip")
        .setBody(Buffer().write(zipBytes))

    webServer!!.enqueue(validResponse)

    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val customComponentsResult =
      remoteDefinitionsProvider.getConnectorCustomComponents(CONNECTOR_REPOSITORY, CONNECTOR_VERSION)

    Assertions.assertTrue(customComponentsResult.isPresent)
    Assertions.assertEquals(expectedContent, customComponentsResult.get())
  }

  @Test
  fun testGetMissingConnectorCustomComponents() {
    webServer!!.enqueue(makeResponse(404, "not found"))
    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val customComponentsResult =
      remoteDefinitionsProvider.getConnectorCustomComponents(CONNECTOR_REPOSITORY, CONNECTOR_VERSION)
    Assertions.assertTrue(customComponentsResult.isEmpty)
  }

  @Test
  @Throws(IOException::class)
  fun testGetConnectorCustomComponentsWithNoComponentsFile() {
    // Create a zip file containing a different file
    val content = "some other content"
    val zipBytes = createTestZip("different-file.txt", content)

    val validResponse =
      MockResponse()
        .setResponseCode(200)
        .addHeader("Content-Type", "application/zip")
        .setBody(Buffer().write(zipBytes))

    webServer!!.enqueue(validResponse)

    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val customComponentsResult =
      remoteDefinitionsProvider.getConnectorCustomComponents(CONNECTOR_REPOSITORY, CONNECTOR_VERSION)

    // Verify that an empty Optional is returned when components.py is not found
    Assertions.assertTrue(customComponentsResult.isEmpty)
  }

  @Test
  fun testGetConnectorCustomComponentsWithMalformedZip() {
    // Create an invalid zip file (just some random bytes)
    val invalidZipBytes = "not a valid zip file content".toByteArray(StandardCharsets.UTF_8)

    val invalidResponse =
      MockResponse()
        .setResponseCode(200)
        .addHeader("Content-Type", "application/zip")
        .setBody(Buffer().write(invalidZipBytes))

    webServer!!.enqueue(invalidResponse)

    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(baseUrl, AIRBYTE_EDITION, TimeUnit.SECONDS.toMillis(30))
    val customComponentsResult =
      remoteDefinitionsProvider.getConnectorCustomComponents(CONNECTOR_REPOSITORY, CONNECTOR_VERSION)

    // Verify that an empty Optional is returned when the zip is malformed
    Assertions.assertTrue(customComponentsResult.isEmpty)
  }

  @Test
  fun missingConnectorManifest() {
    webServer!!.enqueue(makeResponse(404, "not found"))
    val remoteDefinitionsProvider =
      RemoteDefinitionsProvider(
        baseUrl,
        AIRBYTE_EDITION,
        TimeUnit.SECONDS.toMillis(30),
      )
    val manifestResult =
      remoteDefinitionsProvider.getConnectorManifest(
        CONNECTOR_REPOSITORY,
        CONNECTOR_VERSION,
      )
    Assertions.assertTrue(manifestResult.isEmpty)
  }

  companion object {
    private val AIRBYTE_EDITION = AirbyteEdition.COMMUNITY
    private const val CONNECTOR_REPOSITORY = "airbyte/source-stripe"
    private const val CONNECTOR_VERSION = "0.2.1"

    fun makeResponse(
      statusCode: Int,
      body: String,
    ): MockResponse =
      MockResponse()
        .setResponseCode(statusCode)
        .addHeader("Content-Type", "application/json; charset=utf-8")
        .addHeader("Cache-Control", "no-cache")
        .setBody(body)
  }
}
