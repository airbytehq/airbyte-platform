/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.controllers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.protocol.AirbyteMessageMigrator
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory
import io.airbyte.commons.protocol.ConfiguredAirbyteCatalogMigrator
import io.airbyte.commons.protocol.serde.AirbyteMessageV0Deserializer
import io.airbyte.commons.protocol.serde.AirbyteMessageV0Serializer
import io.airbyte.commons.resources.Resources
import io.airbyte.commons.server.builder.contributions.ContributionTemplates
import io.airbyte.commons.server.builder.exceptions.ConnectorBuilderException
import io.airbyte.commons.server.handlers.AssistProxyHandler
import io.airbyte.commons.server.handlers.ConnectorContributionHandler
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifestRequestBody
import io.airbyte.connectorbuilder.api.model.generated.StreamReadRequestBody
import io.airbyte.connectorbuilder.commandrunner.MockSynchronousPythonCdkCommandRunner
import io.airbyte.connectorbuilder.commandrunner.SynchronousCdkCommandRunner
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.airbyte.connectorbuilder.exceptions.CdkProcessException
import io.airbyte.connectorbuilder.exceptions.CdkUnknownException
import io.airbyte.connectorbuilder.filewriter.MockAirbyteFileWriterImpl
import io.airbyte.connectorbuilder.handlers.FullResolveManifestHandler
import io.airbyte.connectorbuilder.handlers.HealthHandler
import io.airbyte.connectorbuilder.handlers.ResolveManifestHandler
import io.airbyte.connectorbuilder.handlers.StreamHandler
import io.airbyte.connectorbuilder.requester.AirbyteCdkRequesterImpl
import io.airbyte.workers.helper.GsonPksExtractor
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.util.Optional

internal class ConnectorBuilderControllerIntegrationTest {
  private lateinit var healthHandler: HealthHandler
  private lateinit var writer: MockAirbyteFileWriterImpl
  private lateinit var streamFactory: AirbyteStreamFactory
  private lateinit var contributionTemplates: ContributionTemplates
  private lateinit var assistProxyHandler: AssistProxyHandler

  @BeforeEach
  fun setup() {
    healthHandler = mockk()
    writer = MockAirbyteFileWriterImpl()

    val serDeProvider =
      AirbyteMessageSerDeProvider(
        listOf(AirbyteMessageV0Deserializer()),
        listOf(AirbyteMessageV0Serializer()),
      )
    serDeProvider.initialize()
    val airbyteMessageMigrator = AirbyteMessageMigrator(listOf())
    airbyteMessageMigrator.initialize()
    val configuredAirbyteCatalogMigrator = ConfiguredAirbyteCatalogMigrator(listOf())
    configuredAirbyteCatalogMigrator.initialize()
    val migratorFactory = AirbyteProtocolVersionedMigratorFactory(airbyteMessageMigrator, configuredAirbyteCatalogMigrator)

    streamFactory =
      VersionedAirbyteStreamFactory<Any>(
        serDeProvider = serDeProvider,
        migratorFactory = migratorFactory,
        protocolVersion = AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION,
        connectionId = Optional.empty(),
        configuredAirbyteCatalog = Optional.empty(),
        invalidLineFailureConfiguration = VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration(false),
        gsonPksExtractor = GsonPksExtractor(),
        metricClient = mockk(relaxed = true),
      )
    contributionTemplates = ContributionTemplates()
    assistProxyHandler = mockk()
  }

  fun createControllerWithSynchronousRunner(
    shouldThrow: Boolean,
    exitCode: Int,
    inputStream: InputStream?,
    errorStream: InputStream?,
    outputStream: OutputStream?,
  ): ConnectorBuilderController {
    val commandRunner: SynchronousCdkCommandRunner =
      MockSynchronousPythonCdkCommandRunner(
        writer = writer,
        streamFactory = streamFactory,
        shouldThrow = shouldThrow,
        exitCode = exitCode,
        inputStream = inputStream,
        errorStream = errorStream,
        outputStream = outputStream,
      )
    val requester = AirbyteCdkRequesterImpl(commandRunner)
    return ConnectorBuilderController(
      healthHandler,
      ResolveManifestHandler(requester),
      FullResolveManifestHandler(requester),
      StreamHandler(requester),
      ConnectorContributionHandler(contributionTemplates, null),
      assistProxyHandler,
    )
  }

  @Test
  fun testStreamRead() {
    val controller = givenAirbyteCdkReturnMessage(streamRead)
    val streamRead =
      controller.readStream(
        StreamReadRequestBody().config(A_CONFIG).manifest(A_MANIFEST).stream(
          A_STREAM,
        ),
      )
    assertFalse(streamRead.getLogs().isEmpty())
    assertFalse(streamRead.getSlices().isEmpty())
    assertNotNull(streamRead.getInferredSchema())
    assertFalse(streamRead.getTestReadLimitReached())
  }

  @Test
  fun testStreamReadWithOptionalInputs() {
    val controller = givenAirbyteCdkReturnMessage(streamRead)
    val streamRead =
      controller.readStream(
        StreamReadRequestBody()
          .config(A_CONFIG)
          .manifest(A_MANIFEST)
          .stream(A_STREAM)
          .formGeneratedManifest(true),
      )
    assertFalse(streamRead.getLogs().isEmpty())
    assertFalse(streamRead.getSlices().isEmpty())
    assertNotNull(streamRead.getInferredSchema())
    assertFalse(streamRead.getTestReadLimitReached())
  }

  @Test
  fun givenTraceMessageWhenStreamReadThenThrowException() {
    val controller = givenAirbyteCdkReturnMessage(traceManifestResolve)
    assertThrows<AirbyteCdkInvalidInputException> {
      controller.readStream(
        StreamReadRequestBody().config(A_CONFIG).manifest(A_MANIFEST).stream(A_STREAM),
      )
    }
  }

  @Test
  fun testResolveManifestSuccess() {
    val stream: InputStream =
      ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(recordManifestResolve).array(),
      )

    val controller =
      createControllerWithSynchronousRunner(
        false,
        0,
        stream,
        null,
        null,
      )
    val resolveManifestRequestBody = ResolveManifestRequestBody()
    resolveManifestRequestBody.setManifest(validManifest)

    val resolvedManifest = controller.resolveManifest(resolveManifestRequestBody)
    assertNotNull(resolvedManifest.getManifest())
  }

  @Test
  fun testResolveManifestNoRecordsReturnsError() {
    val controller =
      createControllerWithSynchronousRunner(
        false,
        0,
        null,
        null,
        null,
      )

    val resolveManifestRequestBody = ResolveManifestRequestBody()
    resolveManifestRequestBody.setManifest(validManifest)

    val exception: Exception = assertThrows<CdkUnknownException> { controller.resolveManifest(resolveManifestRequestBody) }
    assertTrue(exception.message?.contains("no records nor trace were found") == true)
    assertNotNull(exception.getStackTrace())
  }

  @Test
  fun testResolveManifestTraceResponseReturnsError() {
    val stream: InputStream =
      ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(traceManifestResolve).array(),
      )

    val controller =
      createControllerWithSynchronousRunner(
        false,
        0,
        stream,
        null,
        null,
      )

    val resolveManifestRequestBody = ResolveManifestRequestBody()
    resolveManifestRequestBody.setManifest(validManifest)

    val exception =
      assertThrows<AirbyteCdkInvalidInputException>
      { controller.resolveManifest(resolveManifestRequestBody) }

    assertTrue(exception.message?.contains("AirbyteTraceMessage response from CDK") == true)
    assertNotNull(exception.getStackTrace())
  }

  @Test
  fun testResolveManifestNonzeroExitCodeReturnsError() {
    val errorStream: InputStream =
      ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(cdkException).array(),
      )

    val controller =
      createControllerWithSynchronousRunner(
        false,
        1,
        null,
        errorStream,
        null,
      )

    val resolveManifestRequestBody = ResolveManifestRequestBody()
    resolveManifestRequestBody.setManifest(validManifest)

    val exception: Exception = assertThrows<CdkProcessException> { controller.resolveManifest(resolveManifestRequestBody) }
    assertTrue(exception.message?.contains("CDK subprocess for resolve_manifest finished with exit code 1.") == true)
    assertNotNull(exception.getStackTrace())
  }

  @Test
  fun testResolveManifestNonzeroExitCodeAndInputStreamReturnsError() {
    val emptyInputStream: InputStream =
      object : InputStream() {
        override fun read(): Int = -1
      }
    val errorStream: InputStream =
      ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(cdkException).array(),
      )

    val controller =
      createControllerWithSynchronousRunner(
        false,
        1,
        emptyInputStream,
        errorStream,
        null,
      )

    val resolveManifestRequestBody = ResolveManifestRequestBody()
    resolveManifestRequestBody.setManifest(validManifest)

    val exception: Exception = assertThrows<CdkProcessException> { controller.resolveManifest(resolveManifestRequestBody) }
    assertTrue(exception.message?.contains("CDK subprocess for resolve_manifest finished with exit code 1.") == true)
    assertNotNull(exception.stackTrace)
  }

  @Test
  fun testResolveManifestServerExceptionReturnsError() {
    val stream: InputStream =
      ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(recordManifestResolve).array(),
      )

    val controller =
      createControllerWithSynchronousRunner(
        true,
        0,
        stream,
        null,
        null,
      )

    val resolveManifestRequestBody = ResolveManifestRequestBody()
    resolveManifestRequestBody.setManifest(validManifest)

    val exception: Exception =
      assertThrows<ConnectorBuilderException> { controller.resolveManifest(resolveManifestRequestBody) }
    assertTrue(exception.message?.contains("Error handling resolve_manifest request.") == true)
    assertNotNull(exception.stackTrace)
  }

  private fun givenAirbyteCdkReturnMessage(message: String): ConnectorBuilderController {
    val stream: InputStream = ByteArrayInputStream(StandardCharsets.UTF_8.encode(message).array())
    return createControllerWithSynchronousRunner(false, 0, stream, null, null)
  }

  companion object {
    private val A_CONFIG: JsonNode = ObjectMapper().readTree("""{"config": 1}""")
    private val A_MANIFEST: JsonNode = ObjectMapper().readTree("""{"manifest": 1}""")
    private const val A_STREAM = "stream"

    lateinit var cdkException: String
    lateinit var streamRead: String
    lateinit var recordManifestResolve: String
    lateinit var traceManifestResolve: String
    lateinit var validManifest: JsonNode

    @BeforeAll
    @JvmStatic
    fun setUpClass() {
      validManifest = ObjectMapper().readTree(readContents("fixtures/ValidManifest.json"))
      streamRead = readContents("fixtures/RecordStreamRead.json")
      recordManifestResolve = readContents("fixtures/RecordManifestResolve.json")
      traceManifestResolve = readContents("fixtures/TraceManifestResolve.json")
      cdkException = readContents("fixtures/CdkException.txt")
    }

    fun readContents(filepath: String): String = Resources.read(filepath).replace("\\R".toRegex(), "")
  }
}
