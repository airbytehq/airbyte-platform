/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.controllers;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.connector_builder.api.model.generated.ResolveManifest;
import io.airbyte.connector_builder.api.model.generated.ResolveManifestRequestBody;
import io.airbyte.connector_builder.api.model.generated.StreamRead;
import io.airbyte.connector_builder.api.model.generated.StreamReadRequestBody;
import io.airbyte.connector_builder.command_runner.MockSynchronousPythonCdkCommandRunner;
import io.airbyte.connector_builder.command_runner.SynchronousCdkCommandRunner;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.exceptions.CdkProcessException;
import io.airbyte.connector_builder.exceptions.CdkUnknownException;
import io.airbyte.connector_builder.exceptions.ConnectorBuilderException;
import io.airbyte.connector_builder.file_writer.MockAirbyteFileWriterImpl;
import io.airbyte.connector_builder.handlers.AssistProxyHandler;
import io.airbyte.connector_builder.handlers.ConnectorContributionHandler;
import io.airbyte.connector_builder.handlers.HealthHandler;
import io.airbyte.connector_builder.handlers.ResolveManifestHandler;
import io.airbyte.connector_builder.handlers.StreamHandler;
import io.airbyte.connector_builder.requester.AirbyteCdkRequesterImpl;
import io.airbyte.connector_builder.templates.ContributionTemplates;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorBuilderControllerIntegrationTest {

  private static final JsonNode A_CONFIG;
  private static final JsonNode A_MANIFEST;
  private static final String A_STREAM = "stream";

  static {
    try {
      A_CONFIG = new ObjectMapper().readTree("{\"config\": 1}");
      A_MANIFEST = new ObjectMapper().readTree("{\"manifest\": 1}");
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private HealthHandler healthHandler;
  static String cdkException;
  static String streamRead;
  static String recordManifestResolve;
  static String traceManifestResolve;
  static JsonNode validManifest;
  private MockAirbyteFileWriterImpl writer;
  private AirbyteStreamFactory streamFactory;
  private ContributionTemplates contributionTemplates;
  private AssistProxyHandler assistProxyHandler;

  @BeforeEach
  void setup() {
    this.healthHandler = mock(HealthHandler.class);
    this.writer = new MockAirbyteFileWriterImpl();
    this.streamFactory = VersionedAirbyteStreamFactory.noMigrationVersionedAirbyteStreamFactory();
    this.contributionTemplates = new ContributionTemplates();
    this.assistProxyHandler = mock(AssistProxyHandler.class);
  }

  @BeforeAll
  public static void setUpClass() throws IOException {
    final String relativeDir = "src/test/java/io/airbyte/connector_builder/fixtures";
    validManifest = new ObjectMapper().readTree(readContents(String.format("%s/ValidManifest.json", relativeDir)));
    streamRead = readContents(String.format("%s/RecordStreamRead.json", relativeDir));
    recordManifestResolve = readContents(String.format("%s/RecordManifestResolve.json", relativeDir));
    traceManifestResolve = readContents(String.format("%s/TraceManifestResolve.json", relativeDir));
    cdkException = readContents(String.format("%s/CdkException.txt", relativeDir));
  }

  static String readContents(final String filepath) throws IOException {
    final File file = new File(filepath);
    return Files.readString(file.toPath()).replaceAll("\\R", "");
  }

  ConnectorBuilderController createControllerWithSynchronousRunner(
                                                                   final boolean shouldThrow,
                                                                   final int exitCode,
                                                                   final InputStream inputStream,
                                                                   final InputStream errorStream,
                                                                   final OutputStream outputStream) {
    final SynchronousCdkCommandRunner commandRunner = new MockSynchronousPythonCdkCommandRunner(
        this.writer, this.streamFactory, shouldThrow, exitCode, inputStream, errorStream, outputStream);
    final AirbyteCdkRequesterImpl requester = new AirbyteCdkRequesterImpl(commandRunner);
    return new ConnectorBuilderController(this.healthHandler, new ResolveManifestHandler(requester), new StreamHandler(requester),
        new ConnectorContributionHandler(contributionTemplates, null), this.assistProxyHandler);
  }

  @Test
  void testStreamRead() {
    final ConnectorBuilderController controller = givenAirbyteCdkReturnMessage(streamRead);
    final StreamRead streamRead = controller.readStream(new StreamReadRequestBody().config(A_CONFIG).manifest(A_MANIFEST).stream(A_STREAM));
    assertFalse(streamRead.getLogs().isEmpty());
    assertFalse(streamRead.getSlices().isEmpty());
    assertNotNull(streamRead.getInferredSchema());
    assertFalse(streamRead.getTestReadLimitReached());
  }

  @Test
  void testStreamReadWithOptionalInputs() {
    final ConnectorBuilderController controller = givenAirbyteCdkReturnMessage(streamRead);
    final StreamRead streamRead =
        controller.readStream(new StreamReadRequestBody().config(A_CONFIG).manifest(A_MANIFEST).stream(A_STREAM).formGeneratedManifest(true));
    assertFalse(streamRead.getLogs().isEmpty());
    assertFalse(streamRead.getSlices().isEmpty());
    assertNotNull(streamRead.getInferredSchema());
    assertFalse(streamRead.getTestReadLimitReached());
  }

  @Test
  void givenTraceMessageWhenStreamReadThenThrowException() {
    final ConnectorBuilderController controller = givenAirbyteCdkReturnMessage(traceManifestResolve);
    Assertions.assertThrows(AirbyteCdkInvalidInputException.class,
        () -> controller.readStream(new StreamReadRequestBody().config(A_CONFIG).manifest(A_MANIFEST).stream(A_STREAM)));
  }

  @Test
  void testResolveManifestSuccess() {
    final InputStream stream = new ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(recordManifestResolve).array());

    final ConnectorBuilderController controller = createControllerWithSynchronousRunner(
        false, 0, stream, null, null);
    final ResolveManifestRequestBody resolveManifestRequestBody = new ResolveManifestRequestBody();
    resolveManifestRequestBody.setManifest(validManifest);

    final ResolveManifest resolvedManifest = controller.resolveManifest(resolveManifestRequestBody);
    assertNotNull(resolvedManifest.getManifest());
  }

  @Test
  void testResolveManifestNoRecordsReturnsError() {
    final ConnectorBuilderController controller = createControllerWithSynchronousRunner(
        false, 0, null, null, null);

    final ResolveManifestRequestBody resolveManifestRequestBody = new ResolveManifestRequestBody();
    resolveManifestRequestBody.setManifest(validManifest);

    final Exception exception =
        Assertions.assertThrows(CdkUnknownException.class, () -> controller.resolveManifest(resolveManifestRequestBody));
    assertTrue(exception.getMessage().contains("no records nor trace were found"));
    assertNotNull(exception.getStackTrace());
  }

  @Test
  void testResolveManifestTraceResponseReturnsError() {
    final InputStream stream = new ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(traceManifestResolve).array());

    final ConnectorBuilderController controller = createControllerWithSynchronousRunner(
        false, 0, stream, null, null);

    final ResolveManifestRequestBody resolveManifestRequestBody = new ResolveManifestRequestBody();
    resolveManifestRequestBody.setManifest(validManifest);

    final Exception exception =
        Assertions.assertThrows(AirbyteCdkInvalidInputException.class, () -> controller.resolveManifest(resolveManifestRequestBody));
    assertTrue(exception.getMessage().contains("AirbyteTraceMessage response from CDK"));
    assertNotNull(exception.getStackTrace());
  }

  @Test
  void testResolveManifestNonzeroExitCodeReturnsError() {
    final InputStream errorStream = new ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(cdkException).array());

    final ConnectorBuilderController controller = createControllerWithSynchronousRunner(
        false, 1, null, errorStream, null);

    final ResolveManifestRequestBody resolveManifestRequestBody = new ResolveManifestRequestBody();
    resolveManifestRequestBody.setManifest(validManifest);

    final Exception exception = Assertions.assertThrows(CdkProcessException.class, () -> controller.resolveManifest(resolveManifestRequestBody));
    assertTrue(exception.getMessage().contains("CDK subprocess for resolve_manifest finished with exit code 1."));
    assertNotNull(exception.getStackTrace());
  }

  @Test
  void testResolveManifestNonzeroExitCodeAndInputStreamReturnsError() {
    final InputStream emptyInputStream = new InputStream() {

      @Override
      public int read() {
        return -1;
      }

    };
    final InputStream errorStream = new ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(cdkException).array());

    final ConnectorBuilderController controller = createControllerWithSynchronousRunner(
        false, 1, emptyInputStream, errorStream, null);

    final ResolveManifestRequestBody resolveManifestRequestBody = new ResolveManifestRequestBody();
    resolveManifestRequestBody.setManifest(validManifest);

    final Exception exception = Assertions.assertThrows(CdkProcessException.class, () -> controller.resolveManifest(resolveManifestRequestBody));
    assertTrue(exception.getMessage().contains("CDK subprocess for resolve_manifest finished with exit code 1."));
    assertNotNull(exception.getStackTrace());
  }

  @Test
  void testResolveManifestServerExceptionReturnsError() {
    final InputStream stream = new ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(recordManifestResolve).array());

    final ConnectorBuilderController controller = createControllerWithSynchronousRunner(
        true, 0, stream, null, null);

    final ResolveManifestRequestBody resolveManifestRequestBody = new ResolveManifestRequestBody();
    resolveManifestRequestBody.setManifest(validManifest);

    final Exception exception =
        Assertions.assertThrows(ConnectorBuilderException.class, () -> controller.resolveManifest(resolveManifestRequestBody));
    assertTrue(exception.getMessage().contains("Error handling resolve_manifest request."));
    assertNotNull(exception.getStackTrace());
  }

  private ConnectorBuilderController givenAirbyteCdkReturnMessage(final String message) {
    final InputStream stream = new ByteArrayInputStream(StandardCharsets.UTF_8.encode(message).array());
    return createControllerWithSynchronousRunner(false, 0, stream, null, null);
  }

}
