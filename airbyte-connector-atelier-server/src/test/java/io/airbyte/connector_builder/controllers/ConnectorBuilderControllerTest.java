/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.airbyte.connector_builder.api.model.generated.ResolveManifest;
import io.airbyte.connector_builder.api.model.generated.ResolveManifestRequestBody;
import io.airbyte.connector_builder.api.model.generated.StreamRead;
import io.airbyte.connector_builder.api.model.generated.StreamReadPages;
import io.airbyte.connector_builder.api.model.generated.StreamReadRequestBody;
import io.airbyte.connector_builder.api.model.generated.StreamReadSlices;
import io.airbyte.connector_builder.api.model.generated.StreamsListRead;
import io.airbyte.connector_builder.api.model.generated.StreamsListRequestBody;
import io.airbyte.connector_builder.command_runner.MockSynchronousPythonCdkCommandRunner;
import io.airbyte.connector_builder.command_runner.SynchronousCdkCommandRunner;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.exceptions.CdkProcessException;
import io.airbyte.connector_builder.exceptions.ConnectorBuilderException;
import io.airbyte.connector_builder.file_writer.AirbyteFileWriter;
import io.airbyte.connector_builder.file_writer.MockAirbyteFileWriterImpl;
import io.airbyte.connector_builder.handlers.HealthHandler;
import io.airbyte.connector_builder.handlers.ResolveManifestHandler;
import io.airbyte.connector_builder.requester.AirbyteCdkRequester;
import io.airbyte.connector_builder.requester.AirbyteCdkRequesterImpl;
import io.airbyte.workers.internal.DefaultAirbyteStreamFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorBuilderControllerTest {

  private AirbyteCdkRequester requester;
  private AirbyteFileWriter writer;
  private ConnectorBuilderController controller;
  private DefaultAirbyteStreamFactory streamFactory;
  private HealthHandler healthHandler;
  private ResolveManifestHandler resolveManifestHandler;
  private SynchronousCdkCommandRunner commandRunner;
  static String cdkException;
  static String recordManifestResolve;
  static String traceManifestResolve;
  static String validManifest;

  @BeforeEach
  void setup() {
    this.healthHandler = mock(HealthHandler.class);
    this.writer = new MockAirbyteFileWriterImpl();
    this.streamFactory = new DefaultAirbyteStreamFactory();
    this.commandRunner = new MockSynchronousPythonCdkCommandRunner(this.writer, this.streamFactory);
    this.requester = new AirbyteCdkRequesterImpl(this.commandRunner);
    this.resolveManifestHandler = new ResolveManifestHandler(requester);

    this.controller = new ConnectorBuilderController(
        this.healthHandler, this.resolveManifestHandler);
  }

  @BeforeAll
  public static void setUpClass() throws IOException {
    final String relativeDir = "src/test/java/io/airbyte/connector_builder/fixtures";
    validManifest = readContents(String.format("%s/ValidManifest.json", relativeDir));
    recordManifestResolve = readContents(String.format("%s/RecordManifestResolve.json", relativeDir));
    traceManifestResolve = readContents(String.format("%s/TraceManifestResolve.json", relativeDir));
    cdkException = readContents(String.format("%s/CdkException.txt", relativeDir));
  }

  static String readContents(final String filepath) throws IOException {
    final File file = new File(filepath);
    return Files.readString(file.toPath()).replaceAll("\\R", "");
  }

  @Test
  void testListStreams() {
    final StreamsListRead streams = this.controller.listStreams(new StreamsListRequestBody());

    assertEquals(2, streams.getStreams().size());
  }

  @Test
  void testReadStream() {
    final StreamRead readResponse = this.controller.readStream(new StreamReadRequestBody());

    final List<StreamReadSlices> slices = readResponse.getSlices();
    assertEquals(1, slices.size());

    final List<StreamReadPages> pages = slices.get(0).getPages();
    assertEquals(1, pages.size());

    final List<Object> records = pages.get(0).getRecords();
    assertEquals(2, records.size());
  }

  ConnectorBuilderController getTestResolveManifestController(
                                                              final boolean shouldThrow,
                                                              final int exitCode,
                                                              final InputStream inputStream,
                                                              final InputStream errorStream,
                                                              final OutputStream outputStream) {
    final SynchronousCdkCommandRunner commandRunner = new MockSynchronousPythonCdkCommandRunner(
        this.writer, this.streamFactory, shouldThrow, exitCode, inputStream, errorStream, outputStream);
    this.requester = new AirbyteCdkRequesterImpl(commandRunner);
    this.resolveManifestHandler = new ResolveManifestHandler(requester);
    return new ConnectorBuilderController(this.healthHandler, this.resolveManifestHandler);
  }

  @Test
  void testResolveManifestSuccess() {
    final InputStream stream = new ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(recordManifestResolve).array());

    final ConnectorBuilderController controller = getTestResolveManifestController(
        false, 0, stream, null, null);
    final ResolveManifestRequestBody resolveManifestRequestBody = new ResolveManifestRequestBody();
    resolveManifestRequestBody.setManifest(validManifest);

    final ResolveManifest resolvedManifest = controller.resolveManifest(resolveManifestRequestBody);
    assertNotNull(resolvedManifest.getManifest());
  }

  @Test
  void testResolveManifestNoRecordsReturnsError() {
    final ConnectorBuilderController controller = getTestResolveManifestController(
        false, 0, null, null, null);

    final ResolveManifestRequestBody resolveManifestRequestBody = new ResolveManifestRequestBody();
    resolveManifestRequestBody.setManifest(validManifest);

    final Exception exception =
        Assertions.assertThrows(AirbyteCdkInvalidInputException.class, () -> controller.resolveManifest(resolveManifestRequestBody));
    assertTrue(exception.getMessage().contains("No records found"));
    assertNotNull(exception.getStackTrace());
  }

  @Test
  void testResolveManifestTraceResponseReturnsError() {
    final InputStream stream = new ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(traceManifestResolve).array());

    final ConnectorBuilderController controller = getTestResolveManifestController(
        false, 0, stream, null, null);

    final ResolveManifestRequestBody resolveManifestRequestBody = new ResolveManifestRequestBody();
    resolveManifestRequestBody.setManifest(validManifest);

    final Exception exception =
        Assertions.assertThrows(AirbyteCdkInvalidInputException.class, () -> controller.resolveManifest(resolveManifestRequestBody));
    assertTrue(exception.getMessage().contains("AirbyteTraceMessage response from CDK."));
    assertNotNull(exception.getStackTrace());
  }

  @Test
  void testResolveManifestNonzeroExitCodeReturnsError() {
    final InputStream stream = new ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(cdkException).array());

    final ConnectorBuilderController controller = getTestResolveManifestController(
        false, 1, null, stream, null);

    final ResolveManifestRequestBody resolveManifestRequestBody = new ResolveManifestRequestBody();
    resolveManifestRequestBody.setManifest(validManifest);

    final Exception exception = Assertions.assertThrows(CdkProcessException.class, () -> controller.resolveManifest(resolveManifestRequestBody));
    assertTrue(exception.getMessage().contains("CDK subprocess for resolve_manifest finished with exit code 1."));
    assertNotNull(exception.getStackTrace());
  }

  @Test
  void testResolveManifesServerExceptionReturnsError() {
    final InputStream stream = new ByteArrayInputStream(
        StandardCharsets.UTF_8.encode(recordManifestResolve).array());

    final ConnectorBuilderController controller = getTestResolveManifestController(
        true, 0, stream, null, null);

    final ResolveManifestRequestBody resolveManifestRequestBody = new ResolveManifestRequestBody();
    resolveManifestRequestBody.setManifest(validManifest);

    final Exception exception =
        Assertions.assertThrows(ConnectorBuilderException.class, () -> controller.resolveManifest(resolveManifestRequestBody));
    assertTrue(exception.getMessage().contains("Error handling resolve_manifest request."));
    assertNotNull(exception.getStackTrace());
  }

}
