/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.logging.MdcScope.Builder;
import io.airbyte.commons.protocol.AirbyteMessageMigrator;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.protocol.ConfiguredAirbyteCatalogMigrator;
import io.airbyte.commons.protocol.serde.AirbyteMessageV0Deserializer;
import io.airbyte.commons.protocol.serde.AirbyteMessageV0Serializer;
import io.airbyte.commons.protocol.serde.AirbyteMessageV1Deserializer;
import io.airbyte.commons.protocol.serde.AirbyteMessageV1Serializer;
import io.airbyte.commons.version.Version;
import io.airbyte.protocol.models.AirbyteLogMessage;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ClassLoaderUtils;
import org.slf4j.Logger;

class VersionedAirbyteStreamFactoryTest {

  AirbyteMessageSerDeProvider serDeProvider;
  AirbyteProtocolVersionedMigratorFactory migratorFactory;

  @Nested
  @DisplayName("Test Correct AirbyteMessage Parsing Behavior")
  class ParseMessages {

    private static final String STREAM_NAME = "user_preferences";
    private static final String FIELD_NAME = "favorite_color";
    private Logger logger;

    @BeforeEach
    void setup() {
      logger = mock(Logger.class);
    }

    @Test
    void testValid() {
      final AirbyteMessage record1 = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, "green");

      final Stream<AirbyteMessage> messageStream = stringToMessageStream(Jsons.serialize(record1));
      final Stream<AirbyteMessage> expectedStream = Stream.of(record1);

      assertEquals(expectedStream.collect(Collectors.toList()), messageStream.collect(Collectors.toList()));
      verify(logger).info("Reading messages from protocol version {}{}", "0.2.0", "");
    }

    @Test
    void testLoggingLine() {
      final String invalidRecord = "invalid line";

      final Stream<AirbyteMessage> messageStream = stringToMessageStream(invalidRecord);

      assertEquals(Collections.emptyList(), messageStream.collect(Collectors.toList()));
      verify(logger).error("Deserialization failed: {}", "\"invalid line\"");
    }

    @Test
    void testLoggingLevel() {
      final AirbyteMessage logMessage = AirbyteMessageUtils.createLogMessage(AirbyteLogMessage.Level.WARN, "warning");

      final Stream<AirbyteMessage> messageStream = stringToMessageStream(Jsons.serialize(logMessage));

      assertEquals(Collections.emptyList(), messageStream.collect(Collectors.toList()));
      verify(logger).warn("warning");
    }

    @Test
    void testFailDeserializationObvious() {
      final String invalidRecord = "{ \"type\": \"abc\"}";

      final Stream<AirbyteMessage> messageStream = stringToMessageStream(invalidRecord);

      assertEquals(Collections.emptyList(), messageStream.collect(Collectors.toList()));
      verify(logger).error(anyString(), anyString());
    }

    @Test
    void testFailDeserializationSubtle() {
      final String invalidRecord = "{\"type\": \"record\", \"record\": {}}";

      final Stream<AirbyteMessage> messageStream = stringToMessageStream(invalidRecord);

      assertEquals(Collections.emptyList(), messageStream.collect(Collectors.toList()));
      verify(logger).error(anyString(), anyString());
    }

    @Test
    void testFailValidation() {
      final String invalidRecord = "{ \"fish\": \"tuna\"}";

      final Stream<AirbyteMessage> messageStream = stringToMessageStream(invalidRecord);

      assertEquals(Collections.emptyList(), messageStream.collect(Collectors.toList()));
      verify(logger).error(anyString(), anyString());
    }

    @Test
    void testFailsSize() {
      final AirbyteMessage record1 = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, "green");

      final InputStream inputStream = new ByteArrayInputStream(record1.toString().getBytes(StandardCharsets.UTF_8));
      final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

      final Stream<AirbyteMessage> messageStream =
          VersionedAirbyteStreamFactory.noMigrationVersionedAirbyteStreamFactory(logger, new Builder(), Optional.of(RuntimeException.class), 1L)
              .create(bufferedReader);

      assertThrows(RuntimeException.class, () -> messageStream.toList());
    }

    private Stream<AirbyteMessage> stringToMessageStream(final String inputString) {
      final InputStream inputStream = new ByteArrayInputStream(inputString.getBytes(StandardCharsets.UTF_8));
      final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      return VersionedAirbyteStreamFactory
          .noMigrationVersionedAirbyteStreamFactory(logger, new Builder(), Optional.of(RuntimeException.class), 100000L)
          .create(bufferedReader);
    }

  }

  @Nested
  @DisplayName("Test Correct Protocol Migration Behavior")
  class MigrateMessages {

    @BeforeEach
    void beforeEach() {
      serDeProvider = spy(new AirbyteMessageSerDeProvider(
          List.of(new AirbyteMessageV0Deserializer(), new AirbyteMessageV1Deserializer()),
          List.of(new AirbyteMessageV0Serializer(), new AirbyteMessageV1Serializer())));
      serDeProvider.initialize();

      final AirbyteMessageMigrator airbyteMessageMigrator = new AirbyteMessageMigrator(
          // TODO once data types v1 is re-enabled, this test should contain the migration
          List.of(/* new AirbyteMessageMigrationV1() */));
      airbyteMessageMigrator.initialize();
      final ConfiguredAirbyteCatalogMigrator configuredAirbyteCatalogMigrator = new ConfiguredAirbyteCatalogMigrator(
          // TODO once data types v1 is re-enabled, this test should contain the migration
          List.of(/* new ConfiguredAirbyteCatalogMigrationV1() */));
      configuredAirbyteCatalogMigrator.initialize();
      migratorFactory = spy(new AirbyteProtocolVersionedMigratorFactory(airbyteMessageMigrator, configuredAirbyteCatalogMigrator));
    }

    @Test
    void testCreate() {
      final Version initialVersion = new Version("0.1.2");
      final VersionedAirbyteStreamFactory<?> streamFactory =
          new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, initialVersion, Optional.empty(), Optional.empty());

      final BufferedReader bufferedReader = new BufferedReader(new StringReader(""));
      streamFactory.create(bufferedReader);

      verify(migratorFactory).getAirbyteMessageMigrator(initialVersion);
    }

    @Test
    void testCreateWithVersionDetection() {
      final Version initialVersion = new Version("0.0.0");
      final VersionedAirbyteStreamFactory<?> streamFactory =
          new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, initialVersion, Optional.empty(), Optional.empty())
              .withDetectVersion(true);

      final BufferedReader bufferedReader =
          getBuffereredReader("version-detection/logs-with-version.jsonl");
      final Stream<AirbyteMessage> stream = streamFactory.create(bufferedReader);

      long messageCount = stream.toList().size();
      assertEquals(1, messageCount);
    }

    @Test
    void testCreateWithVersionDetectionFallback() {
      final Version initialVersion = new Version("0.0.6");
      final VersionedAirbyteStreamFactory<?> streamFactory =
          new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, initialVersion, Optional.empty(), Optional.empty())
              .withDetectVersion(true);

      final BufferedReader bufferedReader =
          getBuffereredReader("version-detection/logs-without-version.jsonl");
      final Stream<AirbyteMessage> stream = streamFactory.create(bufferedReader);

      final long messageCount = stream.toList().size();
      assertEquals(1, messageCount);
    }

    @Test
    void testCreateWithVersionDetectionWithoutSpecMessage() {
      final Version initialVersion = new Version("0.0.1");
      final VersionedAirbyteStreamFactory<?> streamFactory =
          new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, initialVersion, Optional.empty(), Optional.empty())
              .withDetectVersion(true);

      final BufferedReader bufferedReader =
          getBuffereredReader("version-detection/logs-without-spec-message.jsonl");
      final Stream<AirbyteMessage> stream = streamFactory.create(bufferedReader);

      final long messageCount = stream.toList().size();
      assertEquals(2, messageCount);
    }

    BufferedReader getBuffereredReader(final String resourceFile) {
      return new BufferedReader(
          new InputStreamReader(
              ClassLoaderUtils.getDefaultClassLoader().getResourceAsStream(resourceFile),
              Charset.defaultCharset()));
    }

  }

}
