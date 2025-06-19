/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.SyncMode;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteMessage.Type;
import io.airbyte.protocol.models.v0.Config;
import io.airbyte.protocol.models.v0.DestinationCatalog;
import io.airbyte.protocol.models.v0.DestinationOperation;
import io.airbyte.workers.internal.exception.SourceException;
import io.airbyte.workers.testutils.AirbyteMessageUtils;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class BasicAirbyteMessageValidatorTest {

  private static final String DATA_KEY_1 = "field_1";
  private static final String STREAM_1 = "stream_1";
  private static final String DATA_VALUE = "green";

  @Test
  void testObviousInvalid() {
    final Optional<AirbyteMessage> bad = Jsons.tryDeserializeExact("{}", AirbyteMessage.class);

    final var m = BasicAirbyteMessageValidator.validate(bad.get(), Optional.empty());
    assertTrue(m.isEmpty());
  }

  @Test
  void testValidRecord() {
    final AirbyteMessage rec = AirbyteMessageUtils.createRecordMessage(STREAM_1, DATA_KEY_1, DATA_VALUE);

    final var m = BasicAirbyteMessageValidator.validate(rec, Optional.empty());
    assertTrue(m.isPresent());
    assertEquals(rec, m.get());
  }

  @Test
  void testSubtleInvalidRecord() {
    final Optional<AirbyteMessage> bad = Jsons.tryDeserializeExact("{\"type\": \"RECORD\", \"record\": {}}", AirbyteMessage.class);

    final var m = BasicAirbyteMessageValidator.validate(bad.get(), Optional.empty());
    assertTrue(m.isEmpty());
  }

  @Test
  void testValidState() {
    final AirbyteMessage rec = AirbyteMessageUtils.createStateMessage(1);

    final var m = BasicAirbyteMessageValidator.validate(rec, Optional.empty());
    assertTrue(m.isPresent());
    assertEquals(rec, m.get());
  }

  @Test
  void testSubtleInvalidState() {
    final Optional<AirbyteMessage> bad = Jsons.tryDeserializeExact("{\"type\": \"STATE\", \"control\": {}}", AirbyteMessage.class);

    final var m = BasicAirbyteMessageValidator.validate(bad.get(), Optional.empty());
    assertTrue(m.isEmpty());
  }

  @Test
  void testValidControl() {
    final AirbyteMessage rec = AirbyteMessageUtils.createConfigControlMessage(new Config(), 1000.0);

    final var m = BasicAirbyteMessageValidator.validate(rec, Optional.empty());
    assertTrue(m.isPresent());
    assertEquals(rec, m.get());
  }

  @Test
  void testSubtleInvalidControl() {
    final Optional<AirbyteMessage> bad = Jsons.tryDeserializeExact("{\"type\": \"CONTROL\", \"state\": {}}", AirbyteMessage.class);

    final var m = BasicAirbyteMessageValidator.validate(bad.get(), Optional.empty());
    assertTrue(m.isEmpty());
  }

  @Test
  void testValidDestinationCatalog() {
    final AirbyteMessage message = new AirbyteMessage()
        .withType(Type.DESTINATION_CATALOG)
        .withDestinationCatalog(new DestinationCatalog().withOperations(List.of(
            new DestinationOperation().withObjectName("my_object"))));
    final var m = BasicAirbyteMessageValidator.validate(message, Optional.empty());
    assertTrue(m.isPresent());
    assertEquals(message, m.get());
  }

  @Test
  void testValidPk() {
    final AirbyteMessage bad = AirbyteMessageUtils.createRecordMessage(STREAM_1, DATA_KEY_1, DATA_VALUE);

    final var m = BasicAirbyteMessageValidator.validate(bad, Optional.of(
        getCatalogWithPk(STREAM_1, List.of(List.of(DATA_KEY_1)))));
    assertTrue(m.isPresent());
  }

  @Test
  void testValidPkWithOneMissingPk() {
    final AirbyteMessage bad = AirbyteMessageUtils.createRecordMessage(STREAM_1, DATA_KEY_1, DATA_VALUE);

    final var m = BasicAirbyteMessageValidator.validate(bad, Optional.of(
        getCatalogWithPk(STREAM_1, List.of(List.of(DATA_KEY_1), List.of("not_field_1")))));
    assertTrue(m.isPresent());
  }

  @Test
  void testNotIncrementalDedup() {
    final AirbyteMessage bad = AirbyteMessageUtils.createRecordMessage(STREAM_1, DATA_KEY_1, DATA_VALUE);

    var m = BasicAirbyteMessageValidator.validate(bad, Optional.of(
        getCatalogNonIncremental(STREAM_1)));
    assertTrue(m.isPresent());

    m = BasicAirbyteMessageValidator.validate(bad, Optional.of(
        getCatalogNonIncrementalDedup(STREAM_1)));
    assertTrue(m.isPresent());
  }

  @Test
  void testInvalidPk() {
    final AirbyteMessage bad = AirbyteMessageUtils.createRecordMessage(STREAM_1, DATA_KEY_1, DATA_VALUE);

    assertThrows(SourceException.class, () -> BasicAirbyteMessageValidator.validate(bad, Optional.of(
        getCatalogWithPk(STREAM_1, List.of(List.of("not_field_1"))))));
  }

  @Test
  void testValidPkInAnotherStream() {
    final AirbyteMessage bad = AirbyteMessageUtils.createRecordMessage(STREAM_1, DATA_KEY_1, DATA_VALUE);

    assertThrows(SourceException.class, () -> BasicAirbyteMessageValidator.validate(bad, Optional.of(
        getCatalogWithPk("stream_2", List.of(List.of(DATA_KEY_1))))));
  }

  private ConfiguredAirbyteCatalog getCatalogWithPk(final String streamName,
                                                    final List<List<String>> pksList) {
    return new ConfiguredAirbyteCatalog()
        .withStreams(List.of(
            new ConfiguredAirbyteStream(new AirbyteStream(streamName, Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL)), SyncMode.INCREMENTAL,
                DestinationSyncMode.APPEND_DEDUP)
                    .withPrimaryKey(pksList)));
  }

  private ConfiguredAirbyteCatalog getCatalogNonIncrementalDedup(final String streamName) {
    return new ConfiguredAirbyteCatalog()
        .withStreams(List.of(
            new ConfiguredAirbyteStream(new AirbyteStream(streamName, Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL)), SyncMode.INCREMENTAL,
                DestinationSyncMode.APPEND)));
  }

  private ConfiguredAirbyteCatalog getCatalogNonIncremental(final String streamName) {
    return new ConfiguredAirbyteCatalog()
        .withStreams(List.of(
            new ConfiguredAirbyteStream(new AirbyteStream(streamName, Jsons.emptyObject(), List.of(SyncMode.FULL_REFRESH)), SyncMode.FULL_REFRESH,
                DestinationSyncMode.APPEND)));
  }

}
