/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.DestinationSyncMode;
import io.airbyte.protocol.models.SyncMode;
import java.util.List;
import org.junit.jupiter.api.Test;

class GsonPksExtractorTest {

  private final GsonPksExtractor gsonPksExtractor = new GsonPksExtractor();

  private static final String STREAM_NAME = "test";
  private static final String FIELD_1_NAME = "field1";

  @Test
  void testNonExistingPk() {
    final var record = """
                       {
                           "record": {
                               "stream": "test",
                               "data": {
                                   "field": "value"
                               }
                           }
                       }
                       """;

    final var configureAirbyteCatalog = getCatalogWithPk(STREAM_NAME, List.of(List.of(FIELD_1_NAME)));

    assertEquals("field1=[MISSING]", gsonPksExtractor.extractPks(configureAirbyteCatalog, record));
  }

  @Test
  void testExistingPkLevel1() {
    final var record = """
                       {
                           "record": {
                               "stream": "test",
                               "data": {
                                   "field1": "value"
                               }
                           }
                       }
                       """;

    final var configureAirbyteCatalog = getCatalogWithPk(STREAM_NAME, List.of(List.of(FIELD_1_NAME)));

    assertEquals("field1=value", gsonPksExtractor.extractPks(configureAirbyteCatalog, record));
  }

  @Test
  void testExistingPkLevel2() {
    final var record = """
                       {
                           "record": {
                               "stream": "test",
                               "data": {
                                   "level1": {
                                       "field1": "value"
                                   }
                               }
                           }
                       }
                       """;

    final var configureAirbyteCatalog = getCatalogWithPk(STREAM_NAME, List.of(List.of("level1", FIELD_1_NAME)));

    assertEquals("level1.field1=value", gsonPksExtractor.extractPks(configureAirbyteCatalog, record));
  }

  @Test
  void testExistingPkMultipleKey() {
    final var record = """
                       {
                           "record": {
                               "stream": "test",
                               "data": {
                                   "field1": "value",
                                   "field2": "value2"
                               }
                           }
                       }
                       """;

    final var configureAirbyteCatalog = getCatalogWithPk(STREAM_NAME, List.of(List.of(FIELD_1_NAME), List.of("field2")));

    assertEquals("field1=value,field2=value2", gsonPksExtractor.extractPks(configureAirbyteCatalog, record));
  }

  @Test
  void testExistingVeryLongRecord() {
    final StringBuilder longStringBuilder = new StringBuilder(5_000_000);
    for (int i = 0; i < 50_000_000; i++) {
      longStringBuilder.append("a");
    }
    final var record = String.format("""
                                     {
                                         "record": {
                                             "stream": "test",
                                             "data": {
                                                 "field1": "value",
                                                 "veryLongField": "%s"
                                             }
                                         }
                                     }
                                     """, longStringBuilder);

    final var configureAirbyteCatalog = getCatalogWithPk(STREAM_NAME, List.of(List.of(FIELD_1_NAME)));

    assertEquals("field1=value", gsonPksExtractor.extractPks(configureAirbyteCatalog, record));
  }

  private ConfiguredAirbyteCatalog getCatalogWithPk(final String streamName,
                                                    final List<List<String>> pksList) {
    return new ConfiguredAirbyteCatalog()
        .withStreams(List.of(
            new ConfiguredAirbyteStream()
                .withStream(new AirbyteStream()
                    .withName(streamName))
                .withPrimaryKey(pksList)
                .withSyncMode(SyncMode.INCREMENTAL)
                .withDestinationSyncMode(DestinationSyncMode.APPEND_DEDUP)));
  }

}
