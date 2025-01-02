/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.SyncMode;
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
            new ConfiguredAirbyteStream(new AirbyteStream(streamName, Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL)), SyncMode.INCREMENTAL,
                DestinationSyncMode.APPEND_DEDUP)
                    .withPrimaryKey(pksList)));
  }

}
