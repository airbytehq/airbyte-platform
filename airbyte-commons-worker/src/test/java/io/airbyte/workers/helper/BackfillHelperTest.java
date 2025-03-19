/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.airbyte.api.client.model.generated.CatalogDiff;
import io.airbyte.api.client.model.generated.FieldTransform;
import io.airbyte.api.client.model.generated.StreamDescriptor;
import io.airbyte.api.client.model.generated.StreamTransform;
import io.airbyte.api.client.model.generated.StreamTransformUpdateStream;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.State;
import io.airbyte.config.SyncMode;
import io.airbyte.config.helpers.StateMessageHelper;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.Test;

@MicronautTest
class BackfillHelperTest {

  @Inject
  private BackfillHelper backfillHelper;
  private static final String STREAM_NAME = "stream-name";
  private static final String STREAM_NAMESPACE = "stream-namespace";
  private static final String ANOTHER_STREAM_NAME = "another-stream-name";
  private static final String ANOTHER_STREAM_NAMESPACE = "another-stream-namespace";
  private static final StreamDescriptor STREAM_DESCRIPTOR = new StreamDescriptor(STREAM_NAME, STREAM_NAMESPACE);
  private static final io.airbyte.config.StreamDescriptor DOMAIN_STREAM_DESCRIPTOR = new io.airbyte.config.StreamDescriptor()
      .withName(STREAM_NAME)
      .withNamespace(STREAM_NAMESPACE);
  private static final StreamDescriptor ANOTHER_STREAM_DESCRIPTOR = new StreamDescriptor(ANOTHER_STREAM_NAME, ANOTHER_STREAM_NAMESPACE);

  private static final ConfiguredAirbyteStream INCREMENTAL_STREAM = new ConfiguredAirbyteStream(
      new AirbyteStream(STREAM_NAME, Jsons.emptyObject(), List.of(SyncMode.INCREMENTAL))
          .withNamespace(STREAM_NAMESPACE),
      SyncMode.INCREMENTAL,
      DestinationSyncMode.APPEND);
  private static final ConfiguredAirbyteStream FULL_REFRESH_STREAM = new ConfiguredAirbyteStream(
      new AirbyteStream(ANOTHER_STREAM_NAME, Jsons.emptyObject(), List.of(SyncMode.FULL_REFRESH))
          .withNamespace(ANOTHER_STREAM_NAMESPACE),
      SyncMode.FULL_REFRESH,
      DestinationSyncMode.APPEND);
  private static final ConfiguredAirbyteCatalog INCREMENTAL_CATALOG = new ConfiguredAirbyteCatalog()
      .withStreams(List.of(INCREMENTAL_STREAM));
  private static final CatalogDiff SINGLE_STREAM_ADD_COLUMN_DIFF = new CatalogDiff(
      List.of(addFieldForStream(STREAM_DESCRIPTOR)));
  private static final CatalogDiff TWO_STREAMS_ADD_COLUMN_DIFF = new CatalogDiff(List.of(
      addFieldForStream(STREAM_DESCRIPTOR),
      addFieldForStream(ANOTHER_STREAM_DESCRIPTOR)));

  private static final State STATE = new State().withState(Jsons.deserialize(
      String.format("""
                    [{
                      "type":"STREAM",
                      "stream":{
                        "stream_descriptor":{
                          "name":"%s",
                          "namespace":"%s"
                        },
                        "stream_state":{"cursor":"6","stream_name":"%s","cursor_field":["id"],"stream_namespace":"%s","cursor_record_count":1}
                      }
                    }]
                    """, STREAM_NAME, STREAM_NAMESPACE, STREAM_NAME, STREAM_NAMESPACE)));

  private static StreamTransform addFieldForStream(final StreamDescriptor streamDescriptor) {
    return new StreamTransform(
        StreamTransform.TransformType.UPDATE_STREAM,
        streamDescriptor,
        new StreamTransformUpdateStream(
            List.of(new FieldTransform(FieldTransform.TransformType.ADD_FIELD, List.of(), false, null, null, null)),
            List.of()));
  }

  @Test
  void testGetStreamsToBackfillWithNewColumn() {
    assertEquals(
        List.of(DOMAIN_STREAM_DESCRIPTOR),
        backfillHelper.getStreamsToBackfill(CatalogDiffConverter.toDomain(SINGLE_STREAM_ADD_COLUMN_DIFF), INCREMENTAL_CATALOG));
  }

  @Test
  void testGetStreamsToBackfillExcludesFullRefresh() {
    final ConfiguredAirbyteCatalog testCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(INCREMENTAL_STREAM, FULL_REFRESH_STREAM));
    // Verify that the second stream is ignored because it's Full Refresh.
    assertEquals(
        1,
        backfillHelper.getStreamsToBackfill(CatalogDiffConverter.toDomain(TWO_STREAMS_ADD_COLUMN_DIFF), testCatalog).size());
    assertEquals(
        List.of(DOMAIN_STREAM_DESCRIPTOR),
        backfillHelper.getStreamsToBackfill(CatalogDiffConverter.toDomain(TWO_STREAMS_ADD_COLUMN_DIFF), testCatalog));
  }

  @Test
  void testClearStateForStreamsToBackfill() {
    final List<io.airbyte.config.StreamDescriptor> streamsToBackfill = List.of(DOMAIN_STREAM_DESCRIPTOR);
    final State updatedState = backfillHelper.clearStateForStreamsToBackfill(STATE, streamsToBackfill);
    assertNotNull(updatedState);
    final var typedState = StateMessageHelper.getTypedState(updatedState.getState());
    assertEquals(1, typedState.get().getStateMessages().size());
    assertEquals(JsonNodeFactory.instance.nullNode(), typedState.get().getStateMessages().getFirst().getStream().getStreamState());
  }

}
