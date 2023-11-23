/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.airbyte.api.client.model.generated.CatalogDiff;
import io.airbyte.api.client.model.generated.FieldTransform;
import io.airbyte.api.client.model.generated.StreamDescriptor;
import io.airbyte.api.client.model.generated.StreamTransform;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.State;
import io.airbyte.config.helpers.StateMessageHelper;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.SyncMode;
import java.util.List;
import org.junit.jupiter.api.Test;

class BackfillHelperTest {

  private static final String STREAM_NAME = "stream-name";
  private static final String STREAM_NAMESPACE = "stream-namespace";
  private static final String ANOTHER_STREAM_NAME = "another-stream-name";
  private static final String ANOTHER_STREAM_NAMESPACE = "another-stream-namespace";
  private static final StreamDescriptor STREAM_DESCRIPTOR = new StreamDescriptor().name(STREAM_NAME).namespace(STREAM_NAMESPACE);
  private static final StreamDescriptor ANOTHER_STREAM_DESCRIPTOR = new StreamDescriptor()
      .name(ANOTHER_STREAM_NAME).namespace(ANOTHER_STREAM_NAMESPACE);

  private static final ConfiguredAirbyteStream INCREMENTAL_STREAM = new ConfiguredAirbyteStream()
      .withStream(new AirbyteStream()
          .withName(STREAM_NAME)
          .withNamespace(STREAM_NAMESPACE))
      .withSyncMode(SyncMode.INCREMENTAL);
  private static final ConfiguredAirbyteStream FULL_REFRESH_STREAM = new ConfiguredAirbyteStream()
      .withStream(new AirbyteStream()
          .withName(ANOTHER_STREAM_NAME)
          .withNamespace(ANOTHER_STREAM_NAMESPACE))
      .withSyncMode(SyncMode.FULL_REFRESH);
  private static final ConfiguredAirbyteCatalog INCREMENTAL_CATALOG = new ConfiguredAirbyteCatalog()
      .withStreams(List.of(INCREMENTAL_STREAM));
  private static CatalogDiff SINGLE_STREAM_ADD_COLUMN_DIFF = new CatalogDiff()
      .addTransformsItem(addFieldForStream(STREAM_DESCRIPTOR));
  private static CatalogDiff TWO_STREAMS_ADD_COLUMN_DIFF = new CatalogDiff()
      .addTransformsItem(addFieldForStream(STREAM_DESCRIPTOR))
      .addTransformsItem(addFieldForStream(ANOTHER_STREAM_DESCRIPTOR));

  private static State STATE = new State().withState(Jsons.deserialize(
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
    return new StreamTransform()
        .streamDescriptor(streamDescriptor)
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .addUpdateStreamItem(new FieldTransform()
            .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD));
  }

  @Test
  void testGetStreamsToBackfillWithNewColumn() {
    assertEquals(
        List.of(STREAM_DESCRIPTOR),
        BackfillHelper.getStreamsToBackfill(SINGLE_STREAM_ADD_COLUMN_DIFF, INCREMENTAL_CATALOG));
  }

  @Test
  void testGetStreamsToBackfillExcludesFullRefresh() {
    final ConfiguredAirbyteCatalog testCatalog = new ConfiguredAirbyteCatalog().withStreams(List.of(INCREMENTAL_STREAM, FULL_REFRESH_STREAM));
    // Verify that the second stream is ignored because it's Full Refresh.
    assertEquals(
        1,
        BackfillHelper.getStreamsToBackfill(TWO_STREAMS_ADD_COLUMN_DIFF, testCatalog).size());
    assertEquals(
        List.of(STREAM_DESCRIPTOR),
        BackfillHelper.getStreamsToBackfill(TWO_STREAMS_ADD_COLUMN_DIFF, testCatalog));
  }

  @Test
  void testClearStateForStreamsToBackfill() {
    List<StreamDescriptor> streamsToBackfill = List.of(STREAM_DESCRIPTOR);
    final var actualState = BackfillHelper.clearStateForStreamsToBackfill(STATE, streamsToBackfill);
    final var typedState = StateMessageHelper.getTypedState(actualState.getState());
    assertEquals(1, typedState.get().getStateMessages().size());
    assertEquals(JsonNodeFactory.instance.nullNode(), typedState.get().getStateMessages().get(0).getStream().getStreamState());
  }

}
