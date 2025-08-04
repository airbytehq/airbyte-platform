/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.api.client.model.generated.CatalogDiff
import io.airbyte.api.client.model.generated.FieldTransform
import io.airbyte.api.client.model.generated.StreamAttributeTransform
import io.airbyte.api.client.model.generated.StreamTransform
import io.airbyte.api.client.model.generated.StreamTransformUpdateStream
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.State
import io.airbyte.config.StateWrapper
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.config.helpers.StateMessageHelper.getTypedState
import io.airbyte.workers.helper.CatalogDiffConverter.toDomain
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.List
import java.util.Optional

@MicronautTest
internal class BackfillHelperTest {
  @Inject
  private lateinit var backfillHelper: BackfillHelper

  @Test
  fun testGetStreamsToBackfillWithNewColumn() {
    Assertions.assertEquals(
      List.of<StreamDescriptor?>(DOMAIN_STREAM_DESCRIPTOR),
      backfillHelper.getStreamsToBackfill(toDomain(SINGLE_STREAM_ADD_COLUMN_DIFF), INCREMENTAL_CATALOG),
    )
  }

  @Test
  fun testGetStreamsToBackfillExcludesFullRefresh() {
    val testCatalog = ConfiguredAirbyteCatalog().withStreams(List.of<ConfiguredAirbyteStream>(INCREMENTAL_STREAM, FULL_REFRESH_STREAM))
    // Verify that the second stream is ignored because it's Full Refresh.
    Assertions.assertEquals(
      1,
      backfillHelper.getStreamsToBackfill(toDomain(TWO_STREAMS_ADD_COLUMN_DIFF), testCatalog).size,
    )
    Assertions.assertEquals(
      List.of<StreamDescriptor?>(DOMAIN_STREAM_DESCRIPTOR),
      backfillHelper.getStreamsToBackfill(toDomain(TWO_STREAMS_ADD_COLUMN_DIFF), testCatalog),
    )
  }

  @Test
  fun testClearStateForStreamsToBackfill() {
    val streamsToBackfill = List.of<StreamDescriptor?>(DOMAIN_STREAM_DESCRIPTOR)
    val updatedState = backfillHelper!!.clearStateForStreamsToBackfill(STATE, streamsToBackfill)
    Assertions.assertNotNull(updatedState)
    val typedState: Optional<StateWrapper> = getTypedState(updatedState!!.getState())
    Assertions.assertEquals(1, typedState.get().getStateMessages().size)
    Assertions.assertEquals(
      JsonNodeFactory.instance.nullNode(),
      typedState
        .get()
        .getStateMessages()[0]
        .getStream()
        .getStreamState(),
    )
  }

  companion object {
    private const val STREAM_NAME = "stream-name"
    private const val STREAM_NAMESPACE = "stream-namespace"
    private const val ANOTHER_STREAM_NAME = "another-stream-name"
    private const val ANOTHER_STREAM_NAMESPACE = "another-stream-namespace"
    private val STREAM_DESCRIPTOR =
      io.airbyte.api.client.model.generated
        .StreamDescriptor(STREAM_NAME, STREAM_NAMESPACE)
    private val DOMAIN_STREAM_DESCRIPTOR: StreamDescriptor =
      StreamDescriptor()
        .withName(STREAM_NAME)
        .withNamespace(STREAM_NAMESPACE)
    private val ANOTHER_STREAM_DESCRIPTOR =
      io.airbyte.api.client.model.generated
        .StreamDescriptor(ANOTHER_STREAM_NAME, ANOTHER_STREAM_NAMESPACE)

    private val INCREMENTAL_STREAM =
      ConfiguredAirbyteStream(
        AirbyteStream(STREAM_NAME, emptyObject(), List.of<SyncMode?>(SyncMode.INCREMENTAL))
          .withNamespace(STREAM_NAMESPACE),
        SyncMode.INCREMENTAL,
        DestinationSyncMode.APPEND,
      )
    private val FULL_REFRESH_STREAM =
      ConfiguredAirbyteStream(
        AirbyteStream(ANOTHER_STREAM_NAME, emptyObject(), List.of<SyncMode?>(SyncMode.FULL_REFRESH))
          .withNamespace(ANOTHER_STREAM_NAMESPACE),
        SyncMode.FULL_REFRESH,
        DestinationSyncMode.APPEND,
      )
    private val INCREMENTAL_CATALOG =
      ConfiguredAirbyteCatalog()
        .withStreams(List.of<ConfiguredAirbyteStream>(INCREMENTAL_STREAM))
    private val SINGLE_STREAM_ADD_COLUMN_DIFF =
      CatalogDiff(
        List.of<StreamTransform>(addFieldForStream(STREAM_DESCRIPTOR)),
      )
    private val TWO_STREAMS_ADD_COLUMN_DIFF =
      CatalogDiff(
        List.of<StreamTransform>(
          addFieldForStream(STREAM_DESCRIPTOR),
          addFieldForStream(ANOTHER_STREAM_DESCRIPTOR),
        ),
      )

    private val STATE: State? =
      State().withState(
        deserialize(
          String.format(
            """
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
            
            """.trimIndent(),
            STREAM_NAME,
            STREAM_NAMESPACE,
            STREAM_NAME,
            STREAM_NAMESPACE,
          ),
        ),
      )

    private fun addFieldForStream(streamDescriptor: io.airbyte.api.client.model.generated.StreamDescriptor): StreamTransform =
      StreamTransform(
        StreamTransform.TransformType.UPDATE_STREAM,
        streamDescriptor,
        StreamTransformUpdateStream(
          List.of<FieldTransform>(FieldTransform(FieldTransform.TransformType.ADD_FIELD, mutableListOf<String>(), false, null, null, null)),
          mutableListOf<StreamAttributeTransform>(),
        ),
      )
  }
}
