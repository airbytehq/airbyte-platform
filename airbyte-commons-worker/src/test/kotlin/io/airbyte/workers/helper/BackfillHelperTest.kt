/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.api.client.model.generated.CatalogDiff
import io.airbyte.api.client.model.generated.FieldTransform
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
import java.util.Optional

@MicronautTest
internal class BackfillHelperTest {
  @Inject
  private lateinit var backfillHelper: BackfillHelper

  @Test
  fun testGetStreamsToBackfillWithNewColumn() {
    Assertions.assertEquals(
      listOf(DOMAIN_STREAM_DESCRIPTOR),
      backfillHelper.getStreamsToBackfill(toDomain(SINGLE_STREAM_ADD_COLUMN_DIFF), INCREMENTAL_CATALOG),
    )
  }

  @Test
  fun testGetStreamsToBackfillExcludesFullRefresh() {
    val testCatalog = ConfiguredAirbyteCatalog().withStreams(listOf(INCREMENTAL_STREAM, FULL_REFRESH_STREAM))
    // Verify that the second stream is ignored because it's Full Refresh.
    Assertions.assertEquals(
      1,
      backfillHelper.getStreamsToBackfill(toDomain(TWO_STREAMS_ADD_COLUMN_DIFF), testCatalog).size,
    )
    Assertions.assertEquals(
      listOf(DOMAIN_STREAM_DESCRIPTOR),
      backfillHelper.getStreamsToBackfill(toDomain(TWO_STREAMS_ADD_COLUMN_DIFF), testCatalog),
    )
  }

  @Test
  fun testClearStateForStreamsToBackfill() {
    val streamsToBackfill = listOf(DOMAIN_STREAM_DESCRIPTOR)
    val updatedState = backfillHelper.clearStateForStreamsToBackfill(STATE, streamsToBackfill)
    Assertions.assertNotNull(updatedState)
    val typedState: Optional<StateWrapper> = getTypedState(updatedState!!.state)
    Assertions.assertEquals(1, typedState.get().stateMessages.size)
    Assertions.assertEquals(
      JsonNodeFactory.instance.nullNode(),
      typedState
        .get()
        .stateMessages[0]
        .stream
        .streamState,
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
        AirbyteStream(STREAM_NAME, emptyObject(), listOf(SyncMode.INCREMENTAL))
          .withNamespace(STREAM_NAMESPACE),
        SyncMode.INCREMENTAL,
        DestinationSyncMode.APPEND,
      )
    private val FULL_REFRESH_STREAM =
      ConfiguredAirbyteStream(
        AirbyteStream(ANOTHER_STREAM_NAME, emptyObject(), listOf(SyncMode.FULL_REFRESH))
          .withNamespace(ANOTHER_STREAM_NAMESPACE),
        SyncMode.FULL_REFRESH,
        DestinationSyncMode.APPEND,
      )
    private val INCREMENTAL_CATALOG =
      ConfiguredAirbyteCatalog()
        .withStreams(listOf(INCREMENTAL_STREAM))
    private val SINGLE_STREAM_ADD_COLUMN_DIFF =
      CatalogDiff(
        listOf(addFieldForStream(STREAM_DESCRIPTOR)),
      )
    private val TWO_STREAMS_ADD_COLUMN_DIFF =
      CatalogDiff(
        listOf(
          addFieldForStream(STREAM_DESCRIPTOR),
          addFieldForStream(ANOTHER_STREAM_DESCRIPTOR),
        ),
      )

    private val STATE: State? =
      State().withState(
        deserialize(
          """
          [{
            "type":"STREAM",
            "stream":{
              "stream_descriptor":{
                "name":"$STREAM_NAME",
                "namespace":"$STREAM_NAMESPACE"
              },
              "stream_state":{"cursor":"6","stream_name":"$STREAM_NAME","cursor_field":["id"],"stream_namespace":"$STREAM_NAMESPACE","cursor_record_count":1}
            }
          }]
          
          """.trimIndent(),
        ),
      )

    private fun addFieldForStream(streamDescriptor: io.airbyte.api.client.model.generated.StreamDescriptor): StreamTransform =
      StreamTransform(
        StreamTransform.TransformType.UPDATE_STREAM,
        streamDescriptor,
        StreamTransformUpdateStream(
          listOf(FieldTransform(FieldTransform.TransformType.ADD_FIELD, mutableListOf(), false, null, null, null)),
          mutableListOf(),
        ),
      )
  }
}
