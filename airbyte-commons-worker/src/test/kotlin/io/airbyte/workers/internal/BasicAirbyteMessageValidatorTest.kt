/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.tryDeserializeExact
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.SyncMode
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.Config
import io.airbyte.protocol.models.v0.DestinationCatalog
import io.airbyte.protocol.models.v0.DestinationOperation
import io.airbyte.workers.internal.BasicAirbyteMessageValidator.validate
import io.airbyte.workers.internal.exception.SourceException
import io.airbyte.workers.testutils.AirbyteMessageUtils.createConfigControlMessage
import io.airbyte.workers.testutils.AirbyteMessageUtils.createRecordMessage
import io.airbyte.workers.testutils.AirbyteMessageUtils.createStateMessage
import org.junit.Assert
import org.junit.jupiter.api.Test
import java.util.Optional

internal class BasicAirbyteMessageValidatorTest {
  @Test
  fun testObviousInvalid() {
    val bad: Optional<AirbyteMessage> = tryDeserializeExact("{}", AirbyteMessage::class.java)

    val m: Optional<AirbyteMessage> = validate(bad.get(), Optional.empty<ConfiguredAirbyteCatalog>(), MessageOrigin.SOURCE)
    Assert.assertTrue(m.isEmpty)
  }

  @Test
  fun testValidRecord() {
    val rec: AirbyteMessage = createRecordMessage(STREAM_1, DATA_KEY_1, DATA_VALUE)

    val m: Optional<AirbyteMessage> = validate(rec, Optional.empty<ConfiguredAirbyteCatalog>(), MessageOrigin.SOURCE)
    Assert.assertTrue(m.isPresent)
    Assert.assertEquals(rec, m.get())
  }

  @Test
  fun testSubtleInvalidRecord() {
    val bad: Optional<AirbyteMessage> = tryDeserializeExact("{\"type\": \"RECORD\", \"record\": {}}", AirbyteMessage::class.java)

    val m: Optional<AirbyteMessage> = validate(bad.get(), Optional.empty<ConfiguredAirbyteCatalog>(), MessageOrigin.SOURCE)
    Assert.assertTrue(m.isEmpty)
  }

  @Test
  fun testValidState() {
    val rec = createStateMessage(1)

    val m: Optional<AirbyteMessage> = validate(rec, Optional.empty<ConfiguredAirbyteCatalog>(), MessageOrigin.SOURCE)
    Assert.assertTrue(m.isPresent)
    Assert.assertEquals(rec, m.get())
  }

  @Test
  fun testSubtleInvalidState() {
    val bad: Optional<AirbyteMessage> = tryDeserializeExact("{\"type\": \"STATE\", \"control\": {}}", AirbyteMessage::class.java)

    val m: Optional<AirbyteMessage> = validate(bad.get(), Optional.empty<ConfiguredAirbyteCatalog>(), MessageOrigin.SOURCE)
    Assert.assertTrue(m.isEmpty)
  }

  @Test
  fun testValidControl() {
    val rec = createConfigControlMessage(Config(), 1000.0)

    val m: Optional<AirbyteMessage> = validate(rec, Optional.empty<ConfiguredAirbyteCatalog>(), MessageOrigin.SOURCE)
    Assert.assertTrue(m.isPresent)
    Assert.assertEquals(rec, m.get())
  }

  @Test
  fun testSubtleInvalidControl() {
    val bad: Optional<AirbyteMessage> = tryDeserializeExact("{\"type\": \"CONTROL\", \"state\": {}}", AirbyteMessage::class.java)

    val m: Optional<AirbyteMessage> = validate(bad.get(), Optional.empty<ConfiguredAirbyteCatalog>(), MessageOrigin.SOURCE)
    Assert.assertTrue(m.isEmpty)
  }

  @Test
  fun testValidDestinationCatalog() {
    val message =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.DESTINATION_CATALOG)
        .withDestinationCatalog(
          DestinationCatalog().withOperations(
            listOf(
              DestinationOperation().withObjectName("my_object"),
            ),
          ),
        )
    val m: Optional<AirbyteMessage> = validate(message, Optional.empty<ConfiguredAirbyteCatalog>(), MessageOrigin.SOURCE)
    Assert.assertTrue(m.isPresent)
    Assert.assertEquals(message, m.get())
  }

  @Test
  fun testValidPk() {
    val bad: AirbyteMessage = createRecordMessage(STREAM_1, DATA_KEY_1, DATA_VALUE)

    val m: Optional<AirbyteMessage> =
      validate(
        bad,
        Optional.of<ConfiguredAirbyteCatalog>(
          getCatalogWithPk(STREAM_1, listOf(listOf(DATA_KEY_1))),
        ),
        MessageOrigin.SOURCE,
      )
    Assert.assertTrue(m.isPresent)
  }

  @Test
  fun testValidPkWithOneMissingPk() {
    val bad: AirbyteMessage = createRecordMessage(STREAM_1, DATA_KEY_1, DATA_VALUE)

    val m: Optional<AirbyteMessage> =
      validate(
        bad,
        Optional.of<ConfiguredAirbyteCatalog>(
          getCatalogWithPk(STREAM_1, listOf(listOf(DATA_KEY_1), listOf("not_field_1"))),
        ),
        MessageOrigin.SOURCE,
      )
    Assert.assertTrue(m.isPresent)
  }

  @Test
  fun testNotIncrementalDedup() {
    val bad: AirbyteMessage = createRecordMessage(STREAM_1, DATA_KEY_1, DATA_VALUE)

    var m: Optional<AirbyteMessage> =
      validate(
        bad,
        Optional.of<ConfiguredAirbyteCatalog>(
          getCatalogNonIncremental(STREAM_1),
        ),
        MessageOrigin.SOURCE,
      )
    Assert.assertTrue(m.isPresent)

    m =
      validate(
        bad,
        Optional.of<ConfiguredAirbyteCatalog>(
          getCatalogNonIncrementalDedup(STREAM_1),
        ),
        MessageOrigin.SOURCE,
      )
    Assert.assertTrue(m.isPresent)
  }

  @Test
  fun testInvalidPk() {
    val bad: AirbyteMessage = createRecordMessage(STREAM_1, DATA_KEY_1, DATA_VALUE)

    Assert.assertThrows(
      SourceException::class.java,
      {
        validate(
          bad,
          Optional.of<ConfiguredAirbyteCatalog>(
            getCatalogWithPk(STREAM_1, listOf(listOf("not_field_1"))),
          ),
          MessageOrigin.SOURCE,
        )
      },
    )
  }

  @Test
  fun testValidPkInAnotherStream() {
    val bad: AirbyteMessage = createRecordMessage(STREAM_1, DATA_KEY_1, DATA_VALUE)

    Assert.assertThrows(
      SourceException::class.java,
      {
        validate(
          bad,
          Optional.of<ConfiguredAirbyteCatalog>(
            getCatalogWithPk("stream_2", listOf(listOf(DATA_KEY_1))),
          ),
          MessageOrigin.SOURCE,
        )
      },
    )
  }

  private fun getCatalogWithPk(
    streamName: String,
    pksList: Any?,
  ): ConfiguredAirbyteCatalog =
    ConfiguredAirbyteCatalog()
      .withStreams(
        listOf(
          ConfiguredAirbyteStream(
            AirbyteStream(streamName, emptyObject(), listOf(SyncMode.INCREMENTAL)),
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND_DEDUP,
          ).withPrimaryKey(pksList as List<List<String>>?),
        ),
      )

  private fun getCatalogNonIncrementalDedup(streamName: String): ConfiguredAirbyteCatalog =
    ConfiguredAirbyteCatalog()
      .withStreams(
        listOf(
          ConfiguredAirbyteStream(
            AirbyteStream(streamName, emptyObject(), listOf(SyncMode.INCREMENTAL)),
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND,
          ),
        ),
      )

  private fun getCatalogNonIncremental(streamName: String): ConfiguredAirbyteCatalog =
    ConfiguredAirbyteCatalog()
      .withStreams(
        listOf(
          ConfiguredAirbyteStream(
            AirbyteStream(streamName, emptyObject(), listOf(SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
        ),
      )

  companion object {
    private const val DATA_KEY_1 = "field_1"
    private const val STREAM_1 = "stream_1"
    private const val DATA_VALUE = "green"
  }
}
