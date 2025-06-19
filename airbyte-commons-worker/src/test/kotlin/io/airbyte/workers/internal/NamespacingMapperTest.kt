/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.helpers.CatalogHelpers
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.v0.Field
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.workers.testutils.AirbyteMessageUtils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_NAMESPACE: String = "source_namespace"
private const val OUTPUT_PREFIX: String = "output_"
private const val STREAM_NAME: String = "user_preferences"
private const val FIELD_NAME: String = "favorite_color"
private const val BLUE: String = "blue"
private const val NAMESPACE_FORMAT: String = "output"
private const val DESTINATION_NAMESPACE: String = "destination_namespace"

internal class NamespacingMapperTest {
  private val catalogHelpers: CatalogHelpers = CatalogHelpers(FieldGenerator())
  private val catalog: ConfiguredAirbyteCatalog =
    catalogHelpers.createConfiguredAirbyteCatalog(
      STREAM_NAME,
      INPUT_NAMESPACE,
      Field.of(FIELD_NAME, JsonSchemaType.STRING),
    )
  private lateinit var recordMessage: AirbyteMessage
  private lateinit var stateMessage: AirbyteMessage
  private val streamStatusMessage: AirbyteMessage =
    AirbyteMessageUtils.createStreamStatusTraceMessageWithType(
      StreamDescriptor().withName(STREAM_NAME).withNamespace(INPUT_NAMESPACE),
      AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE,
    )
  private lateinit var destinationToSourceNamespaceAndStreamName: MutableMap<NamespaceStreamName, NamespaceStreamName>

  private fun createRecordMessage(): AirbyteMessage {
    val message: AirbyteMessage = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, BLUE)
    message.getRecord().withNamespace(INPUT_NAMESPACE)
    return message
  }

  private fun createStateMessage(): AirbyteMessage {
    val message = AirbyteMessageUtils.createStateMessage(STREAM_NAME, FIELD_NAME, BLUE)
    message
      .getState()
      .getStream()
      .getStreamDescriptor()
      .withNamespace((INPUT_NAMESPACE))
    message.getState().withType(AirbyteStateType.STREAM)
    return message
  }

  @BeforeEach
  fun setUp() {
    recordMessage = createRecordMessage()
    stateMessage = createStateMessage()
    destinationToSourceNamespaceAndStreamName = mutableMapOf()
  }

  @Test
  fun testSourceNamespace() {
    val mapper =
      NamespacingMapper(JobSyncConfig.NamespaceDefinitionType.SOURCE, null, OUTPUT_PREFIX, destinationToSourceNamespaceAndStreamName)

    val originalCatalog = Jsons.clone(catalog)
    val expectedCatalog: ConfiguredAirbyteCatalog =
      catalogHelpers.createConfiguredAirbyteCatalog(
        OUTPUT_PREFIX + STREAM_NAME,
        INPUT_NAMESPACE,
        Field.of(FIELD_NAME, JsonSchemaType.STRING),
      )
    val actualCatalog = mapper.mapCatalog(catalog)

    assertEquals(originalCatalog, catalog)
    assertEquals(expectedCatalog, actualCatalog)

    val originalMessage = Jsons.clone(recordMessage)
    assertEquals(originalMessage, recordMessage)

    val expectedMessage: AirbyteMessage = AirbyteMessageUtils.createRecordMessage(OUTPUT_PREFIX + STREAM_NAME, FIELD_NAME, BLUE)
    expectedMessage.record.withNamespace(INPUT_NAMESPACE)

    val actualMessage = mapper.mapMessage(recordMessage)

    assertEquals(expectedMessage, actualMessage)

    val expectedStreamStatusMessage =
      AirbyteMessageUtils.createStreamStatusTraceMessageWithType(
        StreamDescriptor().withName(OUTPUT_PREFIX + STREAM_NAME),
        AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE,
      )
    expectedStreamStatusMessage.trace.streamStatus.streamDescriptor
      .withNamespace(INPUT_NAMESPACE)

    val actualStreamStatusMessage = mapper.mapMessage(streamStatusMessage)

    assertEquals(expectedStreamStatusMessage, actualStreamStatusMessage)
  }

  @Test
  fun testEmptySourceNamespace() {
    val mapper =
      NamespacingMapper(JobSyncConfig.NamespaceDefinitionType.SOURCE, null, OUTPUT_PREFIX, destinationToSourceNamespaceAndStreamName)

    val originalCatalog = Jsons.clone(catalog)
    assertEquals(originalCatalog, catalog)
    originalCatalog.streams[0].stream.withNamespace(null)
    val expectedCatalog: ConfiguredAirbyteCatalog =
      catalogHelpers.createConfiguredAirbyteCatalog(
        OUTPUT_PREFIX + STREAM_NAME,
        null,
        Field.of(FIELD_NAME, JsonSchemaType.STRING),
      )
    val actualCatalog = mapper.mapCatalog(originalCatalog)

    assertEquals(expectedCatalog, actualCatalog)

    val originalMessage = Jsons.clone(recordMessage)
    assertEquals(originalMessage, recordMessage)
    originalMessage.record.withNamespace(null)

    val originalStreamStatusMessage = Jsons.clone(streamStatusMessage)
    assertEquals(originalStreamStatusMessage, streamStatusMessage)
    originalStreamStatusMessage
      .getTrace()
      .getStreamStatus()
      .getStreamDescriptor()
      .withNamespace(null)

    val expectedMessage: AirbyteMessage = AirbyteMessageUtils.createRecordMessage(OUTPUT_PREFIX + STREAM_NAME, FIELD_NAME, BLUE)
    expectedMessage.record.withNamespace(null)
    val actualMessage = mapper.mapMessage(originalMessage)

    assertEquals(expectedMessage, actualMessage)
  }

  @Test
  fun testDestinationNamespace() {
    val mapper =
      NamespacingMapper(JobSyncConfig.NamespaceDefinitionType.DESTINATION, null, OUTPUT_PREFIX, destinationToSourceNamespaceAndStreamName)

    val originalCatalog = Jsons.clone(catalog)
    val expectedCatalog: ConfiguredAirbyteCatalog =
      catalogHelpers.createConfiguredAirbyteCatalog(
        OUTPUT_PREFIX + STREAM_NAME,
        null,
        Field.of(FIELD_NAME, JsonSchemaType.STRING),
      )
    val actualCatalog = mapper.mapCatalog(catalog)

    assertEquals(originalCatalog, catalog)
    assertEquals(expectedCatalog, actualCatalog)

    val originalMessage = Jsons.clone(recordMessage)
    assertEquals(originalMessage, recordMessage)

    val expectedMessage: AirbyteMessage = AirbyteMessageUtils.createRecordMessage(OUTPUT_PREFIX + STREAM_NAME, FIELD_NAME, BLUE)
    val actualMessage = mapper.mapMessage(recordMessage)
    assertEquals(expectedMessage, actualMessage)

    val expectedStreamStatusMessage =
      AirbyteMessageUtils.createStreamStatusTraceMessageWithType(
        StreamDescriptor().withName(OUTPUT_PREFIX + STREAM_NAME),
        AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE,
      )
    val actualStreamStatusMessage = mapper.mapMessage(streamStatusMessage)
    assertEquals(expectedStreamStatusMessage, actualStreamStatusMessage)
  }

  @Test
  fun testCustomFormatWithVariableNamespace() {
    val mapper =
      NamespacingMapper(
        JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT,
        "\${SOURCE_NAMESPACE}_suffix",
        OUTPUT_PREFIX,
        destinationToSourceNamespaceAndStreamName,
      )

    val expectedNamespace: String = INPUT_NAMESPACE + "_suffix"
    val originalCatalog = Jsons.clone(catalog)
    val expectedCatalog: ConfiguredAirbyteCatalog =
      catalogHelpers.createConfiguredAirbyteCatalog(
        OUTPUT_PREFIX + STREAM_NAME,
        expectedNamespace,
        Field.of(FIELD_NAME, JsonSchemaType.STRING),
      )
    val actualCatalog = mapper.mapCatalog(catalog)

    assertEquals(originalCatalog, catalog)
    assertEquals(expectedCatalog, actualCatalog)

    val originalMessage = Jsons.clone(recordMessage)
    assertEquals(originalMessage, recordMessage)

    val expectedMessage: AirbyteMessage = AirbyteMessageUtils.createRecordMessage(OUTPUT_PREFIX + STREAM_NAME, FIELD_NAME, BLUE)
    expectedMessage.record.withNamespace(expectedNamespace)
    val actualMessage = mapper.mapMessage(recordMessage)

    assertEquals(expectedMessage, actualMessage)

    val expectedStreamStatusMessage =
      AirbyteMessageUtils.createStreamStatusTraceMessageWithType(
        StreamDescriptor().withName(OUTPUT_PREFIX + STREAM_NAME),
        AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE,
      )
    expectedStreamStatusMessage.trace.streamStatus.streamDescriptor
      .withNamespace(expectedNamespace)
    val actualStreamStatusMessage = mapper.mapMessage(streamStatusMessage)

    assertEquals(expectedStreamStatusMessage, actualStreamStatusMessage)
  }

  @Test
  fun testCustomFormatWithoutVariableNamespace() {
    val mapper =
      NamespacingMapper(
        JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT,
        NAMESPACE_FORMAT,
        OUTPUT_PREFIX,
        destinationToSourceNamespaceAndStreamName,
      )

    val expectedNamespace: String = NAMESPACE_FORMAT
    val originalCatalog = Jsons.clone(catalog)
    val expectedCatalog: ConfiguredAirbyteCatalog =
      catalogHelpers.createConfiguredAirbyteCatalog(
        OUTPUT_PREFIX + STREAM_NAME,
        expectedNamespace,
        Field.of(FIELD_NAME, JsonSchemaType.STRING),
      )
    val actualCatalog = mapper.mapCatalog(catalog)

    assertEquals(originalCatalog, catalog)
    assertEquals(expectedCatalog, actualCatalog)

    val originalMessage = Jsons.clone(recordMessage)
    assertEquals(originalMessage, recordMessage)

    val expectedMessage: AirbyteMessage = AirbyteMessageUtils.createRecordMessage(OUTPUT_PREFIX + STREAM_NAME, FIELD_NAME, BLUE)
    expectedMessage.record.withNamespace(expectedNamespace)
    val actualMessage = mapper.mapMessage(recordMessage)

    assertEquals(expectedMessage, actualMessage)

    val expectedStreamStatusMessage =
      AirbyteMessageUtils.createStreamStatusTraceMessageWithType(
        StreamDescriptor().withName(OUTPUT_PREFIX + STREAM_NAME),
        AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE,
      )
    expectedStreamStatusMessage.trace.streamStatus.streamDescriptor
      .withNamespace(expectedNamespace)
    val actualStreamStatusMessage = mapper.mapMessage(streamStatusMessage)

    assertEquals(expectedStreamStatusMessage, actualStreamStatusMessage)
  }

  @Test
  fun testEmptyCustomFormatWithVariableNamespace() {
    val mapper =
      NamespacingMapper(
        JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT,
        "\${SOURCE_NAMESPACE}",
        OUTPUT_PREFIX,
        destinationToSourceNamespaceAndStreamName,
      )

    val originalCatalog = Jsons.clone(catalog)
    assertEquals(originalCatalog, catalog)
    originalCatalog.streams
      .get(0)
      .stream
      .withNamespace(null)
    val expectedCatalog: ConfiguredAirbyteCatalog =
      catalogHelpers.createConfiguredAirbyteCatalog(
        OUTPUT_PREFIX + STREAM_NAME,
        null,
        Field.of(FIELD_NAME, JsonSchemaType.STRING),
      )
    val actualCatalog = mapper.mapCatalog(originalCatalog)

    assertEquals(expectedCatalog, actualCatalog)

    val originalMessage = Jsons.clone(recordMessage)
    assertEquals(originalMessage, recordMessage)
    originalMessage.record.withNamespace(null)

    val expectedMessage: AirbyteMessage = AirbyteMessageUtils.createRecordMessage(OUTPUT_PREFIX + STREAM_NAME, FIELD_NAME, BLUE)
    expectedMessage.record.withNamespace(null)
    val actualMessage = mapper.mapMessage(originalMessage)

    assertEquals(expectedMessage, actualMessage)

    val originalStreamStatusMessage = Jsons.clone(streamStatusMessage)
    assertEquals(originalStreamStatusMessage, streamStatusMessage)
    originalStreamStatusMessage.trace.streamStatus.streamDescriptor
      .withNamespace(null)

    val expectedStreamStatusMessage =
      AirbyteMessageUtils.createStreamStatusTraceMessageWithType(
        StreamDescriptor().withName(OUTPUT_PREFIX + STREAM_NAME),
        AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE,
      )
    expectedStreamStatusMessage.trace.streamStatus.streamDescriptor
      .withNamespace(null)
    val actualStreamStatusMessage = mapper.mapMessage(originalStreamStatusMessage)

    assertEquals(expectedStreamStatusMessage, actualStreamStatusMessage)
  }

  @Test
  fun testMapStateMessage() {
    val mapper =
      NamespacingMapper(
        JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT,
        NAMESPACE_FORMAT,
        OUTPUT_PREFIX,
        destinationToSourceNamespaceAndStreamName,
      )

    val originalMessage = Jsons.clone(stateMessage)
    val expectedMessage = Jsons.clone(stateMessage)

    val expectedNamespace: String = NAMESPACE_FORMAT

    expectedMessage.state.stream.streamDescriptor
      .withNamespace(expectedNamespace)
    expectedMessage.state.stream.streamDescriptor
      .withName(OUTPUT_PREFIX + STREAM_NAME)

    val actualMessage = mapper.mapMessage(originalMessage)
    assertEquals(
      NamespaceStreamName(INPUT_NAMESPACE, STREAM_NAME),
      destinationToSourceNamespaceAndStreamName[
        NamespaceStreamName(
          expectedNamespace,
          OUTPUT_PREFIX + STREAM_NAME,
        ),
      ],
    )
    assertEquals(expectedMessage, actualMessage)
  }

  @Test
  fun testEmptyPrefix() {
    val mapper =
      NamespacingMapper(JobSyncConfig.NamespaceDefinitionType.SOURCE, null, null, destinationToSourceNamespaceAndStreamName)

    val originalCatalog = Jsons.clone(catalog)
    val expectedCatalog: ConfiguredAirbyteCatalog =
      catalogHelpers.createConfiguredAirbyteCatalog(
        STREAM_NAME,
        INPUT_NAMESPACE,
        Field.of(FIELD_NAME, JsonSchemaType.STRING),
      )
    val actualCatalog = mapper.mapCatalog(catalog)

    assertEquals(originalCatalog, catalog)
    assertEquals(expectedCatalog, actualCatalog)

    val originalMessage = Jsons.clone(recordMessage)
    assertEquals(originalMessage, recordMessage)

    val expectedMessage: AirbyteMessage =
      AirbyteMessageUtils.createRecordMessage(
        STREAM_NAME,
        FIELD_NAME,
        BLUE,
      )
    expectedMessage.record.withNamespace(INPUT_NAMESPACE)
    val actualMessage = mapper.mapMessage(recordMessage)

    assertEquals(expectedMessage, actualMessage)
  }

  @Test
  fun testRevertMapStateMessage() {
    val mapper =
      NamespacingMapper(JobSyncConfig.NamespaceDefinitionType.SOURCE, null, OUTPUT_PREFIX, destinationToSourceNamespaceAndStreamName)

    val originalMessage = Jsons.clone(stateMessage)
    originalMessage.state.stream.streamDescriptor
      .withNamespace(DESTINATION_NAMESPACE)
    originalMessage.state.stream.streamDescriptor
      .withName(OUTPUT_PREFIX + STREAM_NAME)

    destinationToSourceNamespaceAndStreamName.put(
      NamespaceStreamName(
        DESTINATION_NAMESPACE,
        OUTPUT_PREFIX + STREAM_NAME,
      ),
      NamespaceStreamName(INPUT_NAMESPACE, STREAM_NAME),
    )

    val actualMessage = mapper.revertMap(originalMessage)

    val expectedMessage = Jsons.clone(stateMessage)

    expectedMessage.state.stream.streamDescriptor
      .withNamespace(INPUT_NAMESPACE)
    expectedMessage.state.stream.streamDescriptor
      .withName(STREAM_NAME)

    assertEquals(expectedMessage, actualMessage)
  }
}
