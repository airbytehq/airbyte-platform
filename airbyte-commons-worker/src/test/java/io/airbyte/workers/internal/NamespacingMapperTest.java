/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NamespacingMapperTest {

  private static final String INPUT_NAMESPACE = "source_namespace";
  private static final String OUTPUT_PREFIX = "output_";
  private static final String STREAM_NAME = "user_preferences";
  private static final String FIELD_NAME = "favorite_color";
  private static final String BLUE = "blue";
  private static final String NAMESPACE_FORMAT = "output";
  private static final String DESTINATION_NAMESPACE = "destination_namespace";

  private static final ConfiguredAirbyteCatalog CATALOG = CatalogHelpers.createConfiguredAirbyteCatalog(
      STREAM_NAME,
      INPUT_NAMESPACE,
      Field.of(FIELD_NAME, JsonSchemaType.STRING));
  private AirbyteMessage recordMessage;
  private AirbyteMessage stateMessage;
  private Map<NamespaceStreamName, NamespaceStreamName> destinationToSourceNamespaceAndStreamName;

  private static AirbyteMessage createRecordMessage() {
    final AirbyteMessage message = AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, BLUE);
    message.getRecord().withNamespace(INPUT_NAMESPACE);
    return message;
  }

  private static AirbyteMessage createStateMessage() {
    final AirbyteMessage message = AirbyteMessageUtils.createStateMessage(STREAM_NAME, FIELD_NAME, BLUE);
    message.getState().getStream().getStreamDescriptor().withNamespace((INPUT_NAMESPACE));
    message.getState().withType(AirbyteStateType.STREAM);
    return message;
  }

  @BeforeEach
  void setUp() {
    recordMessage = createRecordMessage();
    stateMessage = createStateMessage();
    destinationToSourceNamespaceAndStreamName = mock(Map.class);
  }

  @Test
  void testSourceNamespace() {
    final NamespacingMapper mapper =
        new NamespacingMapper(NamespaceDefinitionType.SOURCE, null, OUTPUT_PREFIX, destinationToSourceNamespaceAndStreamName);

    final ConfiguredAirbyteCatalog originalCatalog = Jsons.clone(CATALOG);
    final ConfiguredAirbyteCatalog expectedCatalog = CatalogHelpers.createConfiguredAirbyteCatalog(
        OUTPUT_PREFIX + STREAM_NAME,
        INPUT_NAMESPACE,
        Field.of(FIELD_NAME, JsonSchemaType.STRING));
    final ConfiguredAirbyteCatalog actualCatalog = mapper.mapCatalog(CATALOG);

    assertEquals(originalCatalog, CATALOG);
    assertEquals(expectedCatalog, actualCatalog);

    final AirbyteMessage originalMessage = Jsons.clone(recordMessage);
    assertEquals(originalMessage, recordMessage);

    final AirbyteMessage expectedMessage = AirbyteMessageUtils.createRecordMessage(OUTPUT_PREFIX + STREAM_NAME, FIELD_NAME, BLUE);
    expectedMessage.getRecord().withNamespace(INPUT_NAMESPACE);

    final AirbyteMessage actualMessage = mapper.mapMessage(recordMessage);

    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  void testEmptySourceNamespace() {
    final NamespacingMapper mapper =
        new NamespacingMapper(NamespaceDefinitionType.SOURCE, null, OUTPUT_PREFIX, destinationToSourceNamespaceAndStreamName);

    final ConfiguredAirbyteCatalog originalCatalog = Jsons.clone(CATALOG);
    assertEquals(originalCatalog, CATALOG);
    originalCatalog.getStreams().get(0).getStream().withNamespace(null);
    final ConfiguredAirbyteCatalog expectedCatalog = CatalogHelpers.createConfiguredAirbyteCatalog(
        OUTPUT_PREFIX + STREAM_NAME,
        null,
        Field.of(FIELD_NAME, JsonSchemaType.STRING));
    final ConfiguredAirbyteCatalog actualCatalog = mapper.mapCatalog(originalCatalog);

    assertEquals(expectedCatalog, actualCatalog);

    final AirbyteMessage originalMessage = Jsons.clone(recordMessage);
    assertEquals(originalMessage, recordMessage);
    originalMessage.getRecord().withNamespace(null);

    final AirbyteMessage expectedMessage = AirbyteMessageUtils.createRecordMessage(OUTPUT_PREFIX + STREAM_NAME, FIELD_NAME, BLUE);
    expectedMessage.getRecord().withNamespace(null);
    final AirbyteMessage actualMessage = mapper.mapMessage(originalMessage);

    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  void testDestinationNamespace() {
    final NamespacingMapper mapper =
        new NamespacingMapper(NamespaceDefinitionType.DESTINATION, null, OUTPUT_PREFIX, destinationToSourceNamespaceAndStreamName);

    final ConfiguredAirbyteCatalog originalCatalog = Jsons.clone(CATALOG);
    final ConfiguredAirbyteCatalog expectedCatalog = CatalogHelpers.createConfiguredAirbyteCatalog(
        OUTPUT_PREFIX + STREAM_NAME,
        null,
        Field.of(FIELD_NAME, JsonSchemaType.STRING));
    final ConfiguredAirbyteCatalog actualCatalog = mapper.mapCatalog(CATALOG);

    assertEquals(originalCatalog, CATALOG);
    assertEquals(expectedCatalog, actualCatalog);

    final AirbyteMessage originalMessage = Jsons.clone(recordMessage);
    assertEquals(originalMessage, recordMessage);

    final AirbyteMessage expectedMessage = AirbyteMessageUtils.createRecordMessage(OUTPUT_PREFIX + STREAM_NAME, FIELD_NAME, BLUE);
    final AirbyteMessage actualMessage = mapper.mapMessage(recordMessage);
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  void testCustomFormatWithVariableNamespace() {
    final NamespacingMapper mapper = new NamespacingMapper(
        NamespaceDefinitionType.CUSTOMFORMAT,
        "${SOURCE_NAMESPACE}_suffix",
        OUTPUT_PREFIX,
        destinationToSourceNamespaceAndStreamName);

    final String expectedNamespace = INPUT_NAMESPACE + "_suffix";
    final ConfiguredAirbyteCatalog originalCatalog = Jsons.clone(CATALOG);
    final ConfiguredAirbyteCatalog expectedCatalog = CatalogHelpers.createConfiguredAirbyteCatalog(
        OUTPUT_PREFIX + STREAM_NAME, expectedNamespace,
        Field.of(FIELD_NAME, JsonSchemaType.STRING));
    final ConfiguredAirbyteCatalog actualCatalog = mapper.mapCatalog(CATALOG);

    assertEquals(originalCatalog, CATALOG);
    assertEquals(expectedCatalog, actualCatalog);

    final AirbyteMessage originalMessage = Jsons.clone(recordMessage);
    assertEquals(originalMessage, recordMessage);

    final AirbyteMessage expectedMessage = AirbyteMessageUtils.createRecordMessage(OUTPUT_PREFIX + STREAM_NAME, FIELD_NAME, BLUE);
    expectedMessage.getRecord().withNamespace(expectedNamespace);
    final AirbyteMessage actualMessage = mapper.mapMessage(recordMessage);

    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  void testCustomFormatWithoutVariableNamespace() {
    final NamespacingMapper mapper = new NamespacingMapper(
        NamespaceDefinitionType.CUSTOMFORMAT,
        NAMESPACE_FORMAT,
        OUTPUT_PREFIX,
        destinationToSourceNamespaceAndStreamName);

    final String expectedNamespace = NAMESPACE_FORMAT;
    final ConfiguredAirbyteCatalog originalCatalog = Jsons.clone(CATALOG);
    final ConfiguredAirbyteCatalog expectedCatalog = CatalogHelpers.createConfiguredAirbyteCatalog(
        OUTPUT_PREFIX + STREAM_NAME, expectedNamespace,
        Field.of(FIELD_NAME, JsonSchemaType.STRING));
    final ConfiguredAirbyteCatalog actualCatalog = mapper.mapCatalog(CATALOG);

    assertEquals(originalCatalog, CATALOG);
    assertEquals(expectedCatalog, actualCatalog);

    final AirbyteMessage originalMessage = Jsons.clone(recordMessage);
    assertEquals(originalMessage, recordMessage);

    final AirbyteMessage expectedMessage = AirbyteMessageUtils.createRecordMessage(OUTPUT_PREFIX + STREAM_NAME, FIELD_NAME, BLUE);
    expectedMessage.getRecord().withNamespace(expectedNamespace);
    final AirbyteMessage actualMessage = mapper.mapMessage(recordMessage);

    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  void testEmptyCustomFormatWithVariableNamespace() {
    final NamespacingMapper mapper = new NamespacingMapper(
        NamespaceDefinitionType.CUSTOMFORMAT,
        "${SOURCE_NAMESPACE}",
        OUTPUT_PREFIX,
        destinationToSourceNamespaceAndStreamName);

    final ConfiguredAirbyteCatalog originalCatalog = Jsons.clone(CATALOG);
    assertEquals(originalCatalog, CATALOG);
    originalCatalog.getStreams().get(0).getStream().withNamespace(null);
    final ConfiguredAirbyteCatalog expectedCatalog = CatalogHelpers.createConfiguredAirbyteCatalog(
        OUTPUT_PREFIX + STREAM_NAME,
        null,
        Field.of(FIELD_NAME, JsonSchemaType.STRING));
    final ConfiguredAirbyteCatalog actualCatalog = mapper.mapCatalog(originalCatalog);

    assertEquals(expectedCatalog, actualCatalog);

    final AirbyteMessage originalMessage = Jsons.clone(recordMessage);
    assertEquals(originalMessage, recordMessage);
    originalMessage.getRecord().withNamespace(null);

    final AirbyteMessage expectedMessage = AirbyteMessageUtils.createRecordMessage(OUTPUT_PREFIX + STREAM_NAME, FIELD_NAME, BLUE);
    expectedMessage.getRecord().withNamespace(null);
    final AirbyteMessage actualMessage = mapper.mapMessage(originalMessage);

    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  void testMapStateMessage() {
    final NamespacingMapper mapper = new NamespacingMapper(
        NamespaceDefinitionType.CUSTOMFORMAT,
        NAMESPACE_FORMAT,
        OUTPUT_PREFIX,
        destinationToSourceNamespaceAndStreamName);

    final AirbyteMessage originalMessage = Jsons.clone(stateMessage);
    final AirbyteMessage expectedMessage = Jsons.clone(stateMessage);

    final String expectedNamespace = NAMESPACE_FORMAT;

    expectedMessage.getState().getStream().getStreamDescriptor().withNamespace(expectedNamespace);
    expectedMessage.getState().getStream().getStreamDescriptor().withName(OUTPUT_PREFIX + STREAM_NAME);

    final AirbyteMessage actualMessage = mapper.mapMessage(originalMessage);
    verify(destinationToSourceNamespaceAndStreamName).put(
        new NamespaceStreamName(expectedNamespace, OUTPUT_PREFIX + STREAM_NAME),
        new NamespaceStreamName(INPUT_NAMESPACE, STREAM_NAME));
    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  void testEmptyPrefix() {
    final NamespacingMapper mapper =
        new NamespacingMapper(NamespaceDefinitionType.SOURCE, null, null, destinationToSourceNamespaceAndStreamName);

    final ConfiguredAirbyteCatalog originalCatalog = Jsons.clone(CATALOG);
    final ConfiguredAirbyteCatalog expectedCatalog = CatalogHelpers.createConfiguredAirbyteCatalog(
        STREAM_NAME,
        INPUT_NAMESPACE,
        Field.of(FIELD_NAME, JsonSchemaType.STRING));
    final ConfiguredAirbyteCatalog actualCatalog = mapper.mapCatalog(CATALOG);

    assertEquals(originalCatalog, CATALOG);
    assertEquals(expectedCatalog, actualCatalog);

    final AirbyteMessage originalMessage = Jsons.clone(recordMessage);
    assertEquals(originalMessage, recordMessage);

    final AirbyteMessage expectedMessage = AirbyteMessageUtils.createRecordMessage(
        STREAM_NAME,
        FIELD_NAME, BLUE);
    expectedMessage.getRecord().withNamespace(INPUT_NAMESPACE);
    final AirbyteMessage actualMessage = mapper.mapMessage(recordMessage);

    assertEquals(expectedMessage, actualMessage);
  }

  @Test
  void testRevertMapStateMessage() {
    final NamespacingMapper mapper =
        new NamespacingMapper(NamespaceDefinitionType.SOURCE, null, OUTPUT_PREFIX, destinationToSourceNamespaceAndStreamName);

    final AirbyteMessage originalMessage = Jsons.clone(stateMessage);
    originalMessage.getState().getStream().getStreamDescriptor().withNamespace(DESTINATION_NAMESPACE);
    originalMessage.getState().getStream().getStreamDescriptor().withName(OUTPUT_PREFIX + STREAM_NAME);

    when(destinationToSourceNamespaceAndStreamName.get(new NamespaceStreamName(DESTINATION_NAMESPACE, OUTPUT_PREFIX + STREAM_NAME)))
        .thenReturn(new NamespaceStreamName(INPUT_NAMESPACE, STREAM_NAME));
    final AirbyteMessage actualMessage = mapper.revertMap(originalMessage);

    final AirbyteMessage expectedMessage = Jsons.clone(stateMessage);

    expectedMessage.getState().getStream().getStreamDescriptor().withNamespace(INPUT_NAMESPACE);
    expectedMessage.getState().getStream().getStreamDescriptor().withName(STREAM_NAME);

    assertEquals(expectedMessage, actualMessage);
  }

}
