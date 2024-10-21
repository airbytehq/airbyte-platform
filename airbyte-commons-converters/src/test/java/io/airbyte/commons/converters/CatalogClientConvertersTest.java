/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Lists;
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.client.model.generated.ConfiguredStreamMapper;
import io.airbyte.api.client.model.generated.StreamMapperType;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.text.Names;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredMapper;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.mappers.helpers.MapperHelperKt;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.SyncMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class CatalogClientConvertersTest {

  private static final FieldGenerator fieldGenerator = spy(new FieldGenerator());
  public static final String ID_FIELD_NAME = "id";
  private static final String STREAM_NAME = "users-data";
  private static final AirbyteStream STREAM = new AirbyteStream()
      .withName(STREAM_NAME)
      .withJsonSchema(
          CatalogHelpers.fieldsToJsonSchema(Field.of(ID_FIELD_NAME, JsonSchemaType.STRING)))
      .withDefaultCursorField(Lists.newArrayList(ID_FIELD_NAME))
      .withSourceDefinedCursor(false)
      .withSourceDefinedPrimaryKey(Collections.emptyList())
      .withSupportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));

  private static final io.airbyte.api.client.model.generated.AirbyteStream CLIENT_STREAM =
      new io.airbyte.api.client.model.generated.AirbyteStream(
          STREAM_NAME,
          CatalogHelpers.fieldsToJsonSchema(Field.of(ID_FIELD_NAME, JsonSchemaType.STRING)),
          List.of(io.airbyte.api.client.model.generated.SyncMode.FULL_REFRESH,
              io.airbyte.api.client.model.generated.SyncMode.INCREMENTAL),
          false,
          List.of(ID_FIELD_NAME),
          List.of(),
          null,
          null);
  private static final io.airbyte.api.client.model.generated.AirbyteStreamConfiguration CLIENT_DEFAULT_STREAM_CONFIGURATION =
      new io.airbyte.api.client.model.generated.AirbyteStreamConfiguration(
          io.airbyte.api.client.model.generated.SyncMode.FULL_REFRESH,
          io.airbyte.api.client.model.generated.DestinationSyncMode.APPEND,
          List.of(ID_FIELD_NAME),
          List.of(),
          Names.toAlphanumericAndUnderscore(STREAM_NAME),
          true,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null);

  private static final AirbyteCatalog BASIC_MODEL_CATALOG = new AirbyteCatalog().withStreams(
      Lists.newArrayList(STREAM));

  private static final io.airbyte.api.client.model.generated.AirbyteCatalog EXPECTED_CLIENT_CATALOG =
      new io.airbyte.api.client.model.generated.AirbyteCatalog(
          List.of(
              new io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration(
                  CLIENT_STREAM,
                  CLIENT_DEFAULT_STREAM_CONFIGURATION)));

  @Test
  void testConvertToClientAPI() {
    assertEquals(EXPECTED_CLIENT_CATALOG,
        CatalogClientConverters.toAirbyteCatalogClientApi(BASIC_MODEL_CATALOG));
  }

  @Test
  void testConvertToProtocol() {
    assertEquals(BASIC_MODEL_CATALOG,
        CatalogClientConverters.toAirbyteProtocol(EXPECTED_CLIENT_CATALOG));
  }

  @Test
  void testConvertInternalWithMapping() {
    reset(fieldGenerator);

    final ConfiguredMapper hashingMapper = MapperHelperKt.createHashingMapper(ID_FIELD_NAME);

    final var streamConfig = new io.airbyte.api.client.model.generated.AirbyteStreamConfiguration(
        io.airbyte.api.client.model.generated.SyncMode.FULL_REFRESH,
        io.airbyte.api.client.model.generated.DestinationSyncMode.APPEND,
        List.of(ID_FIELD_NAME),
        List.of(),
        Names.toAlphanumericAndUnderscore(STREAM_NAME),
        true,
        null,
        null,
        null,
        null,
        List.of(new ConfiguredStreamMapper(StreamMapperType.HASHING, Jsons.jsonNode(hashingMapper.getConfig()))),
        null,
        null,
        null);
    final io.airbyte.api.client.model.generated.AirbyteCatalog clientCatalog =
        new io.airbyte.api.client.model.generated.AirbyteCatalog(
            List.of(
                new io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration(
                    CLIENT_STREAM,
                    streamConfig)));

    final ConfiguredAirbyteCatalog configuredCatalog = CatalogClientConverters.toConfiguredAirbyteInternal(clientCatalog);
    final var stream = configuredCatalog.getStreams().getFirst();
    assertEquals(STREAM_NAME, stream.getStream().getName());
    assertEquals(1, stream.getFields().size());
    assertEquals(1, stream.getMappers().size());
    assertEquals(fieldGenerator.getFieldsFromSchema(stream.getStream().getJsonSchema()), stream.getFields());
    assertEquals(MapperHelperKt.createHashingMapper(ID_FIELD_NAME), stream.getMappers().getFirst());
  }

  @Test
  void testIsResumableImport() {
    final List<Boolean> boolValues = new ArrayList<>(List.of(Boolean.TRUE, Boolean.FALSE));
    boolValues.add(null);
    for (final Boolean isResumable : boolValues) {
      final AirbyteCatalog catalog = new AirbyteCatalog()
          .withStreams(List.of(new AirbyteStream().withName("user").withIsResumable(isResumable)));
      final io.airbyte.api.client.model.generated.AirbyteCatalog apiCatalog = CatalogClientConverters.toAirbyteCatalogClientApi(catalog);
      assertEquals(isResumable, apiCatalog.getStreams().get(0).getStream().isResumable());
    }
  }

  @Test
  void testIsResumableExport() {
    final List<Boolean> boolValues = new ArrayList<>(List.of(Boolean.TRUE, Boolean.FALSE));
    boolValues.add(null);
    for (final Boolean isResumable : boolValues) {
      final var stream = new io.airbyte.api.client.model.generated.AirbyteStream(
          STREAM_NAME,
          CatalogHelpers.fieldsToJsonSchema(Field.of(ID_FIELD_NAME, JsonSchemaType.STRING)),
          List.of(io.airbyte.api.client.model.generated.SyncMode.FULL_REFRESH,
              io.airbyte.api.client.model.generated.SyncMode.INCREMENTAL),
          false,
          List.of(ID_FIELD_NAME),
          List.of(),
          null,
          isResumable);
      final var conf = new io.airbyte.api.client.model.generated.AirbyteStreamConfiguration(
          io.airbyte.api.client.model.generated.SyncMode.FULL_REFRESH,
          io.airbyte.api.client.model.generated.DestinationSyncMode.APPEND,
          List.of(ID_FIELD_NAME),
          List.of(),
          Names.toAlphanumericAndUnderscore(STREAM_NAME),
          true,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null);
      final var streamAndConf = new AirbyteStreamAndConfiguration(stream, conf);
      final List<AirbyteStreamAndConfiguration> streams = List.of(streamAndConf);
      final var apiCatalog = new io.airbyte.api.client.model.generated.AirbyteCatalog(streams);

      final AirbyteCatalog catalog = CatalogClientConverters.toAirbyteProtocol(apiCatalog);
      assertEquals(isResumable, catalog.getStreams().get(0).getIsResumable());
    }
  }

}
