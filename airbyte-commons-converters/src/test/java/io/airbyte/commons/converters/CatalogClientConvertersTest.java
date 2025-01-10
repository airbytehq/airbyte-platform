/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.client.model.generated.ConfiguredStreamMapper;
import io.airbyte.api.client.model.generated.StreamMapperType;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.text.Names;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.mapper.configs.HashingMapperConfig;
import io.airbyte.mappers.helpers.MapperHelperKt;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.SyncMode;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@MicronautTest
class CatalogClientConvertersTest {

  @Inject
  private FieldGenerator fieldGenerator;
  @Inject
  private CatalogClientConverters catalogClientConverters;
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
        catalogClientConverters.toAirbyteCatalogClientApi(BASIC_MODEL_CATALOG));
  }

  @Test
  void testConvertToProtocol() {
    assertEquals(BASIC_MODEL_CATALOG,
        catalogClientConverters.toAirbyteProtocol(EXPECTED_CLIENT_CATALOG));
  }

  @Test
  void testConvertInternalWithMapping() {
    final UUID mapperId = UUID.randomUUID();
    final HashingMapperConfig hashingMapper = MapperHelperKt.createHashingMapper(ID_FIELD_NAME, mapperId);

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
        List.of(new ConfiguredStreamMapper(StreamMapperType.HASHING, Jsons.jsonNode(hashingMapper.getConfig()), mapperId)),
        null,
        null,
        null);
    final io.airbyte.api.client.model.generated.AirbyteCatalog clientCatalog =
        new io.airbyte.api.client.model.generated.AirbyteCatalog(
            List.of(
                new io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration(
                    CLIENT_STREAM,
                    streamConfig)));

    final ConfiguredAirbyteCatalog configuredCatalog = catalogClientConverters.toConfiguredAirbyteInternal(clientCatalog);
    final var stream = configuredCatalog.getStreams().getFirst();
    assertEquals(STREAM_NAME, stream.getStream().getName());
    assertEquals(1, stream.getFields().size());
    assertEquals(1, stream.getMappers().size());
    assertEquals(fieldGenerator.getFieldsFromSchema(stream.getStream().getJsonSchema()), stream.getFields());
    assertEquals(hashingMapper, stream.getMappers().getFirst());
  }

  @Test
  void testIsResumableImport() {
    final List<Boolean> boolValues = new ArrayList<>(List.of(Boolean.TRUE, Boolean.FALSE));
    boolValues.add(null);
    for (final Boolean isResumable : boolValues) {
      final AirbyteCatalog catalog = new AirbyteCatalog()
          .withStreams(List.of(new AirbyteStream().withName("user").withIsResumable(isResumable)));
      final io.airbyte.api.client.model.generated.AirbyteCatalog apiCatalog = catalogClientConverters.toAirbyteCatalogClientApi(catalog);
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

      final AirbyteCatalog catalog = catalogClientConverters.toAirbyteProtocol(apiCatalog);
      assertEquals(isResumable, catalog.getStreams().get(0).getIsResumable());
    }
  }

}
