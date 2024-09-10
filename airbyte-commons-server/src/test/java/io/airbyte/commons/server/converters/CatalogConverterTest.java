/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters;

import static io.airbyte.commons.server.helpers.ConnectionHelpers.FIELD_NAME;
import static io.airbyte.commons.server.helpers.ConnectionHelpers.SECOND_FIELD_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.SelectedFieldInfo;
import io.airbyte.api.model.generated.SyncMode;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.helpers.ConnectionHelpers;
import io.airbyte.config.ConfiguredMapper;
import io.airbyte.config.DataType;
import io.airbyte.config.Field;
import io.airbyte.config.FieldSelectionData;
import io.airbyte.config.FieldType;
import io.airbyte.mappers.helpers.MapperHelperKt;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.validation.json.JsonValidationException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CatalogConverterTest {

  @Test
  void testConvertToProtocol() throws JsonValidationException {
    assertEquals(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog(),
        CatalogConverter.toConfiguredInternal(ConnectionHelpers.generateBasicApiCatalog(), true));
  }

  @Test
  void testConvertToAPI() {
    assertEquals(ConnectionHelpers.generateBasicApiCatalog(), CatalogConverter.toApi(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog(),
        new FieldSelectionData()));
  }

  @Test
  void testEnumConversion() {
    assertTrue(Enums.isCompatible(io.airbyte.api.model.generated.DataType.class, DataType.class));
    assertTrue(Enums.isCompatible(io.airbyte.config.SyncMode.class, io.airbyte.api.model.generated.SyncMode.class));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testConvertInternal(final boolean enableMappers) throws JsonValidationException {
    final var apiCatalog = ConnectionHelpers.generateApiCatalogWithTwoFields();
    final var apiStream = apiCatalog.getStreams().getFirst();
    apiStream.getConfig().setHashedFields(List.of(new SelectedFieldInfo().fieldPath(List.of(SECOND_FIELD_NAME))));

    final var internalCatalog = CatalogConverter.toConfiguredInternal(apiCatalog, enableMappers);
    assertEquals(1, internalCatalog.getStreams().size());
    final var internalStream = internalCatalog.getStreams().getFirst();
    if (enableMappers) {
      final var mappers = internalStream.getMappers();
      assertEquals(1, mappers.size());

      final var fields = internalStream.getFields();
      assertEquals(2, fields.size());

      final ConfiguredMapper expectedMapper = MapperHelperKt.createHashingMapper(SECOND_FIELD_NAME);
      assertEquals(expectedMapper, mappers.getFirst());
    } else {
      final var mappers = internalStream.getMappers();
      assertEquals(0, mappers.size());
      final var fields = internalStream.getFields();
      assertEquals(null, fields);
    }

  }

  @Test
  void testConvertToInternalOnlyIncludeSelectedFields() throws JsonValidationException {
    final var apiCatalog = ConnectionHelpers.generateApiCatalogWithTwoFields();
    final var apiStream = apiCatalog.getStreams().getFirst();
    assertEquals(2, apiStream.getStream().getJsonSchema().get("properties").size());

    apiStream.getConfig().fieldSelectionEnabled(true).addSelectedFieldsItem(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME));

    final var internalCatalog = CatalogConverter.toConfiguredInternal(apiCatalog, true);
    assertEquals(1, internalCatalog.getStreams().size());

    final var internalStream = internalCatalog.getStreams().getFirst();
    final var properties = (ObjectNode) internalStream.getStream().getJsonSchema().get("properties");
    assertEquals(1, properties.size());
    assertTrue(properties.has(FIELD_NAME));

    final var fields = internalStream.getFields();
    assertNotNull(fields);
    assertEquals(1, fields.size());

    final var expectedField = new Field(FIELD_NAME, FieldType.STRING);
    assertEquals(expectedField, fields.getFirst());
  }

  @Test
  void testConvertToProtocolColumnSelectionValidation() {
    assertThrows(JsonValidationException.class, () -> {
      // fieldSelectionEnabled=true but selectedFields=null.
      final var catalog = ConnectionHelpers.generateBasicApiCatalog();
      catalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).selectedFields(null);
      CatalogConverter.toConfiguredInternal(catalog, true);
    });

    assertThrows(JsonValidationException.class, () -> {
      // JSON schema has no `properties` node.
      final var catalog = ConnectionHelpers.generateBasicApiCatalog();
      ((ObjectNode) catalog.getStreams().get(0).getStream().getJsonSchema()).remove("properties");
      catalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).addSelectedFieldsItem(new SelectedFieldInfo().addFieldPathItem("foo"));
      CatalogConverter.toConfiguredInternal(catalog, true);
    });

    assertThrows(JsonValidationException.class, () -> {
      // SelectedFieldInfo with empty path.
      final var catalog = ConnectionHelpers.generateBasicApiCatalog();
      catalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).addSelectedFieldsItem(new SelectedFieldInfo());
      CatalogConverter.toConfiguredInternal(catalog, true);
    });

    assertThrows(UnsupportedOperationException.class, () -> {
      // SelectedFieldInfo with nested field path.
      final var catalog = ConnectionHelpers.generateBasicApiCatalog();
      catalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true)
          .addSelectedFieldsItem(new SelectedFieldInfo().addFieldPathItem("foo").addFieldPathItem("bar"));
      CatalogConverter.toConfiguredInternal(catalog, true);
    });

    assertThrows(JsonValidationException.class, () -> {
      // SelectedFieldInfo with empty path.
      final var catalog = ConnectionHelpers.generateBasicApiCatalog();
      catalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).addSelectedFieldsItem(new SelectedFieldInfo().addFieldPathItem("foo"));
      CatalogConverter.toConfiguredInternal(catalog, true);
    });

    assertThrows(JsonValidationException.class, () -> {
      final var catalog = ConnectionHelpers.generateApiCatalogWithTwoFields();
      // Only FIELD_NAME is selected.
      catalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).addSelectedFieldsItem(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME));
      // The sync mode is INCREMENTAL and SECOND_FIELD_NAME is a cursor field.
      catalog.getStreams().get(0).getConfig().syncMode(SyncMode.INCREMENTAL).cursorField(List.of(SECOND_FIELD_NAME));
      CatalogConverter.toConfiguredInternal(catalog, true);
    });

    assertDoesNotThrow(() -> {
      final var catalog = ConnectionHelpers.generateApiCatalogWithTwoFields();
      // Only FIELD_NAME is selected.
      catalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).addSelectedFieldsItem(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME));
      // The cursor field is not selected, but it's okay because it's FULL_REFRESH so it doesn't throw.
      catalog.getStreams().get(0).getConfig().syncMode(SyncMode.FULL_REFRESH).cursorField(List.of(SECOND_FIELD_NAME));
      CatalogConverter.toConfiguredInternal(catalog, true);
    });

    assertThrows(JsonValidationException.class, () -> {
      final var catalog = ConnectionHelpers.generateApiCatalogWithTwoFields();
      // Only FIELD_NAME is selected.
      catalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).addSelectedFieldsItem(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME));
      // The destination sync mode is DEDUP and SECOND_FIELD_NAME is a primary key.
      catalog.getStreams().get(0).getConfig().destinationSyncMode(DestinationSyncMode.APPEND_DEDUP).primaryKey(List.of(List.of(SECOND_FIELD_NAME)));
      CatalogConverter.toConfiguredInternal(catalog, true);
    });

    assertDoesNotThrow(() -> {
      final var catalog = ConnectionHelpers.generateApiCatalogWithTwoFields();
      // Only FIELD_NAME is selected.
      catalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).addSelectedFieldsItem(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME));
      // The primary key is not selected but that's okay because the destination sync mode is OVERWRITE.
      catalog.getStreams().get(0).getConfig().destinationSyncMode(DestinationSyncMode.OVERWRITE).primaryKey(List.of(List.of(SECOND_FIELD_NAME)));
      CatalogConverter.toConfiguredInternal(catalog, true);
    });
  }

  @Test
  void testConvertToProtocolFieldSelection() throws JsonValidationException {
    final var catalog = ConnectionHelpers.generateApiCatalogWithTwoFields();
    catalog.getStreams().get(0).getConfig().fieldSelectionEnabled(true).addSelectedFieldsItem(new SelectedFieldInfo().addFieldPathItem(FIELD_NAME));
    assertEquals(ConnectionHelpers.generateBasicConfiguredAirbyteCatalog(), CatalogConverter.toConfiguredInternal(catalog, true));
  }

  @Test
  void testDiscoveredToApiDefaultSyncModesNoSourceCursor() throws JsonValidationException {
    final AirbyteCatalog persistedCatalog = CatalogConverter.toProtocol(ConnectionHelpers.generateBasicApiCatalog());
    final var actualStreamConfig = CatalogConverter.toApi(persistedCatalog, null).getStreams().get(0).getConfig();
    final var actualSyncMode = actualStreamConfig.getSyncMode();
    final var actualDestinationSyncMode = actualStreamConfig.getDestinationSyncMode();
    assertEquals(SyncMode.FULL_REFRESH, actualSyncMode);
    assertEquals(DestinationSyncMode.OVERWRITE, actualDestinationSyncMode);
  }

  @Test
  void testDiscoveredToApiDefaultSyncModesSourceCursorAndPrimaryKey() throws JsonValidationException {
    final AirbyteCatalog persistedCatalog = CatalogConverter.toProtocol(ConnectionHelpers.generateBasicApiCatalog());
    persistedCatalog.getStreams().get(0).withSourceDefinedCursor(true).withSourceDefinedPrimaryKey(List.of(List.of("unused")));
    final var actualStreamConfig = CatalogConverter.toApi(persistedCatalog, null).getStreams().get(0).getConfig();
    final var actualSyncMode = actualStreamConfig.getSyncMode();
    final var actualDestinationSyncMode = actualStreamConfig.getDestinationSyncMode();
    assertEquals(SyncMode.INCREMENTAL, actualSyncMode);
    assertEquals(DestinationSyncMode.APPEND_DEDUP, actualDestinationSyncMode);
  }

  @Test
  void testDiscoveredToApiDefaultSyncModesSourceCursorNoPrimaryKey() throws JsonValidationException {
    final AirbyteCatalog persistedCatalog = CatalogConverter.toProtocol(ConnectionHelpers.generateBasicApiCatalog());
    persistedCatalog.getStreams().get(0).withSourceDefinedCursor(true);
    final var actualStreamConfig = CatalogConverter.toApi(persistedCatalog, null).getStreams().get(0).getConfig();
    final var actualSyncMode = actualStreamConfig.getSyncMode();
    final var actualDestinationSyncMode = actualStreamConfig.getDestinationSyncMode();
    assertEquals(SyncMode.FULL_REFRESH, actualSyncMode);
    assertEquals(DestinationSyncMode.OVERWRITE, actualDestinationSyncMode);
  }

  @Test
  void testDiscoveredToApiDefaultSyncModesSourceCursorNoFullRefresh() throws JsonValidationException {
    final AirbyteCatalog persistedCatalog = CatalogConverter.toProtocol(ConnectionHelpers.generateBasicApiCatalog());
    persistedCatalog.getStreams().get(0).withSourceDefinedCursor(true)
        .withSupportedSyncModes(List.of(io.airbyte.protocol.models.SyncMode.INCREMENTAL));
    final var actualStreamConfig = CatalogConverter.toApi(persistedCatalog, null).getStreams().get(0).getConfig();
    final var actualSyncMode = actualStreamConfig.getSyncMode();
    final var actualDestinationSyncMode = actualStreamConfig.getDestinationSyncMode();
    assertEquals(SyncMode.INCREMENTAL, actualSyncMode);
    assertEquals(DestinationSyncMode.APPEND, actualDestinationSyncMode);
  }

}
