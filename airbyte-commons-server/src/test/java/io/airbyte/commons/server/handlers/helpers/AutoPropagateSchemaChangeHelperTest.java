/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.commons.server.handlers.helpers.AutoPropagateSchemaChangeHelper.extractStreamAndConfigPerStreamDescriptor;
import static io.airbyte.commons.server.handlers.helpers.AutoPropagateSchemaChangeHelper.getUpdatedSchema;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStream;
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.SyncMode;
import io.airbyte.commons.json.Jsons;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class AutoPropagateSchemaChangeHelperTest {

  private static final String NAME1 = "name1";
  private static final String NAMESPACE1 = "namespace1";
  private static final String NAME2 = "name2";
  private static final String NAMESPACE2 = "namespace2";

  @Test
  void extractStreamAndConfigPerStreamDescriptorTest() {
    AirbyteCatalog airbyteCatalog = new AirbyteCatalog();
    AirbyteStreamAndConfiguration airbyteStreamConfiguration1 = new AirbyteStreamAndConfiguration()
        .stream(new AirbyteStream().name(NAME1).namespace(NAMESPACE1).addSupportedSyncModesItem(SyncMode.FULL_REFRESH));
    AirbyteStreamAndConfiguration airbyteStreamConfiguration2 = new AirbyteStreamAndConfiguration()
        .stream(new AirbyteStream().name(NAME2).namespace(NAMESPACE2).addSupportedSyncModesItem(SyncMode.INCREMENTAL));
    airbyteCatalog.streams(List.of(airbyteStreamConfiguration1, airbyteStreamConfiguration2));

    final Map<StreamDescriptor, AirbyteStreamAndConfiguration> result = extractStreamAndConfigPerStreamDescriptor(airbyteCatalog);

    Assertions.assertThat(result).hasSize(2);
    Assertions.assertThat(result).isEqualTo(
        Map.of(
            new StreamDescriptor().name(NAME1).namespace(NAMESPACE1), airbyteStreamConfiguration1,
            new StreamDescriptor().name(NAME2).namespace(NAMESPACE2), airbyteStreamConfiguration2));
  }

  @Test
  void extractStreamAndConfigPerStreamDescriptorNoNamespaceTest() {
    AirbyteCatalog airbyteCatalog = new AirbyteCatalog();
    AirbyteStreamAndConfiguration airbyteStreamConfiguration1 = new AirbyteStreamAndConfiguration()
        .stream(new AirbyteStream().name(NAME1).addSupportedSyncModesItem(SyncMode.FULL_REFRESH));
    AirbyteStreamAndConfiguration airbyteStreamConfiguration2 = new AirbyteStreamAndConfiguration()
        .stream(new AirbyteStream().name(NAME2).addSupportedSyncModesItem(SyncMode.INCREMENTAL));
    airbyteCatalog.streams(List.of(airbyteStreamConfiguration1, airbyteStreamConfiguration2));

    final Map<StreamDescriptor, AirbyteStreamAndConfiguration> result = extractStreamAndConfigPerStreamDescriptor(airbyteCatalog);

    Assertions.assertThat(result).hasSize(2);
    Assertions.assertThat(result).isEqualTo(
        Map.of(
            new StreamDescriptor().name(NAME1), airbyteStreamConfiguration1,
            new StreamDescriptor().name(NAME2), airbyteStreamConfiguration2));
  }

  @Test
  void applyUpdate() {
    JsonNode oldSchema = Jsons.deserialize("""
                                           {
                                             "schema": "old"
                                           }
                                           """);
    AirbyteCatalog oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema);

    JsonNode newSchema = Jsons.deserialize("""
                                           {
                                             "schema": "new"
                                           }
                                           """);
    AirbyteCatalog newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, newSchema);

    StreamTransform transform = new StreamTransform()
        .streamDescriptor(new StreamDescriptor().name(NAME1))
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM);

    AirbyteCatalog result = getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform));

    Assertions.assertThat(result.getStreams()).hasSize(1);
    Assertions.assertThat(result.getStreams().get(0).getStream().getJsonSchema()).isEqualTo(newSchema);
  }

  @Test
  void applyAdd() {
    JsonNode oldSchema = Jsons.deserialize("""
                                           {
                                             "schema": "old"
                                           }
                                           """);
    AirbyteCatalog oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema);

    JsonNode newSchema = Jsons.deserialize("""
                                           {
                                             "schema": "new"
                                           }
                                           """);
    AirbyteCatalog newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema);

    StreamTransform transform = new StreamTransform()
        .streamDescriptor(new StreamDescriptor().name(NAME2))
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM);

    AirbyteCatalog result = getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform));

    Assertions.assertThat(result.getStreams()).hasSize(2);
    Assertions.assertThat(result.getStreams().get(0).getStream().getName()).isEqualTo(NAME1);
    Assertions.assertThat(result.getStreams().get(0).getStream().getJsonSchema()).isEqualTo(oldSchema);
    Assertions.assertThat(result.getStreams().get(1).getStream().getName()).isEqualTo(NAME2);
    Assertions.assertThat(result.getStreams().get(1).getStream().getJsonSchema()).isEqualTo(newSchema);
  }

  @Test
  void applyRemove() {
    JsonNode oldSchema = Jsons.deserialize("""
                                           {
                                             "schema": "old"
                                           }
                                           """);
    AirbyteCatalog oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema);

    AirbyteCatalog newAirbyteCatalog = new AirbyteCatalog();

    StreamTransform transform = new StreamTransform()
        .streamDescriptor(new StreamDescriptor().name(NAME1))
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM);

    AirbyteCatalog result = getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform));

    Assertions.assertThat(result.getStreams()).hasSize(0);
  }

  private AirbyteCatalog createAirbyteCatalogWithSchema(String name, JsonNode schema) {
    AirbyteCatalog airbyteCatalog = new AirbyteCatalog();

    AirbyteStreamAndConfiguration airbyteStreamConfiguration1 = new AirbyteStreamAndConfiguration()
        .stream(new AirbyteStream().name(name).jsonSchema(schema));

    airbyteCatalog.streams(List.of(airbyteStreamConfiguration1));

    return airbyteCatalog;
  }

}
