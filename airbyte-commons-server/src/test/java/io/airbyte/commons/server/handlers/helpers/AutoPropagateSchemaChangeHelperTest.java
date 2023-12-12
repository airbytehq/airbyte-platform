/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.commons.server.handlers.helpers.AutoPropagateSchemaChangeHelper.extractStreamAndConfigPerStreamDescriptor;
import static io.airbyte.commons.server.handlers.helpers.AutoPropagateSchemaChangeHelper.getUpdatedSchema;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStream;
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.FieldTransform;
import io.airbyte.api.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.SyncMode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class AutoPropagateSchemaChangeHelperTest {

  private static final String NAME1 = "name1";
  private static final String NAMESPACE1 = "namespace1";
  private static final String NAME2 = "name2";
  private static final String NAMESPACE2 = "namespace2";
  private static final String OLD_SCHEMA = """
                                           {
                                             "schema": "old"
                                           }
                                           """;
  private static final String NEW_SCHEMA = """
                                           {
                                             "schema": "old"
                                           }
                                           """;

  private static final List<DestinationSyncMode> SUPPORTED_DESTINATION_SYNC_MODES = List.of(
      DestinationSyncMode.OVERWRITE, DestinationSyncMode.APPEND, DestinationSyncMode.APPEND_DEDUP);
  private FeatureFlagClient featureFlagClient;

  @BeforeEach
  void beforeEach() {
    featureFlagClient = mock(TestClient.class);
    when(featureFlagClient.boolVariation(any(), any())).thenReturn(false);
  }

  @Test
  void extractStreamAndConfigPerStreamDescriptorTest() {
    final AirbyteCatalog airbyteCatalog = new AirbyteCatalog();
    final AirbyteStreamAndConfiguration airbyteStreamConfiguration1 = new AirbyteStreamAndConfiguration()
        .stream(new AirbyteStream().name(NAME1).namespace(NAMESPACE1).addSupportedSyncModesItem(SyncMode.FULL_REFRESH));
    final AirbyteStreamAndConfiguration airbyteStreamConfiguration2 = new AirbyteStreamAndConfiguration()
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
    final AirbyteCatalog airbyteCatalog = new AirbyteCatalog();
    final AirbyteStreamAndConfiguration airbyteStreamConfiguration1 = new AirbyteStreamAndConfiguration()
        .stream(new AirbyteStream().name(NAME1).addSupportedSyncModesItem(SyncMode.FULL_REFRESH));
    final AirbyteStreamAndConfiguration airbyteStreamConfiguration2 = new AirbyteStreamAndConfiguration()
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
    final JsonNode oldSchema = Jsons.deserialize(OLD_SCHEMA);
    final AirbyteCatalog oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema);

    final JsonNode newSchema = Jsons.deserialize(NEW_SCHEMA);
    final AirbyteCatalog newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, newSchema);

    final StreamTransform transform = new StreamTransform()
        .streamDescriptor(new StreamDescriptor().name(NAME1))
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM);

    final AirbyteCatalog result =
        getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
            SUPPORTED_DESTINATION_SYNC_MODES, featureFlagClient,
            UUID.randomUUID()).catalog();

    Assertions.assertThat(result.getStreams()).hasSize(1);
    Assertions.assertThat(result.getStreams().get(0).getStream().getJsonSchema()).isEqualTo(newSchema);
  }

  @Test
  void applyAddNoFlag() {
    final JsonNode oldSchema = Jsons.deserialize(OLD_SCHEMA);
    final AirbyteCatalog oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema);

    final JsonNode newSchema = Jsons.deserialize(NEW_SCHEMA);
    final AirbyteCatalog newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema);

    final StreamTransform transform = new StreamTransform()
        .streamDescriptor(new StreamDescriptor().name(NAME2))
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM);

    final AirbyteCatalog result =
        getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
            SUPPORTED_DESTINATION_SYNC_MODES, featureFlagClient,
            UUID.randomUUID()).catalog();

    Assertions.assertThat(result.getStreams()).hasSize(2);
    Assertions.assertThat(result.getStreams().get(0).getStream().getName()).isEqualTo(NAME1);
    Assertions.assertThat(result.getStreams().get(0).getStream().getJsonSchema()).isEqualTo(oldSchema);
    Assertions.assertThat(result.getStreams().get(0).getConfig().getSelected()).isTrue();
    Assertions.assertThat(result.getStreams().get(1).getStream().getName()).isEqualTo(NAME2);
    Assertions.assertThat(result.getStreams().get(1).getStream().getJsonSchema()).isEqualTo(newSchema);
  }

  @Test
  void applyAdd() {
    final JsonNode oldSchema = Jsons.deserialize(OLD_SCHEMA);
    final AirbyteCatalog oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema);

    final JsonNode newSchema = Jsons.deserialize(NEW_SCHEMA);
    final AirbyteCatalog newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema);

    final StreamTransform transform = new StreamTransform()
        .streamDescriptor(new StreamDescriptor().name(NAME2))
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM);

    final AirbyteCatalog result =
        getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
            SUPPORTED_DESTINATION_SYNC_MODES, featureFlagClient,
            UUID.randomUUID()).catalog();

    Assertions.assertThat(result.getStreams()).hasSize(2);
    final var stream0 = result.getStreams().get(0);
    final var stream1 = result.getStreams().get(1);
    Assertions.assertThat(stream0.getStream().getName()).isEqualTo(NAME1);
    Assertions.assertThat(stream0.getStream().getJsonSchema()).isEqualTo(oldSchema);
    Assertions.assertThat(stream0.getConfig().getSelected()).isTrue();
    Assertions.assertThat(stream1.getStream().getName()).isEqualTo(NAME2);
    Assertions.assertThat(stream1.getStream().getJsonSchema()).isEqualTo(newSchema);
    Assertions.assertThat(stream1.getConfig().getSelected()).isTrue();
    Assertions.assertThat(stream1.getConfig().getSyncMode()).isEqualTo(SyncMode.FULL_REFRESH);
    Assertions.assertThat(stream1.getConfig().getDestinationSyncMode()).isEqualTo(DestinationSyncMode.OVERWRITE);
  }

  @Test
  void applyAddWithSourceDefinedCursor() {
    final JsonNode oldSchema = Jsons.deserialize(OLD_SCHEMA);
    final AirbyteCatalog oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema);

    final JsonNode newSchema = Jsons.deserialize(NEW_SCHEMA);
    final AirbyteCatalog newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema);
    newAirbyteCatalog.getStreams().get(0).getStream().sourceDefinedCursor(true).sourceDefinedPrimaryKey(List.of(List.of("test")));

    final StreamTransform transform = new StreamTransform()
        .streamDescriptor(new StreamDescriptor().name(NAME2))
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM);

    final AirbyteCatalog result =
        getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
            SUPPORTED_DESTINATION_SYNC_MODES, featureFlagClient,
            UUID.randomUUID()).catalog();

    Assertions.assertThat(result.getStreams()).hasSize(2);
    final var stream0 = result.getStreams().get(0);
    final var stream1 = result.getStreams().get(1);
    Assertions.assertThat(stream0.getStream().getName()).isEqualTo(NAME1);
    Assertions.assertThat(stream0.getStream().getJsonSchema()).isEqualTo(oldSchema);
    Assertions.assertThat(stream0.getConfig().getSelected()).isTrue();
    Assertions.assertThat(stream1.getStream().getName()).isEqualTo(NAME2);
    Assertions.assertThat(stream1.getStream().getJsonSchema()).isEqualTo(newSchema);
    Assertions.assertThat(stream1.getConfig().getSelected()).isTrue();
    Assertions.assertThat(stream1.getConfig().getSyncMode()).isEqualTo(SyncMode.INCREMENTAL);
    Assertions.assertThat(stream1.getConfig().getDestinationSyncMode()).isEqualTo(DestinationSyncMode.APPEND_DEDUP);
  }

  @Test
  void applyAddWithSourceDefinedCursorNoPrimaryKey() {
    final JsonNode oldSchema = Jsons.deserialize(OLD_SCHEMA);
    final AirbyteCatalog oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema);

    final JsonNode newSchema = Jsons.deserialize(NEW_SCHEMA);
    final AirbyteCatalog newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema);
    newAirbyteCatalog.getStreams().get(0).getStream().sourceDefinedCursor(true);

    final StreamTransform transform = new StreamTransform()
        .streamDescriptor(new StreamDescriptor().name(NAME2))
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM);

    final AirbyteCatalog result =
        getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
            SUPPORTED_DESTINATION_SYNC_MODES, featureFlagClient,
            UUID.randomUUID()).catalog();

    Assertions.assertThat(result.getStreams()).hasSize(2);
    final var stream1 = result.getStreams().get(1);
    Assertions.assertThat(stream1.getConfig().getSyncMode()).isEqualTo(SyncMode.FULL_REFRESH);
    Assertions.assertThat(stream1.getConfig().getDestinationSyncMode()).isEqualTo(DestinationSyncMode.OVERWRITE);
  }

  @Test
  void applyAddWithSourceDefinedCursorNoPrimaryKeyNoFullRefresh() {
    final JsonNode oldSchema = Jsons.deserialize(OLD_SCHEMA);
    final AirbyteCatalog oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema);

    final JsonNode newSchema = Jsons.deserialize(NEW_SCHEMA);
    final AirbyteCatalog newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema);
    newAirbyteCatalog.getStreams().get(0).getStream().sourceDefinedCursor(true).supportedSyncModes(List.of(SyncMode.INCREMENTAL));

    final StreamTransform transform = new StreamTransform()
        .streamDescriptor(new StreamDescriptor().name(NAME2))
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM);

    final AirbyteCatalog result =
        getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
            SUPPORTED_DESTINATION_SYNC_MODES, featureFlagClient,
            UUID.randomUUID()).catalog();

    Assertions.assertThat(result.getStreams()).hasSize(2);
    final var stream1 = result.getStreams().get(1);
    Assertions.assertThat(stream1.getConfig().getSyncMode()).isEqualTo(SyncMode.INCREMENTAL);
    Assertions.assertThat(stream1.getConfig().getDestinationSyncMode()).isEqualTo(DestinationSyncMode.APPEND);
  }

  @Test
  void applyRemove() {
    final JsonNode oldSchema = Jsons.deserialize(OLD_SCHEMA);
    final AirbyteCatalog oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema);

    final AirbyteCatalog newAirbyteCatalog = new AirbyteCatalog();

    final StreamTransform transform = new StreamTransform()
        .streamDescriptor(new StreamDescriptor().name(NAME1))
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM);

    final AirbyteCatalog result =
        getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
            SUPPORTED_DESTINATION_SYNC_MODES, featureFlagClient,
            UUID.randomUUID()).catalog();

    Assertions.assertThat(result.getStreams()).hasSize(0);
  }

  @Test
  void applyAddNotFully() {
    final JsonNode oldSchema = Jsons.deserialize(OLD_SCHEMA);
    final AirbyteCatalog oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema);

    final JsonNode newSchema = Jsons.deserialize(NEW_SCHEMA);
    final AirbyteCatalog newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema);

    final StreamTransform transform = new StreamTransform()
        .streamDescriptor(new StreamDescriptor().name(NAME2))
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM);

    final AirbyteCatalog result =
        getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_COLUMNS,
            SUPPORTED_DESTINATION_SYNC_MODES, featureFlagClient,
            UUID.randomUUID()).catalog();

    Assertions.assertThat(result.getStreams()).hasSize(1);
    Assertions.assertThat(result.getStreams().get(0).getStream().getName()).isEqualTo(NAME1);
    Assertions.assertThat(result.getStreams().get(0).getStream().getJsonSchema()).isEqualTo(oldSchema);
  }

  @Test
  void applyRemoveNotFully() {
    final JsonNode oldSchema = Jsons.deserialize(OLD_SCHEMA);
    final AirbyteCatalog oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema);

    final AirbyteCatalog newAirbyteCatalog = new AirbyteCatalog();

    final StreamTransform transform = new StreamTransform()
        .streamDescriptor(new StreamDescriptor().name(NAME1))
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM);

    final AirbyteCatalog result =
        getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_COLUMNS,
            SUPPORTED_DESTINATION_SYNC_MODES, featureFlagClient,
            UUID.randomUUID()).catalog();

    Assertions.assertThat(result.getStreams()).hasSize(1);
    Assertions.assertThat(result.getStreams().get(0).getStream().getName()).isEqualTo(NAME1);
    Assertions.assertThat(result.getStreams().get(0).getStream().getJsonSchema()).isEqualTo(oldSchema);
  }

  @Test
  void addStreamFormat() {
    StreamTransform transform = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("bar"));
    Assertions.assertThat(AutoPropagateSchemaChangeHelper.staticFormatDiff(transform)).isEqualTo("Added new stream 'bar.foo'");

    StreamTransform transformNoNS = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo"));
    Assertions.assertThat(AutoPropagateSchemaChangeHelper.staticFormatDiff(transformNoNS)).isEqualTo("Added new stream 'foo'");
  }

  @Test
  void removeStreamFormat() {
    StreamTransform transform = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("bar"));
    Assertions.assertThat(AutoPropagateSchemaChangeHelper.staticFormatDiff(transform)).isEqualTo("Removed stream 'bar.foo'");

    StreamTransform transformNoNS = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo"));
    Assertions.assertThat(AutoPropagateSchemaChangeHelper.staticFormatDiff(transformNoNS)).isEqualTo("Removed stream 'foo'");
  }

  @Test
  void newColumnInStreamFormat() {
    StreamTransform transform = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("bar"))
        .updateStream(List.of(
            new FieldTransform().fieldName(List.of("path", "new_field"))
                .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD),
            new FieldTransform().fieldName(List.of("path", "other_field"))
                .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)));

    Assertions.assertThat(AutoPropagateSchemaChangeHelper.staticFormatDiff(transform))
        .isEqualTo("Modified stream 'bar.foo': Added fields ['path.new_field', 'path.other_field']");
  }

  @Test
  void updatedColumnInStreamFormat() {
    StreamTransform transform = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("bar"))
        .updateStream(List.of(
            new FieldTransform().fieldName(List.of("path", "new_field"))
                .transformType(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA),
            new FieldTransform().fieldName(List.of("path", "other_field"))
                .transformType(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA)));

    Assertions.assertThat(AutoPropagateSchemaChangeHelper.staticFormatDiff(transform))
        .isEqualTo("Modified stream 'bar.foo': Altered fields ['path.new_field', 'path.other_field']");
  }

  @Test
  void removedColumnsInStreamFormat() {
    StreamTransform transform = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("bar"))
        .updateStream(List.of(
            new FieldTransform().fieldName(List.of("path", "new_field"))
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD),
            new FieldTransform().fieldName(List.of("other_field"))
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD)));

    Assertions.assertThat(AutoPropagateSchemaChangeHelper.staticFormatDiff(transform))
        .isEqualTo("Modified stream 'bar.foo': Removed fields ['path.new_field', 'other_field']");
  }

  private AirbyteCatalog createAirbyteCatalogWithSchema(final String name, final JsonNode schema) {
    final AirbyteCatalog airbyteCatalog = new AirbyteCatalog();

    final AirbyteStreamAndConfiguration airbyteStreamConfiguration1 = new AirbyteStreamAndConfiguration()
        .stream(new AirbyteStream().name(name).jsonSchema(schema).supportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL)))
        .config(new AirbyteStreamConfiguration().selected(true));

    airbyteCatalog.streams(List.of(airbyteStreamConfiguration1));

    return airbyteCatalog;
  }

  @SuppressWarnings("LineLength")
  @Test
  void mixedChangesInStreamFormat() {
    StreamTransform transform = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("bar"))
        .updateStream(List.of(
            new FieldTransform().fieldName(List.of("path", "new_field"))
                .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD),
            new FieldTransform().fieldName(List.of("old_field"))
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD),
            new FieldTransform().fieldName(List.of("old_path", "deprecated"))
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD),
            new FieldTransform().fieldName(List.of("properties", "changed_type"))
                .transformType(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA)));

    Assertions.assertThat(AutoPropagateSchemaChangeHelper.staticFormatDiff(transform))
        .isEqualTo(
            "Modified stream 'bar.foo': Added fields ['path.new_field'], Removed fields ['old_field', 'old_path.deprecated'], Altered fields ['properties.changed_type']");
  }

}
