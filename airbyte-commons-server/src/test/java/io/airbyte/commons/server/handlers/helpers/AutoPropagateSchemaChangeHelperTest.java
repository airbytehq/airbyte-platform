/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStream;
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.FieldTransform;
import io.airbyte.api.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.model.generated.SelectedFieldInfo;
import io.airbyte.api.model.generated.StreamAttributeTransform;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.StreamTransformUpdateStream;
import io.airbyte.api.model.generated.SyncMode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class AutoPropagateSchemaChangeHelperTest {

  private final ApplySchemaChangeHelper applySchemaChangeHelper =
      new ApplySchemaChangeHelper(new CatalogConverter(new FieldGenerator(), Collections.emptyList()));

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

    final Map<StreamDescriptor, AirbyteStreamAndConfiguration> result =
        applySchemaChangeHelper.extractStreamAndConfigPerStreamDescriptor(airbyteCatalog);

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

    final Map<StreamDescriptor, AirbyteStreamAndConfiguration> result =
        applySchemaChangeHelper.extractStreamAndConfigPerStreamDescriptor(airbyteCatalog);

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
        applySchemaChangeHelper
            .getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
                SUPPORTED_DESTINATION_SYNC_MODES)
            .catalog();

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
        applySchemaChangeHelper
            .getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
                SUPPORTED_DESTINATION_SYNC_MODES)
            .catalog();

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
        applySchemaChangeHelper
            .getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
                SUPPORTED_DESTINATION_SYNC_MODES)
            .catalog();

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
        applySchemaChangeHelper
            .getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
                SUPPORTED_DESTINATION_SYNC_MODES)
            .catalog();

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
        applySchemaChangeHelper
            .getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
                SUPPORTED_DESTINATION_SYNC_MODES)
            .catalog();

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
        applySchemaChangeHelper
            .getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
                SUPPORTED_DESTINATION_SYNC_MODES)
            .catalog();

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
        applySchemaChangeHelper
            .getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_FULLY,
                SUPPORTED_DESTINATION_SYNC_MODES)
            .catalog();

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
        applySchemaChangeHelper
            .getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_COLUMNS,
                SUPPORTED_DESTINATION_SYNC_MODES)
            .catalog();

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
        applySchemaChangeHelper
            .getUpdatedSchema(oldAirbyteCatalog, newAirbyteCatalog, List.of(transform), NonBreakingChangesPreference.PROPAGATE_COLUMNS,
                SUPPORTED_DESTINATION_SYNC_MODES)
            .catalog();

    Assertions.assertThat(result.getStreams()).hasSize(1);
    Assertions.assertThat(result.getStreams().get(0).getStream().getName()).isEqualTo(NAME1);
    Assertions.assertThat(result.getStreams().get(0).getStream().getJsonSchema()).isEqualTo(oldSchema);
  }

  @Test
  void addStreamFormat() {
    final StreamTransform transform = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("bar"));
    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transform)).isEqualTo("Added new stream 'bar.foo'");

    final StreamTransform transformNoNS = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo"));
    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transformNoNS)).isEqualTo("Added new stream 'foo'");
  }

  @Test
  void removeStreamFormat() {
    final StreamTransform transform = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("bar"));
    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transform)).isEqualTo("Removed stream 'bar.foo'");

    final StreamTransform transformNoNS = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo"));
    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transformNoNS)).isEqualTo("Removed stream 'foo'");
  }

  @Test
  void newColumnInStreamFormat() {
    final StreamTransform transform = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("bar"))
        .updateStream(new StreamTransformUpdateStream().fieldTransforms(List.of(
            new FieldTransform().fieldName(List.of("path", "new_field"))
                .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD),
            new FieldTransform().fieldName(List.of("path", "other_field"))
                .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD))));

    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transform))
        .isEqualTo("Modified stream 'bar.foo': Added fields ['path.new_field', 'path.other_field']");
  }

  @Test
  void updatedColumnInStreamFormat() {
    final StreamTransform transform = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("bar"))
        .updateStream(new StreamTransformUpdateStream().fieldTransforms(List.of(
            new FieldTransform().fieldName(List.of("path", "new_field"))
                .transformType(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA),
            new FieldTransform().fieldName(List.of("path", "other_field"))
                .transformType(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA))));

    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transform))
        .isEqualTo("Modified stream 'bar.foo': Altered fields ['path.new_field', 'path.other_field']");
  }

  @Test
  void removedColumnsInStreamFormat() {
    final StreamTransform transform = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("bar"))
        .updateStream(new StreamTransformUpdateStream().fieldTransforms(List.of(
            new FieldTransform().fieldName(List.of("path", "new_field"))
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD),
            new FieldTransform().fieldName(List.of("other_field"))
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD))));

    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transform))
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
    final StreamTransform transform = new StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(new StreamDescriptor().name("foo").namespace("bar"))
        .updateStream(new StreamTransformUpdateStream().fieldTransforms(List.of(
            new FieldTransform().fieldName(List.of("path", "new_field"))
                .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD),
            new FieldTransform().fieldName(List.of("old_field"))
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD),
            new FieldTransform().fieldName(List.of("old_path", "deprecated"))
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD),
            new FieldTransform().fieldName(List.of("properties", "changed_type"))
                .transformType(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA))));

    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transform))
        .isEqualTo(
            "Modified stream 'bar.foo': Added fields ['path.new_field'], Removed fields ['old_field', 'old_path.deprecated'], Altered fields ['properties.changed_type']");
  }

  @Test
  void emptyDiffShouldAlwaysPropagate() {
    Assertions.assertThat(applySchemaChangeHelper.shouldAutoPropagate(new CatalogDiff(),
        new ConnectionRead().nonBreakingChangesPreference(NonBreakingChangesPreference.IGNORE))).isTrue();
  }

  @Test
  void emptyDiffCanBeApplied() {
    final JsonNode oldSchema = Jsons.deserialize(OLD_SCHEMA);
    final AirbyteCatalog oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema);

    final ApplySchemaChangeHelper.UpdateSchemaResult result =
        applySchemaChangeHelper.getUpdatedSchema(oldAirbyteCatalog, oldAirbyteCatalog, List.of(),
            NonBreakingChangesPreference.PROPAGATE_FULLY,
            SUPPORTED_DESTINATION_SYNC_MODES);

    Assertions.assertThat(result.catalog()).isEqualTo(oldAirbyteCatalog);
    Assertions.assertThat(result.appliedDiff().getTransforms()).isEmpty();
    Assertions.assertThat(result.changeDescription()).isEmpty();
  }

  @Test
  void testContainsBreakingChange() {
    final StreamTransformUpdateStream updateWithNoBreakingTransforms = new StreamTransformUpdateStream()
        .addFieldTransformsItem(new FieldTransform().breaking(false))
        .addStreamAttributeTransformsItem(new StreamAttributeTransform().breaking(false));
    final CatalogDiff catalogDiff1 = new CatalogDiff().transforms(List.of(
        new StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM).updateStream(updateWithNoBreakingTransforms)));

    assertFalse(applySchemaChangeHelper.containsBreakingChange(catalogDiff1));

    final StreamTransformUpdateStream updateWithBreakingFieldTransform = new StreamTransformUpdateStream()
        .addFieldTransformsItem(new FieldTransform().breaking(true))
        .addStreamAttributeTransformsItem(new StreamAttributeTransform().breaking(false));
    final CatalogDiff catalogDiff2 = new CatalogDiff().transforms(List.of(
        new StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM).updateStream(updateWithBreakingFieldTransform)));

    assertTrue(applySchemaChangeHelper.containsBreakingChange(catalogDiff2));

    final StreamTransformUpdateStream updateWithBreakingAttributeTransform = new StreamTransformUpdateStream()
        .addFieldTransformsItem(new FieldTransform().breaking(false))
        .addStreamAttributeTransformsItem(new StreamAttributeTransform().breaking(true));
    final CatalogDiff catalogDiff3 = new CatalogDiff().transforms(List.of(
        new StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM).updateStream(updateWithBreakingAttributeTransform)));

    assertTrue(applySchemaChangeHelper.containsBreakingChange(catalogDiff3));
  }

  @Nested
  class FieldSelectionInteractions {

    final AirbyteCatalog oldCatalog = new AirbyteCatalog()
        .streams(List.of(
            new AirbyteStreamAndConfiguration()
                .stream(new AirbyteStream()
                    .name("users")
                    .namespace("public"))
                .config(new AirbyteStreamConfiguration()
                    .selected(true)
                    .fieldSelectionEnabled(true)
                    .selectedFields(List.of(
                        new SelectedFieldInfo().fieldPath(List.of("id")),
                        new SelectedFieldInfo().fieldPath(List.of("address")))))));
    final AirbyteCatalog newCatalog = new AirbyteCatalog()
        .streams(List.of(
            new AirbyteStreamAndConfiguration()
                .stream(new AirbyteStream()
                    .name("users")
                    .namespace("public"))
                .config(new AirbyteStreamConfiguration().selected(true).fieldSelectionEnabled(true))));

    private int fieldIsSelected(final AirbyteCatalog catalog, final List<String> path) {
      return catalog.getStreams().get(0).getConfig().getSelectedFields().stream()
          .filter(selected -> selected.equals(new SelectedFieldInfo().fieldPath(path))).toList().size();
    }

    @Test
    void testPropagateChangesDoesNotRemoveAlreadySelectedFields() {

      final List<StreamTransform> transformations = List.of(
          new StreamTransform()
              .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
              .streamDescriptor(new StreamDescriptor().name("users").namespace("public"))
              .updateStream(new StreamTransformUpdateStream().fieldTransforms(
                  List.of(new FieldTransform().transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                      .fieldName(List.of("ssn"))))));
      final ApplySchemaChangeHelper.UpdateSchemaResult result = applySchemaChangeHelper.getUpdatedSchema(oldCatalog, newCatalog,
          transformations, NonBreakingChangesPreference.PROPAGATE_COLUMNS, List.of());
      assertEquals(1, fieldIsSelected(result.catalog(), List.of("id")));
      assertEquals(1, fieldIsSelected(result.catalog(), List.of("address")));
    }

    @Test
    void testPropagateChangesNewFieldIsSelected() {

      final List<StreamTransform> transformations = List.of(
          new StreamTransform()
              .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
              .streamDescriptor(new StreamDescriptor().name("users").namespace("public"))
              .updateStream(new StreamTransformUpdateStream().fieldTransforms(
                  List.of(new FieldTransform().transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                      .fieldName(List.of("ssn"))))));
      final ApplySchemaChangeHelper.UpdateSchemaResult result = applySchemaChangeHelper.getUpdatedSchema(oldCatalog, newCatalog,
          transformations, NonBreakingChangesPreference.PROPAGATE_COLUMNS, List.of());
      assertEquals(1, fieldIsSelected(result.catalog(), List.of("ssn")));
    }

    @Test
    void testNewSubfieldAlreadySelected() {
      final List<StreamTransform> transformations = List.of(
          new StreamTransform()
              .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
              .streamDescriptor(new StreamDescriptor().name("users").namespace("public"))
              .updateStream(new StreamTransformUpdateStream().fieldTransforms(
                  List.of(new FieldTransform().transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                      .fieldName(List.of("address", "zip"))))));
      final ApplySchemaChangeHelper.UpdateSchemaResult result = applySchemaChangeHelper.getUpdatedSchema(oldCatalog, newCatalog,
          transformations, NonBreakingChangesPreference.PROPAGATE_COLUMNS, List.of());
      assertEquals(1, fieldIsSelected(result.catalog(), List.of("address")));
    }

    @Test
    void testNewSubfieldNotSelected() {
      final List<StreamTransform> transformations = List.of(
          new StreamTransform()
              .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
              .streamDescriptor(new StreamDescriptor().name("users").namespace("public"))
              .updateStream(new StreamTransformUpdateStream().fieldTransforms(
                  List.of(new FieldTransform().transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                      .fieldName(List.of("username", "domain"))))));
      final ApplySchemaChangeHelper.UpdateSchemaResult result = applySchemaChangeHelper.getUpdatedSchema(oldCatalog, newCatalog,
          transformations, NonBreakingChangesPreference.PROPAGATE_COLUMNS, List.of());
      assertEquals(0, fieldIsSelected(result.catalog(), List.of("username")));
      assertEquals(0, fieldIsSelected(result.catalog(), List.of("username", "domain")));
    }

    @Test
    void testNewFieldAndSubField() {
      final List<StreamTransform> transformations = List.of(
          new StreamTransform()
              .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
              .streamDescriptor(new StreamDescriptor().name("users").namespace("public"))
              .updateStream(new StreamTransformUpdateStream().fieldTransforms(
                  List.of(new FieldTransform().transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                      .fieldName(List.of("username", "domain")),
                      new FieldTransform().transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                          .fieldName(List.of("username"))))));
      final ApplySchemaChangeHelper.UpdateSchemaResult result = applySchemaChangeHelper.getUpdatedSchema(oldCatalog, newCatalog,
          transformations, NonBreakingChangesPreference.PROPAGATE_COLUMNS, List.of());
      assertEquals(1, fieldIsSelected(result.catalog(), List.of("username")));
      assertEquals(0, fieldIsSelected(result.catalog(), List.of("username", "domain")));

    }

  }

}
