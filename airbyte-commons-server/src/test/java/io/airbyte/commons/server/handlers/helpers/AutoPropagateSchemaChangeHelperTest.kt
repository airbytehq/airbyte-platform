/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStream
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.FieldTransform
import io.airbyte.api.model.generated.NonBreakingChangesPreference
import io.airbyte.api.model.generated.SelectedFieldInfo
import io.airbyte.api.model.generated.StreamAttributeTransform
import io.airbyte.api.model.generated.StreamDescriptor
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.api.model.generated.StreamTransformUpdateStream
import io.airbyte.api.model.generated.SyncMode
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.config.MapperConfig
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.mappers.transformations.Mapper
import jakarta.validation.Valid
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.whenever
import java.util.List

internal class AutoPropagateSchemaChangeHelperTest {
  private val applySchemaChangeHelper = ApplySchemaChangeHelper(CatalogConverter(FieldGenerator(), mutableListOf<Mapper<out MapperConfig>>()))

  private lateinit var featureFlagClient: FeatureFlagClient

  @BeforeEach
  fun beforeEach() {
    featureFlagClient = Mockito.mock(TestClient::class.java)
    whenever(featureFlagClient.boolVariation(anyOrNull(), anyOrNull()))
      .thenReturn(false)
  }

  @Test
  fun extractStreamAndConfigPerStreamDescriptorTest() {
    val airbyteCatalog = AirbyteCatalog()
    val airbyteStreamConfiguration1 =
      AirbyteStreamAndConfiguration()
        .stream(AirbyteStream().name(NAME1).namespace(NAMESPACE1).addSupportedSyncModesItem(SyncMode.FULL_REFRESH))
    val airbyteStreamConfiguration2 =
      AirbyteStreamAndConfiguration()
        .stream(AirbyteStream().name(NAME2).namespace(NAMESPACE2).addSupportedSyncModesItem(SyncMode.INCREMENTAL))
    airbyteCatalog.streams(listOf<AirbyteStreamAndConfiguration>(airbyteStreamConfiguration1, airbyteStreamConfiguration2))

    val result =
      applySchemaChangeHelper.extractStreamAndConfigPerStreamDescriptor(airbyteCatalog)

    Assertions.assertThat(result).hasSize(2)
    Assertions.assertThat(result).isEqualTo(
      mapOf(
        StreamDescriptor().name(NAME1).namespace(NAMESPACE1) to airbyteStreamConfiguration1,
        StreamDescriptor().name(NAME2).namespace(NAMESPACE2) to airbyteStreamConfiguration2,
      ),
    )
  }

  @Test
  fun extractStreamAndConfigPerStreamDescriptorNoNamespaceTest() {
    val airbyteCatalog = AirbyteCatalog()
    val airbyteStreamConfiguration1 =
      AirbyteStreamAndConfiguration()
        .stream(AirbyteStream().name(NAME1).addSupportedSyncModesItem(SyncMode.FULL_REFRESH))
    val airbyteStreamConfiguration2 =
      AirbyteStreamAndConfiguration()
        .stream(AirbyteStream().name(NAME2).addSupportedSyncModesItem(SyncMode.INCREMENTAL))
    airbyteCatalog.streams(listOf<AirbyteStreamAndConfiguration>(airbyteStreamConfiguration1, airbyteStreamConfiguration2))

    val result =
      applySchemaChangeHelper.extractStreamAndConfigPerStreamDescriptor(airbyteCatalog)

    Assertions.assertThat(result).hasSize(2)
    Assertions.assertThat(result).isEqualTo(
      mapOf(
        StreamDescriptor().name(NAME1) to airbyteStreamConfiguration1,
        StreamDescriptor().name(NAME2) to airbyteStreamConfiguration2,
      ),
    )
  }

  @Test
  fun applyUpdate() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newSchema = deserialize(NEW_SCHEMA)
    val newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, newSchema)

    val transform =
      StreamTransform()
        .streamDescriptor(StreamDescriptor().name(NAME1))
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat(result.getStreams()).hasSize(1)
    Assertions.assertThat(result.getStreams()[0].getStream().getJsonSchema()).isEqualTo(newSchema)
  }

  @Test
  fun applyAddNoFlag() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newSchema = deserialize(NEW_SCHEMA)
    val newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema)

    val transform =
      StreamTransform()
        .streamDescriptor(StreamDescriptor().name(NAME2))
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat(result.getStreams()).hasSize(2)
    Assertions.assertThat(result.getStreams()[0].getStream().getName()).isEqualTo(NAME1)
    Assertions.assertThat(result.getStreams()[0].getStream().getJsonSchema()).isEqualTo(oldSchema)
    Assertions.assertThat(result.getStreams()[0].getConfig().getSelected()).isTrue()
    Assertions.assertThat(result.getStreams()[1].getStream().getName()).isEqualTo(NAME2)
    Assertions.assertThat(result.getStreams()[1].getStream().getJsonSchema()).isEqualTo(newSchema)
  }

  @Test
  fun applyAdd() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newSchema = deserialize(NEW_SCHEMA)
    val newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema)

    val transform =
      StreamTransform()
        .streamDescriptor(StreamDescriptor().name(NAME2))
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat<@Valid AirbyteStreamAndConfiguration?>(result.getStreams()).hasSize(2)
    val stream0 = result.getStreams()[0]
    val stream1 = result.getStreams()[1]
    Assertions.assertThat(stream0.getStream().getName()).isEqualTo(NAME1)
    Assertions.assertThat(stream0.getStream().getJsonSchema()).isEqualTo(oldSchema)
    Assertions.assertThat(stream0.getConfig().getSelected()).isTrue()
    Assertions.assertThat(stream1.getStream().getName()).isEqualTo(NAME2)
    Assertions.assertThat(stream1.getStream().getJsonSchema()).isEqualTo(newSchema)
    Assertions.assertThat(stream1.getConfig().getSelected()).isTrue()
    Assertions.assertThat(stream1.getConfig().getSyncMode()).isEqualTo(SyncMode.FULL_REFRESH)
    Assertions.assertThat(stream1.getConfig().getDestinationSyncMode()).isEqualTo(DestinationSyncMode.OVERWRITE)
  }

  @Test
  fun applyAddWithSourceDefinedCursor() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newSchema = deserialize(NEW_SCHEMA)
    val newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema)
    newAirbyteCatalog.getStreams()[0].getStream().sourceDefinedCursor(true).sourceDefinedPrimaryKey(
      listOf(
        mutableListOf("test"),
      ),
    )

    val transform =
      StreamTransform()
        .streamDescriptor(StreamDescriptor().name(NAME2))
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat<@Valid AirbyteStreamAndConfiguration?>(result.getStreams()).hasSize(2)
    val stream0 = result.getStreams()[0]
    val stream1 = result.getStreams()[1]
    Assertions.assertThat(stream0.getStream().getName()).isEqualTo(NAME1)
    Assertions.assertThat(stream0.getStream().getJsonSchema()).isEqualTo(oldSchema)
    Assertions.assertThat(stream0.getConfig().getSelected()).isTrue()
    Assertions.assertThat(stream1.getStream().getName()).isEqualTo(NAME2)
    Assertions.assertThat(stream1.getStream().getJsonSchema()).isEqualTo(newSchema)
    Assertions.assertThat(stream1.getConfig().getSelected()).isTrue()
    Assertions.assertThat(stream1.getConfig().getSyncMode()).isEqualTo(SyncMode.INCREMENTAL)
    Assertions.assertThat(stream1.getConfig().getDestinationSyncMode()).isEqualTo(DestinationSyncMode.APPEND_DEDUP)
  }

  @Test
  fun applyAddWithSourceDefinedCursorNoPrimaryKey() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newSchema = deserialize(NEW_SCHEMA)
    val newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema)
    newAirbyteCatalog.getStreams()[0].getStream().sourceDefinedCursor(true)

    val transform =
      StreamTransform()
        .streamDescriptor(StreamDescriptor().name(NAME2))
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat<@Valid AirbyteStreamAndConfiguration?>(result.getStreams()).hasSize(2)
    val stream1 = result.getStreams()[1]
    Assertions.assertThat(stream1.getConfig().getSyncMode()).isEqualTo(SyncMode.FULL_REFRESH)
    Assertions.assertThat(stream1.getConfig().getDestinationSyncMode()).isEqualTo(DestinationSyncMode.OVERWRITE)
  }

  @Test
  fun applyAddWithSourceDefinedCursorNoPrimaryKeyNoFullRefresh() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newSchema = deserialize(NEW_SCHEMA)
    val newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema)
    newAirbyteCatalog
      .getStreams()[0]
      .getStream()
      .sourceDefinedCursor(true)
      .supportedSyncModes(listOf(SyncMode.INCREMENTAL))

    val transform =
      StreamTransform()
        .streamDescriptor(StreamDescriptor().name(NAME2))
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat<@Valid AirbyteStreamAndConfiguration?>(result.getStreams()).hasSize(2)
    val stream1 = result.getStreams()[1]
    Assertions.assertThat(stream1.getConfig().getSyncMode()).isEqualTo(SyncMode.INCREMENTAL)
    Assertions.assertThat(stream1.getConfig().getDestinationSyncMode()).isEqualTo(DestinationSyncMode.APPEND)
  }

  @Test
  fun applyRemove() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newAirbyteCatalog = AirbyteCatalog()

    val transform =
      StreamTransform()
        .streamDescriptor(StreamDescriptor().name(NAME1))
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat<@Valid AirbyteStreamAndConfiguration?>(result.getStreams()).hasSize(0)
  }

  @Test
  fun applyAddNotFully() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newSchema = deserialize(NEW_SCHEMA)
    val newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema)

    val transform =
      StreamTransform()
        .streamDescriptor(StreamDescriptor().name(NAME2))
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_COLUMNS,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat<@Valid AirbyteStreamAndConfiguration?>(result.getStreams()).hasSize(1)
    Assertions.assertThat(result.getStreams()[0].getStream().getName()).isEqualTo(NAME1)
    Assertions.assertThat(result.getStreams()[0].getStream().getJsonSchema()).isEqualTo(oldSchema)
  }

  @Test
  fun applyRemoveNotFully() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newAirbyteCatalog = AirbyteCatalog()

    val transform =
      StreamTransform()
        .streamDescriptor(StreamDescriptor().name(NAME1))
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_COLUMNS,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat<@Valid AirbyteStreamAndConfiguration?>(result.getStreams()).hasSize(1)
    Assertions.assertThat(result.getStreams()[0].getStream().getName()).isEqualTo(NAME1)
    Assertions.assertThat(result.getStreams()[0].getStream().getJsonSchema()).isEqualTo(oldSchema)
  }

  @Test
  fun addStreamFormat() {
    val transform =
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
        .streamDescriptor(StreamDescriptor().name("foo").namespace("bar"))
    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transform)).isEqualTo("Added new stream 'bar.foo'")

    val transformNoNS =
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
        .streamDescriptor(StreamDescriptor().name("foo"))
    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transformNoNS)).isEqualTo("Added new stream 'foo'")
  }

  @Test
  fun removeStreamFormat() {
    val transform =
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(StreamDescriptor().name("foo").namespace("bar"))
    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transform)).isEqualTo("Removed stream 'bar.foo'")

    val transformNoNS =
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
        .streamDescriptor(StreamDescriptor().name("foo"))
    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transformNoNS)).isEqualTo("Removed stream 'foo'")
  }

  @Test
  fun newColumnInStreamFormat() {
    val transform =
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(StreamDescriptor().name("foo").namespace("bar"))
        .updateStream(
          StreamTransformUpdateStream().fieldTransforms(
            listOf(
              FieldTransform()
                .fieldName(mutableListOf("path", "new_field"))
                .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD),
              FieldTransform()
                .fieldName(mutableListOf("path", "other_field"))
                .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD),
            ),
          ),
        )

    Assertions
      .assertThat(applySchemaChangeHelper.formatDiff(transform))
      .isEqualTo("Modified stream 'bar.foo': Added fields ['path.new_field', 'path.other_field']")
  }

  @Test
  fun updatedColumnInStreamFormat() {
    val transform =
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(StreamDescriptor().name("foo").namespace("bar"))
        .updateStream(
          StreamTransformUpdateStream().fieldTransforms(
            listOf(
              FieldTransform()
                .fieldName(mutableListOf("path", "new_field"))
                .transformType(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA),
              FieldTransform()
                .fieldName(mutableListOf("path", "other_field"))
                .transformType(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA),
            ),
          ),
        )

    Assertions
      .assertThat(applySchemaChangeHelper.formatDiff(transform))
      .isEqualTo("Modified stream 'bar.foo': Altered fields ['path.new_field', 'path.other_field']")
  }

  @Test
  fun removedColumnsInStreamFormat() {
    val transform =
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(StreamDescriptor().name("foo").namespace("bar"))
        .updateStream(
          StreamTransformUpdateStream().fieldTransforms(
            listOf(
              FieldTransform()
                .fieldName(mutableListOf("path", "new_field"))
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD),
              FieldTransform()
                .fieldName(mutableListOf("other_field"))
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD),
            ),
          ),
        )

    Assertions
      .assertThat(applySchemaChangeHelper.formatDiff(transform))
      .isEqualTo("Modified stream 'bar.foo': Removed fields ['path.new_field', 'other_field']")
  }

  private fun createAirbyteCatalogWithSchema(
    name: String?,
    schema: JsonNode?,
  ): AirbyteCatalog {
    val airbyteCatalog = AirbyteCatalog()

    val airbyteStreamConfiguration1 =
      AirbyteStreamAndConfiguration()
        .stream(AirbyteStream().name(name).jsonSchema(schema).supportedSyncModes(listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL)))
        .config(AirbyteStreamConfiguration().selected(true))

    airbyteCatalog.streams(listOf<AirbyteStreamAndConfiguration>(airbyteStreamConfiguration1))

    return airbyteCatalog
  }

  @Test
  fun mixedChangesInStreamFormat() {
    val transform =
      StreamTransform()
        .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
        .streamDescriptor(StreamDescriptor().name("foo").namespace("bar"))
        .updateStream(
          StreamTransformUpdateStream().fieldTransforms(
            listOf(
              FieldTransform()
                .fieldName(mutableListOf("path", "new_field"))
                .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD),
              FieldTransform()
                .fieldName(mutableListOf("old_field"))
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD),
              FieldTransform()
                .fieldName(mutableListOf("old_path", "deprecated"))
                .transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD),
              FieldTransform()
                .fieldName(mutableListOf("properties", "changed_type"))
                .transformType(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA),
            ),
          ),
        )

    Assertions
      .assertThat(applySchemaChangeHelper.formatDiff(transform))
      .isEqualTo(
        "Modified stream 'bar.foo': Added fields ['path.new_field'], Removed fields ['old_field', 'old_path.deprecated'], Altered fields ['properties.changed_type']",
      )
  }

  @Test
  fun emptyDiffShouldAlwaysPropagate() {
    Assertions
      .assertThat(
        applySchemaChangeHelper.shouldAutoPropagate(
          CatalogDiff(),
          ConnectionRead().nonBreakingChangesPreference(NonBreakingChangesPreference.IGNORE),
        ),
      ).isTrue()
  }

  @Test
  fun emptyDiffCanBeApplied() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val result =
      applySchemaChangeHelper.getUpdatedSchema(
        oldAirbyteCatalog,
        oldAirbyteCatalog,
        mutableListOf(),
        NonBreakingChangesPreference.PROPAGATE_FULLY,
        SUPPORTED_DESTINATION_SYNC_MODES,
      )

    Assertions.assertThat(result.catalog).isEqualTo(oldAirbyteCatalog)
    Assertions.assertThat<@Valid StreamTransform?>(result.appliedDiff.getTransforms()).isEmpty()
    Assertions.assertThat(result.changeDescription).isEmpty()
  }

  @Test
  fun testContainsBreakingChange() {
    val updateWithNoBreakingTransforms =
      StreamTransformUpdateStream()
        .addFieldTransformsItem(FieldTransform().breaking(false))
        .addStreamAttributeTransformsItem(StreamAttributeTransform().breaking(false))
    val catalogDiff1 =
      CatalogDiff().transforms(
        listOf(
          StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM).updateStream(updateWithNoBreakingTransforms),
        ),
      )

    org.junit.jupiter.api.Assertions
      .assertFalse(applySchemaChangeHelper.containsBreakingChange(catalogDiff1))

    val updateWithBreakingFieldTransform =
      StreamTransformUpdateStream()
        .addFieldTransformsItem(FieldTransform().breaking(true))
        .addStreamAttributeTransformsItem(StreamAttributeTransform().breaking(false))
    val catalogDiff2 =
      CatalogDiff().transforms(
        listOf(
          StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM).updateStream(updateWithBreakingFieldTransform),
        ),
      )

    org.junit.jupiter.api.Assertions
      .assertTrue(applySchemaChangeHelper.containsBreakingChange(catalogDiff2))

    val updateWithBreakingAttributeTransform =
      StreamTransformUpdateStream()
        .addFieldTransformsItem(FieldTransform().breaking(false))
        .addStreamAttributeTransformsItem(StreamAttributeTransform().breaking(true))
    val catalogDiff3 =
      CatalogDiff().transforms(
        listOf(
          StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM).updateStream(updateWithBreakingAttributeTransform),
        ),
      )

    org.junit.jupiter.api.Assertions
      .assertTrue(applySchemaChangeHelper.containsBreakingChange(catalogDiff3))
  }

  @Nested
  internal inner class FieldSelectionInteractions {
    val oldCatalog: AirbyteCatalog =
      AirbyteCatalog()
        .streams(
          listOf(
            AirbyteStreamAndConfiguration()
              .stream(
                AirbyteStream()
                  .name("users")
                  .namespace("public"),
              ).config(
                AirbyteStreamConfiguration()
                  .selected(true)
                  .fieldSelectionEnabled(true)
                  .selectedFields(
                    List.of<@Valid SelectedFieldInfo?>(
                      SelectedFieldInfo().fieldPath(mutableListOf("id")),
                      SelectedFieldInfo().fieldPath(mutableListOf("address")),
                    ),
                  ),
              ),
          ),
        )
    val newCatalog: AirbyteCatalog =
      AirbyteCatalog()
        .streams(
          listOf(
            AirbyteStreamAndConfiguration()
              .stream(
                AirbyteStream()
                  .name("users")
                  .namespace("public"),
              ).config(AirbyteStreamConfiguration().selected(true).fieldSelectionEnabled(true)),
          ),
        )

    private fun fieldIsSelected(
      catalog: AirbyteCatalog,
      path: MutableList<String?>?,
    ): Int =
      catalog
        .getStreams()[0]
        .getConfig()
        .getSelectedFields()
        .stream()
        .filter { selected: SelectedFieldInfo? -> selected == SelectedFieldInfo().fieldPath(path) }
        .toList()
        .size

    @Test
    fun testPropagateChangesDoesNotRemoveAlreadySelectedFields() {
      val transformations =
        listOf(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .streamDescriptor(StreamDescriptor().name("users").namespace("public"))
            .updateStream(
              StreamTransformUpdateStream().fieldTransforms(
                listOf(
                  FieldTransform()
                    .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                    .fieldName(mutableListOf("ssn")),
                ),
              ),
            ),
        )
      val result =
        applySchemaChangeHelper.getUpdatedSchema(
          oldCatalog,
          newCatalog,
          transformations,
          NonBreakingChangesPreference.PROPAGATE_COLUMNS,
          mutableListOf(),
        )
      org.junit.jupiter.api.Assertions
        .assertEquals(1, fieldIsSelected(result.catalog, mutableListOf("id")))
      org.junit.jupiter.api.Assertions
        .assertEquals(1, fieldIsSelected(result.catalog, mutableListOf("address")))
    }

    @Test
    fun testPropagateChangesNewFieldIsSelected() {
      val transformations =
        listOf(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .streamDescriptor(StreamDescriptor().name("users").namespace("public"))
            .updateStream(
              StreamTransformUpdateStream().fieldTransforms(
                listOf(
                  FieldTransform()
                    .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                    .fieldName(mutableListOf("ssn")),
                ),
              ),
            ),
        )
      val result =
        applySchemaChangeHelper.getUpdatedSchema(
          oldCatalog,
          newCatalog,
          transformations,
          NonBreakingChangesPreference.PROPAGATE_COLUMNS,
          mutableListOf(),
        )
      org.junit.jupiter.api.Assertions
        .assertEquals(1, fieldIsSelected(result.catalog, mutableListOf("ssn")))
    }

    @Test
    fun testNewSubfieldAlreadySelected() {
      val transformations =
        listOf(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .streamDescriptor(StreamDescriptor().name("users").namespace("public"))
            .updateStream(
              StreamTransformUpdateStream().fieldTransforms(
                listOf(
                  FieldTransform()
                    .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                    .fieldName(mutableListOf("address", "zip")),
                ),
              ),
            ),
        )
      val result =
        applySchemaChangeHelper.getUpdatedSchema(
          oldCatalog,
          newCatalog,
          transformations,
          NonBreakingChangesPreference.PROPAGATE_COLUMNS,
          mutableListOf(),
        )
      org.junit.jupiter.api.Assertions
        .assertEquals(1, fieldIsSelected(result.catalog, mutableListOf("address")))
    }

    @Test
    fun testNewSubfieldNotSelected() {
      val transformations =
        listOf(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .streamDescriptor(StreamDescriptor().name("users").namespace("public"))
            .updateStream(
              StreamTransformUpdateStream().fieldTransforms(
                listOf(
                  FieldTransform()
                    .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                    .fieldName(mutableListOf("username", "domain")),
                ),
              ),
            ),
        )
      val result =
        applySchemaChangeHelper.getUpdatedSchema(
          oldCatalog,
          newCatalog,
          transformations,
          NonBreakingChangesPreference.PROPAGATE_COLUMNS,
          mutableListOf(),
        )
      org.junit.jupiter.api.Assertions
        .assertEquals(0, fieldIsSelected(result.catalog, mutableListOf("username")))
      org.junit.jupiter.api.Assertions
        .assertEquals(0, fieldIsSelected(result.catalog, mutableListOf("username", "domain")))
    }

    @Test
    fun testNewFieldAndSubField() {
      val transformations =
        listOf(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .streamDescriptor(StreamDescriptor().name("users").namespace("public"))
            .updateStream(
              StreamTransformUpdateStream().fieldTransforms(
                listOf(
                  FieldTransform()
                    .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                    .fieldName(mutableListOf("username", "domain")),
                  FieldTransform()
                    .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                    .fieldName(mutableListOf("username")),
                ),
              ),
            ),
        )
      val result =
        applySchemaChangeHelper.getUpdatedSchema(
          oldCatalog,
          newCatalog,
          transformations,
          NonBreakingChangesPreference.PROPAGATE_COLUMNS,
          mutableListOf(),
        )
      org.junit.jupiter.api.Assertions
        .assertEquals(1, fieldIsSelected(result.catalog, mutableListOf("username")))
      org.junit.jupiter.api.Assertions
        .assertEquals(0, fieldIsSelected(result.catalog, mutableListOf("username", "domain")))
    }
  }

  companion object {
    private const val NAME1 = "name1"
    private const val NAMESPACE1 = "namespace1"
    private const val NAME2 = "name2"
    private const val NAMESPACE2 = "namespace2"
    private val OLD_SCHEMA =
      """
      {
        "schema": "old"
      }
      
      """.trimIndent()
    private val NEW_SCHEMA =
      """
      {
        "schema": "old"
      }
      
      """.trimIndent()

    private val SUPPORTED_DESTINATION_SYNC_MODES: MutableList<DestinationSyncMode> =
      mutableListOf(
        DestinationSyncMode.OVERWRITE,
        DestinationSyncMode.APPEND,
        DestinationSyncMode.APPEND_DEDUP,
      )
  }
}
