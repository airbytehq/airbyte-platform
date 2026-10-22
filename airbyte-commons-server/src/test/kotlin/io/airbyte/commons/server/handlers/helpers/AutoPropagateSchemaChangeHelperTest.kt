/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.AirbyteStream
import io.airbyte.api.server.generated.models.AirbyteStreamAndConfiguration
import io.airbyte.api.server.generated.models.AirbyteStreamConfiguration
import io.airbyte.api.server.generated.models.CatalogDiff
import io.airbyte.api.server.generated.models.ConnectionRead
import io.airbyte.api.server.generated.models.ConnectionStatus
import io.airbyte.api.server.generated.models.DestinationSyncMode
import io.airbyte.api.server.generated.models.FieldTransform
import io.airbyte.api.server.generated.models.NonBreakingChangesPreference
import io.airbyte.api.server.generated.models.SelectedFieldInfo
import io.airbyte.api.server.generated.models.StreamAttributeTransform
import io.airbyte.api.server.generated.models.StreamDescriptor
import io.airbyte.api.server.generated.models.StreamTransform
import io.airbyte.api.server.generated.models.StreamTransformUpdateStream
import io.airbyte.api.server.generated.models.SyncMode
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.config.MapperConfig
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.mappers.transformations.Mapper
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.whenever
import java.util.UUID

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
    val airbyteStreamConfiguration1 =
      AirbyteStreamAndConfiguration(
        stream = AirbyteStream(name = NAME1, namespace = NAMESPACE1, supportedSyncModes = listOf(SyncMode.FULL_REFRESH)),
      )
    val airbyteStreamConfiguration2 =
      AirbyteStreamAndConfiguration(
        stream = AirbyteStream(name = NAME2, namespace = NAMESPACE2, supportedSyncModes = listOf(SyncMode.INCREMENTAL)),
      )
    val airbyteCatalog = AirbyteCatalog(streams = listOf(airbyteStreamConfiguration1, airbyteStreamConfiguration2))

    val result =
      applySchemaChangeHelper.extractStreamAndConfigPerStreamDescriptor(airbyteCatalog)

    Assertions.assertThat(result).hasSize(2)
    Assertions.assertThat(result).isEqualTo(
      mapOf(
        StreamDescriptor(name = NAME1, namespace = NAMESPACE1) to airbyteStreamConfiguration1,
        StreamDescriptor(name = NAME2, namespace = NAMESPACE2) to airbyteStreamConfiguration2,
      ),
    )
  }

  @Test
  fun extractStreamAndConfigPerStreamDescriptorNoNamespaceTest() {
    val airbyteStreamConfiguration1 =
      AirbyteStreamAndConfiguration(
        stream = AirbyteStream(name = NAME1, supportedSyncModes = listOf(SyncMode.FULL_REFRESH)),
      )
    val airbyteStreamConfiguration2 =
      AirbyteStreamAndConfiguration(
        stream = AirbyteStream(name = NAME2, supportedSyncModes = listOf(SyncMode.INCREMENTAL)),
      )
    val airbyteCatalog = AirbyteCatalog(streams = listOf(airbyteStreamConfiguration1, airbyteStreamConfiguration2))

    val result =
      applySchemaChangeHelper.extractStreamAndConfigPerStreamDescriptor(airbyteCatalog)

    Assertions.assertThat(result).hasSize(2)
    Assertions.assertThat(result).isEqualTo(
      mapOf(
        StreamDescriptor(name = NAME1) to airbyteStreamConfiguration1,
        StreamDescriptor(name = NAME2) to airbyteStreamConfiguration2,
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
      StreamTransform(
        streamDescriptor = StreamDescriptor(name = NAME1),
        transformType = StreamTransform.TransformType.UPDATE_STREAM,
      )

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat(result.streams).hasSize(1)
    Assertions.assertThat(result.streams[0].stream!!.jsonSchema).isEqualTo(newSchema)
  }

  @Test
  fun applyAddNoFlag() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newSchema = deserialize(NEW_SCHEMA)
    val newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema)

    val transform =
      StreamTransform(
        streamDescriptor = StreamDescriptor(name = NAME2),
        transformType = StreamTransform.TransformType.ADD_STREAM,
      )

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat(result.streams).hasSize(2)
    Assertions.assertThat(result.streams[0].stream!!.name).isEqualTo(NAME1)
    Assertions.assertThat(result.streams[0].stream!!.jsonSchema).isEqualTo(oldSchema)
    Assertions.assertThat(result.streams[0].config!!.selected).isTrue()
    Assertions.assertThat(result.streams[1].stream!!.name).isEqualTo(NAME2)
    Assertions.assertThat(result.streams[1].stream!!.jsonSchema).isEqualTo(newSchema)
  }

  @Test
  fun applyAdd() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newSchema = deserialize(NEW_SCHEMA)
    val newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema)

    val transform =
      StreamTransform(
        streamDescriptor = StreamDescriptor(name = NAME2),
        transformType = StreamTransform.TransformType.ADD_STREAM,
      )

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat(result.streams).hasSize(2)
    val stream0 = result.streams[0]
    val stream1 = result.streams[1]
    Assertions.assertThat(stream0.stream!!.name).isEqualTo(NAME1)
    Assertions.assertThat(stream0.stream!!.jsonSchema).isEqualTo(oldSchema)
    Assertions.assertThat(stream0.config!!.selected).isTrue()
    Assertions.assertThat(stream1.stream!!.name).isEqualTo(NAME2)
    Assertions.assertThat(stream1.stream!!.jsonSchema).isEqualTo(newSchema)
    Assertions.assertThat(stream1.config!!.selected).isTrue()
    Assertions.assertThat(stream1.config!!.syncMode).isEqualTo(SyncMode.FULL_REFRESH)
    Assertions.assertThat(stream1.config!!.destinationSyncMode).isEqualTo(DestinationSyncMode.OVERWRITE)
  }

  @Test
  fun applyAddWithSourceDefinedCursor() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newSchema = deserialize(NEW_SCHEMA)
    val newAirbyteCatalog =
      createAirbyteCatalogWithSchema(NAME2, newSchema) { it.copy(sourceDefinedCursor = true, sourceDefinedPrimaryKey = listOf(listOf("test"))) }

    val transform =
      StreamTransform(
        streamDescriptor = StreamDescriptor(name = NAME2),
        transformType = StreamTransform.TransformType.ADD_STREAM,
      )

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat(result.streams).hasSize(2)
    val stream0 = result.streams[0]
    val stream1 = result.streams[1]
    Assertions.assertThat(stream0.stream!!.name).isEqualTo(NAME1)
    Assertions.assertThat(stream0.stream!!.jsonSchema).isEqualTo(oldSchema)
    Assertions.assertThat(stream0.config!!.selected).isTrue()
    Assertions.assertThat(stream1.stream!!.name).isEqualTo(NAME2)
    Assertions.assertThat(stream1.stream!!.jsonSchema).isEqualTo(newSchema)
    Assertions.assertThat(stream1.config!!.selected).isTrue()
    Assertions.assertThat(stream1.config!!.syncMode).isEqualTo(SyncMode.INCREMENTAL)
    Assertions.assertThat(stream1.config!!.destinationSyncMode).isEqualTo(DestinationSyncMode.APPEND_DEDUP)
  }

  @Test
  fun applyAddWithSourceDefinedCursorNoPrimaryKey() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newSchema = deserialize(NEW_SCHEMA)
    val newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema) { it.copy(sourceDefinedCursor = true) }

    val transform =
      StreamTransform(
        streamDescriptor = StreamDescriptor(name = NAME2),
        transformType = StreamTransform.TransformType.ADD_STREAM,
      )

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat(result.streams).hasSize(2)
    val stream1 = result.streams[1]
    Assertions.assertThat(stream1.config!!.syncMode).isEqualTo(SyncMode.FULL_REFRESH)
    Assertions.assertThat(stream1.config!!.destinationSyncMode).isEqualTo(DestinationSyncMode.OVERWRITE)
  }

  @Test
  fun applyAddWithSourceDefinedCursorNoPrimaryKeyNoFullRefresh() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newSchema = deserialize(NEW_SCHEMA)
    val newAirbyteCatalog =
      createAirbyteCatalogWithSchema(NAME2, newSchema) { it.copy(sourceDefinedCursor = true, supportedSyncModes = listOf(SyncMode.INCREMENTAL)) }

    val transform =
      StreamTransform(
        streamDescriptor = StreamDescriptor(name = NAME2),
        transformType = StreamTransform.TransformType.ADD_STREAM,
      )

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat(result.streams).hasSize(2)
    val stream1 = result.streams[1]
    Assertions.assertThat(stream1.config!!.syncMode).isEqualTo(SyncMode.INCREMENTAL)
    Assertions.assertThat(stream1.config!!.destinationSyncMode).isEqualTo(DestinationSyncMode.APPEND)
  }

  @Test
  fun applyRemove() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newAirbyteCatalog = AirbyteCatalog(streams = emptyList())

    val transform =
      StreamTransform(
        streamDescriptor = StreamDescriptor(name = NAME1),
        transformType = StreamTransform.TransformType.REMOVE_STREAM,
      )

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_FULLY,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat(result.streams).hasSize(0)
  }

  @Test
  fun applyAddNotFully() {
    val oldSchema = deserialize(OLD_SCHEMA)
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, oldSchema)

    val newSchema = deserialize(NEW_SCHEMA)
    val newAirbyteCatalog = createAirbyteCatalogWithSchema(NAME2, newSchema)

    val transform =
      StreamTransform(
        streamDescriptor = StreamDescriptor(name = NAME2),
        transformType = StreamTransform.TransformType.ADD_STREAM,
      )

    val result =
      applySchemaChangeHelper
        .getUpdatedSchema(
          oldAirbyteCatalog,
          newAirbyteCatalog,
          listOf(transform),
          NonBreakingChangesPreference.PROPAGATE_COLUMNS,
          SUPPORTED_DESTINATION_SYNC_MODES,
        ).catalog

    Assertions.assertThat(result.streams).hasSize(1)
    Assertions.assertThat(result.streams[0].stream!!.name).isEqualTo(NAME1)
    Assertions.assertThat(result.streams[0].stream!!.jsonSchema).isEqualTo(oldSchema)
  }

  @Test
  fun applyRemoveNotFully() {
    val oldAirbyteCatalog = createAirbyteCatalogWithSchema(NAME1, deserialize(OLD_SCHEMA))

    val newAirbyteCatalog = AirbyteCatalog(streams = emptyList())

    val transform =
      StreamTransform(
        streamDescriptor = StreamDescriptor(name = NAME1),
        transformType = StreamTransform.TransformType.REMOVE_STREAM,
      )

    val result =
      applySchemaChangeHelper.getUpdatedSchema(
        oldAirbyteCatalog,
        newAirbyteCatalog,
        listOf(transform),
        NonBreakingChangesPreference.PROPAGATE_COLUMNS,
        SUPPORTED_DESTINATION_SYNC_MODES,
      )

    Assertions.assertThat(result.catalog.streams).isEmpty()
    Assertions.assertThat(result.appliedDiff.transforms).containsExactly(transform)
    Assertions.assertThat(result.changeDescription).containsExactly("Removed stream '$NAME1'")
  }

  @Test
  fun addStreamFormat() {
    val transform =
      StreamTransform(
        transformType = StreamTransform.TransformType.ADD_STREAM,
        streamDescriptor = StreamDescriptor(name = "foo", namespace = "bar"),
      )
    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transform)).isEqualTo("Added new stream 'bar.foo'")

    val transformNoNS =
      StreamTransform(
        transformType = StreamTransform.TransformType.ADD_STREAM,
        streamDescriptor = StreamDescriptor(name = "foo"),
      )
    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transformNoNS)).isEqualTo("Added new stream 'foo'")
  }

  @Test
  fun removeStreamFormat() {
    val transform =
      StreamTransform(
        transformType = StreamTransform.TransformType.REMOVE_STREAM,
        streamDescriptor = StreamDescriptor(name = "foo", namespace = "bar"),
      )
    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transform)).isEqualTo("Removed stream 'bar.foo'")

    val transformNoNS =
      StreamTransform(
        transformType = StreamTransform.TransformType.REMOVE_STREAM,
        streamDescriptor = StreamDescriptor(name = "foo"),
      )
    Assertions.assertThat(applySchemaChangeHelper.formatDiff(transformNoNS)).isEqualTo("Removed stream 'foo'")
  }

  @Test
  fun newColumnInStreamFormat() {
    val transform =
      StreamTransform(
        transformType = StreamTransform.TransformType.UPDATE_STREAM,
        streamDescriptor = StreamDescriptor(name = "foo", namespace = "bar"),
        updateStream =
          StreamTransformUpdateStream(
            fieldTransforms =
              listOf(
                FieldTransform(
                  fieldName = mutableListOf("path", "new_field"),
                  transformType = FieldTransform.TransformType.ADD_FIELD,
                  breaking = false,
                ),
                FieldTransform(
                  fieldName = mutableListOf("path", "other_field"),
                  transformType = FieldTransform.TransformType.ADD_FIELD,
                  breaking = false,
                ),
              ),
            streamAttributeTransforms = emptyList(),
          ),
      )

    Assertions
      .assertThat(applySchemaChangeHelper.formatDiff(transform))
      .isEqualTo("Modified stream 'bar.foo': Added fields ['path.new_field', 'path.other_field']")
  }

  @Test
  fun updatedColumnInStreamFormat() {
    val transform =
      StreamTransform(
        transformType = StreamTransform.TransformType.UPDATE_STREAM,
        streamDescriptor = StreamDescriptor(name = "foo", namespace = "bar"),
        updateStream =
          StreamTransformUpdateStream(
            fieldTransforms =
              listOf(
                FieldTransform(
                  fieldName = mutableListOf("path", "new_field"),
                  transformType = FieldTransform.TransformType.UPDATE_FIELD_SCHEMA,
                  breaking = false,
                ),
                FieldTransform(
                  fieldName = mutableListOf("path", "other_field"),
                  transformType = FieldTransform.TransformType.UPDATE_FIELD_SCHEMA,
                  breaking = false,
                ),
              ),
            streamAttributeTransforms = emptyList(),
          ),
      )

    Assertions
      .assertThat(applySchemaChangeHelper.formatDiff(transform))
      .isEqualTo("Modified stream 'bar.foo': Altered fields ['path.new_field', 'path.other_field']")
  }

  @Test
  fun removedColumnsInStreamFormat() {
    val transform =
      StreamTransform(
        transformType = StreamTransform.TransformType.UPDATE_STREAM,
        streamDescriptor = StreamDescriptor(name = "foo", namespace = "bar"),
        updateStream =
          StreamTransformUpdateStream(
            fieldTransforms =
              listOf(
                FieldTransform(
                  fieldName = mutableListOf("path", "new_field"),
                  transformType = FieldTransform.TransformType.REMOVE_FIELD,
                  breaking = false,
                ),
                FieldTransform(fieldName = mutableListOf("other_field"), transformType = FieldTransform.TransformType.REMOVE_FIELD, breaking = false),
              ),
            streamAttributeTransforms = emptyList(),
          ),
      )

    Assertions
      .assertThat(applySchemaChangeHelper.formatDiff(transform))
      .isEqualTo("Modified stream 'bar.foo': Removed fields ['path.new_field', 'other_field']")
  }

  private fun createAirbyteCatalogWithSchema(
    name: String,
    schema: JsonNode?,
    customizeStream: (AirbyteStream) -> AirbyteStream = { it },
  ): AirbyteCatalog {
    val airbyteStreamConfiguration1 =
      AirbyteStreamAndConfiguration(
        stream =
          customizeStream(
            AirbyteStream(
              name = name,
              jsonSchema = schema,
              supportedSyncModes = listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
            ),
          ),
        config = streamConfig(selected = true),
      )

    return AirbyteCatalog(streams = listOf(airbyteStreamConfiguration1))
  }

  private fun streamConfig(
    selected: Boolean,
    fieldSelectionEnabled: Boolean? = null,
    selectedFields: List<SelectedFieldInfo> = emptyList(),
  ): AirbyteStreamConfiguration =
    AirbyteStreamConfiguration(
      syncMode = SyncMode.FULL_REFRESH,
      destinationSyncMode = DestinationSyncMode.OVERWRITE,
      selected = selected,
      fieldSelectionEnabled = fieldSelectionEnabled,
      selectedFields = selectedFields,
    )

  @Test
  fun mixedChangesInStreamFormat() {
    val transform =
      StreamTransform(
        transformType = StreamTransform.TransformType.UPDATE_STREAM,
        streamDescriptor = StreamDescriptor(name = "foo", namespace = "bar"),
        updateStream =
          StreamTransformUpdateStream(
            fieldTransforms =
              listOf(
                FieldTransform(
                  fieldName = mutableListOf("path", "new_field"),
                  transformType = FieldTransform.TransformType.ADD_FIELD,
                  breaking = false,
                ),
                FieldTransform(fieldName = mutableListOf("old_field"), transformType = FieldTransform.TransformType.REMOVE_FIELD, breaking = false),
                FieldTransform(
                  fieldName = mutableListOf("old_path", "deprecated"),
                  transformType = FieldTransform.TransformType.REMOVE_FIELD,
                  breaking = false,
                ),
                FieldTransform(
                  fieldName = mutableListOf("properties", "changed_type"),
                  transformType = FieldTransform.TransformType.UPDATE_FIELD_SCHEMA,
                  breaking = false,
                ),
              ),
            streamAttributeTransforms = emptyList(),
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
          CatalogDiff(transforms = emptyList()),
          ConnectionRead(
            connectionId = UUID.randomUUID(),
            name = "connection",
            sourceId = UUID.randomUUID(),
            destinationId = UUID.randomUUID(),
            syncCatalog = AirbyteCatalog(streams = emptyList()),
            status = ConnectionStatus.ACTIVE,
            breakingChange = false,
            nonBreakingChangesPreference = NonBreakingChangesPreference.IGNORE,
          ),
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
    Assertions.assertThat(result.appliedDiff.transforms).isEmpty()
    Assertions.assertThat(result.changeDescription).isEmpty()
  }

  @Test
  fun testContainsBreakingChange() {
    val updateWithNoBreakingTransforms =
      StreamTransformUpdateStream(
        fieldTransforms = listOf(FieldTransform(transformType = FieldTransform.TransformType.ADD_FIELD, fieldName = emptyList(), breaking = false)),
        streamAttributeTransforms =
          listOf(
            StreamAttributeTransform(transformType = StreamAttributeTransform.TransformType.UPDATE_PRIMARY_KEY, breaking = false),
          ),
      )
    val catalogDiff1 =
      CatalogDiff(
        transforms =
          listOf(
            StreamTransform(
              transformType = StreamTransform.TransformType.UPDATE_STREAM,
              streamDescriptor = StreamDescriptor(name = NAME1),
              updateStream = updateWithNoBreakingTransforms,
            ),
          ),
      )

    org.junit.jupiter.api.Assertions
      .assertFalse(applySchemaChangeHelper.containsBreakingChange(catalogDiff1))

    val updateWithBreakingFieldTransform =
      StreamTransformUpdateStream(
        fieldTransforms = listOf(FieldTransform(transformType = FieldTransform.TransformType.ADD_FIELD, fieldName = emptyList(), breaking = true)),
        streamAttributeTransforms =
          listOf(
            StreamAttributeTransform(transformType = StreamAttributeTransform.TransformType.UPDATE_PRIMARY_KEY, breaking = false),
          ),
      )
    val catalogDiff2 =
      CatalogDiff(
        transforms =
          listOf(
            StreamTransform(
              transformType = StreamTransform.TransformType.UPDATE_STREAM,
              streamDescriptor = StreamDescriptor(name = NAME1),
              updateStream = updateWithBreakingFieldTransform,
            ),
          ),
      )

    org.junit.jupiter.api.Assertions
      .assertTrue(applySchemaChangeHelper.containsBreakingChange(catalogDiff2))

    val updateWithBreakingAttributeTransform =
      StreamTransformUpdateStream(
        fieldTransforms = listOf(FieldTransform(transformType = FieldTransform.TransformType.ADD_FIELD, fieldName = emptyList(), breaking = false)),
        streamAttributeTransforms =
          listOf(
            StreamAttributeTransform(transformType = StreamAttributeTransform.TransformType.UPDATE_PRIMARY_KEY, breaking = true),
          ),
      )
    val catalogDiff3 =
      CatalogDiff(
        transforms =
          listOf(
            StreamTransform(
              transformType = StreamTransform.TransformType.UPDATE_STREAM,
              streamDescriptor = StreamDescriptor(name = NAME1),
              updateStream = updateWithBreakingAttributeTransform,
            ),
          ),
      )

    org.junit.jupiter.api.Assertions
      .assertTrue(applySchemaChangeHelper.containsBreakingChange(catalogDiff3))
  }

  @Nested
  internal inner class FieldSelectionInteractions {
    val oldCatalog: AirbyteCatalog =
      AirbyteCatalog(
        streams =
          listOf(
            AirbyteStreamAndConfiguration(
              stream = AirbyteStream(name = "users", namespace = "public"),
              config =
                streamConfig(
                  selected = true,
                  fieldSelectionEnabled = true,
                  selectedFields =
                    listOf(
                      SelectedFieldInfo(fieldPath = mutableListOf("id")),
                      SelectedFieldInfo(fieldPath = mutableListOf("address")),
                    ),
                ),
            ),
          ),
      )
    val newCatalog: AirbyteCatalog =
      AirbyteCatalog(
        streams =
          listOf(
            AirbyteStreamAndConfiguration(
              stream = AirbyteStream(name = "users", namespace = "public"),
              config = streamConfig(selected = true, fieldSelectionEnabled = true),
            ),
          ),
      )

    private fun fieldIsSelected(
      catalog: AirbyteCatalog,
      path: List<String>,
    ): Int =
      catalog
        .streams[0]
        .config!!
        .selectedFields
        .orEmpty()
        .filter { selected: SelectedFieldInfo -> selected == SelectedFieldInfo(fieldPath = path) }
        .size

    @Test
    fun testPropagateChangesDoesNotRemoveAlreadySelectedFields() {
      val transformations =
        listOf(
          StreamTransform(
            transformType = StreamTransform.TransformType.UPDATE_STREAM,
            streamDescriptor = StreamDescriptor(name = "users", namespace = "public"),
            updateStream =
              StreamTransformUpdateStream(
                fieldTransforms =
                  listOf(
                    FieldTransform(transformType = FieldTransform.TransformType.ADD_FIELD, fieldName = mutableListOf("ssn"), breaking = false),
                  ),
                streamAttributeTransforms = emptyList(),
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
          StreamTransform(
            transformType = StreamTransform.TransformType.UPDATE_STREAM,
            streamDescriptor = StreamDescriptor(name = "users", namespace = "public"),
            updateStream =
              StreamTransformUpdateStream(
                fieldTransforms =
                  listOf(
                    FieldTransform(transformType = FieldTransform.TransformType.ADD_FIELD, fieldName = mutableListOf("ssn"), breaking = false),
                  ),
                streamAttributeTransforms = emptyList(),
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
          StreamTransform(
            transformType = StreamTransform.TransformType.UPDATE_STREAM,
            streamDescriptor = StreamDescriptor(name = "users", namespace = "public"),
            updateStream =
              StreamTransformUpdateStream(
                fieldTransforms =
                  listOf(
                    FieldTransform(
                      transformType = FieldTransform.TransformType.ADD_FIELD,
                      fieldName = mutableListOf("address", "zip"),
                      breaking = false,
                    ),
                  ),
                streamAttributeTransforms = emptyList(),
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
          StreamTransform(
            transformType = StreamTransform.TransformType.UPDATE_STREAM,
            streamDescriptor = StreamDescriptor(name = "users", namespace = "public"),
            updateStream =
              StreamTransformUpdateStream(
                fieldTransforms =
                  listOf(
                    FieldTransform(
                      transformType = FieldTransform.TransformType.ADD_FIELD,
                      fieldName = mutableListOf("username", "domain"),
                      breaking = false,
                    ),
                  ),
                streamAttributeTransforms = emptyList(),
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
          StreamTransform(
            transformType = StreamTransform.TransformType.UPDATE_STREAM,
            streamDescriptor = StreamDescriptor(name = "users", namespace = "public"),
            updateStream =
              StreamTransformUpdateStream(
                fieldTransforms =
                  listOf(
                    FieldTransform(
                      transformType = FieldTransform.TransformType.ADD_FIELD,
                      fieldName = mutableListOf("username", "domain"),
                      breaking = false,
                    ),
                    FieldTransform(transformType = FieldTransform.TransformType.ADD_FIELD, fieldName = mutableListOf("username"), breaking = false),
                  ),
                streamAttributeTransforms = emptyList(),
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
