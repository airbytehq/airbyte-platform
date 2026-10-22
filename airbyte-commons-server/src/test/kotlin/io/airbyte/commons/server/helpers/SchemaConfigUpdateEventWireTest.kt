/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers

import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.AirbyteStream
import io.airbyte.api.server.generated.models.AirbyteStreamAndConfiguration
import io.airbyte.api.server.generated.models.AirbyteStreamConfiguration
import io.airbyte.api.server.generated.models.CatalogDiff
import io.airbyte.api.server.generated.models.DestinationSyncMode
import io.airbyte.api.server.generated.models.FieldAdd
import io.airbyte.api.server.generated.models.FieldTransform
import io.airbyte.api.server.generated.models.SelectedFieldInfo
import io.airbyte.api.server.generated.models.StreamDescriptor
import io.airbyte.api.server.generated.models.StreamTransform
import io.airbyte.api.server.generated.models.StreamTransformUpdateStream
import io.airbyte.api.server.generated.models.SyncMode
import io.airbyte.commons.json.Jsons
import io.airbyte.data.services.shared.SchemaConfigUpdateEvent
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * The schema config update event is stored as JSON in connection timeline rows and read back by the
 * webapp. The expected document is what the Java-generated models wrote, so it must not change.
 */
class SchemaConfigUpdateEventWireTest {
  private val helper = CatalogConfigDiffHelper()

  @Test
  fun `the stored event JSON is unchanged`() {
    val configDiff = helper.getCatalogConfigDiff(SOURCE_CATALOG, OLD_CATALOG, NEW_CATALOG)
    val event = SchemaConfigUpdateEvent(helper.getAirbyteCatalogDiff(SCHEMA_DIFF, configDiff)!!)

    assertEquals(Jsons.deserialize(EXPECTED_EVENT_JSON), Jsons.jsonNode(event))
  }

  companion object {
    private val SCHEMA =
      Jsons.deserialize(
        """{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"},"updated_at":{"type":"string"}}}""",
      )

    private fun stream(name: String) = AirbyteStream(name = name, namespace = "public", jsonSchema = SCHEMA, supportedSyncModes = listOf())

    private fun config(
      selected: Boolean,
      syncMode: SyncMode = SyncMode.FULL_REFRESH,
      destinationSyncMode: DestinationSyncMode = DestinationSyncMode.OVERWRITE,
      selectedFields: List<String>? = null,
      primaryKey: List<List<String>> = listOf(),
      cursorField: List<String> = listOf(),
    ) = AirbyteStreamConfiguration(
      syncMode = syncMode,
      destinationSyncMode = destinationSyncMode,
      selected = selected,
      fieldSelectionEnabled = selectedFields != null,
      selectedFields = selectedFields?.map { SelectedFieldInfo(fieldPath = listOf(it)) } ?: listOf(),
      primaryKey = primaryKey,
      cursorField = cursorField,
    )

    private val SOURCE_CATALOG =
      AirbyteCatalog(
        streams = listOf("enabled", "disabled", "fields", "keys").map { AirbyteStreamAndConfiguration(stream = stream(it), config = config(false)) },
      )

    private val OLD_CATALOG =
      AirbyteCatalog(
        streams =
          listOf(
            AirbyteStreamAndConfiguration(stream = stream("disabled"), config = config(true)),
            AirbyteStreamAndConfiguration(stream = stream("fields"), config = config(true, selectedFields = listOf("id", "name"))),
            AirbyteStreamAndConfiguration(
              stream = stream("keys"),
              config = config(true, primaryKey = listOf(listOf("id")), cursorField = listOf("updated_at")),
            ),
          ),
      )

    private val NEW_CATALOG =
      AirbyteCatalog(
        streams =
          listOf(
            AirbyteStreamAndConfiguration(stream = stream("enabled"), config = config(true, selectedFields = listOf("id"))),
            AirbyteStreamAndConfiguration(stream = stream("disabled"), config = config(false)),
            AirbyteStreamAndConfiguration(stream = stream("fields"), config = config(true, selectedFields = listOf("id", "updated_at"))),
            AirbyteStreamAndConfiguration(
              stream = stream("keys"),
              config =
                config(
                  true,
                  syncMode = SyncMode.INCREMENTAL,
                  destinationSyncMode = DestinationSyncMode.APPEND_DEDUP,
                  primaryKey = listOf(listOf("name")),
                  cursorField = listOf("id"),
                ),
            ),
          ),
      )

    private val SCHEMA_DIFF =
      CatalogDiff(
        transforms =
          listOf(
            StreamTransform(
              transformType = StreamTransform.TransformType.ADD_STREAM,
              streamDescriptor = StreamDescriptor(name = "enabled", namespace = "public"),
            ),
            StreamTransform(
              transformType = StreamTransform.TransformType.UPDATE_STREAM,
              streamDescriptor = StreamDescriptor(name = "fields", namespace = "public"),
              updateStream =
                StreamTransformUpdateStream(
                  fieldTransforms =
                    listOf(
                      FieldTransform(
                        transformType = FieldTransform.TransformType.ADD_FIELD,
                        fieldName = listOf("updated_at"),
                        breaking = false,
                        addField = FieldAdd(schema = Jsons.deserialize("""{"type":"string"}""")),
                      ),
                    ),
                  streamAttributeTransforms = listOf(),
                ),
            ),
          ),
      )

    private val EXPECTED_EVENT_JSON =
      """
      {
        "airbyteCatalogDiff": {
          "catalogDiff": {
            "transforms": [
              {
                "transformType": "add_stream",
                "streamDescriptor": {
                  "name": "enabled",
                  "namespace": "public"
                }
              },
              {
                "transformType": "update_stream",
                "streamDescriptor": {
                  "name": "fields",
                  "namespace": "public"
                },
                "updateStream": {
                  "fieldTransforms": [
                    {
                      "transformType": "add_field",
                      "fieldName": [
                        "updated_at"
                      ],
                      "breaking": false,
                      "addField": {
                        "schema": {
                          "type": "string"
                        }
                      }
                    }
                  ],
                  "streamAttributeTransforms": []
                }
              }
            ]
          },
          "catalogConfigDiff": {
            "streamsEnabled": [
              {
                "streamName": "enabled",
                "streamNamespace": "public",
                "status": "enabled",
                "fields": [
                  "id"
                ]
              }
            ],
            "streamsDisabled": [
              {
                "streamName": "disabled",
                "streamNamespace": "public",
                "status": "disabled",
                "fields": []
              }
            ],
            "fieldsEnabled": [
              {
                "streamName": "fields",
                "streamNamespace": "public",
                "status": "enabled",
                "fields": [
                  "updated_at"
                ]
              }
            ],
            "fieldsDisabled": [
              {
                "streamName": "fields",
                "streamNamespace": "public",
                "status": "disabled",
                "fields": [
                  "name"
                ]
              }
            ],
            "primaryKeysChanged": [
              {
                "streamName": "keys",
                "streamNamespace": "public",
                "prev": [
                  [
                    "id"
                  ]
                ],
                "current": [
                  [
                    "name"
                  ]
                ]
              }
            ],
            "cursorFieldsChanged": [
              {
                "streamName": "keys",
                "streamNamespace": "public",
                "prev": [
                  "updated_at"
                ],
                "current": [
                  "id"
                ]
              }
            ],
            "syncModesChanged": [
              {
                "streamName": "keys",
                "streamNamespace": "public",
                "prevSourceSyncMode": "full_refresh",
                "prevDestinationSyncMode": "overwrite",
                "currentSourceSyncMode": "incremental",
                "currentDestinationSyncMode": "append_dedup"
              }
            ]
          }
        }
      }
      """.trimIndent()
  }
}
