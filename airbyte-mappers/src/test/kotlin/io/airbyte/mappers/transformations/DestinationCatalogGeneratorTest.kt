package io.airbyte.mappers.transformations

import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.mappers.mocks.TestMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class DestinationCatalogGeneratorTest {
  private val destinationCatalogGeneratorWithoutMapper = DestinationCatalogGenerator(listOf())
  private val destinationCatalogGeneratorWithMapper = DestinationCatalogGenerator(listOf(TestMapper()))

  @Test
  fun `test generateDestinationCatalogWithoutMapper`() {
    val configuredUsersStream =
      ConfiguredAirbyteStream(
        stream =
          AirbyteStream(
            name = "users",
            jsonSchema =
              Jsons.jsonNode(
                mapOf(
                  "type" to "object",
                  "${'$'}schema" to "http://json-schema.org/schema#",
                  "properties" to mapOf("field1" to mapOf("type" to "string")),
                ),
              ),
            supportedSyncModes = listOf(),
          ),
        fields =
          listOf(
            Field(name = "field1", type = FieldType.STRING),
          ),
      )

    val catalog = ConfiguredAirbyteCatalog(streams = listOf(configuredUsersStream))

    val catalogCopy = destinationCatalogGeneratorWithoutMapper.generateDestinationCatalog(catalog)

    assertEquals(catalog.streams[0].stream.jsonSchema, catalogCopy.streams[0].stream.jsonSchema)
  }

  @Test
  fun `test generateDestinationCatalogMissingMapper`() {
    val configuredUsersStream =
      ConfiguredAirbyteStream(
        stream =
          AirbyteStream(
            name = "users",
            jsonSchema =
              Jsons.jsonNode(
                mapOf(
                  "type" to "object",
                  "${'$'}schema" to "http://json-schema.org/schema#",
                  "properties" to mapOf("field1" to mapOf("type" to "string")),
                ),
              ),
            supportedSyncModes = listOf(),
          ),
        fields =
          listOf(
            Field(name = "field1", type = FieldType.STRING),
          ),
        mappers = listOf(ConfiguredMapper("test", mapOf())),
      )

    val catalog = ConfiguredAirbyteCatalog(streams = listOf(configuredUsersStream))

    assertThrows<IllegalStateException> { destinationCatalogGeneratorWithoutMapper.generateDestinationCatalog(catalog) }
  }

  @Test
  fun `test generateDestinationFieldsWithoutMapper`() {
    val configuredUsersStream =
      ConfiguredAirbyteStream(
        stream =
          AirbyteStream(
            name = "users",
            jsonSchema = Jsons.emptyObject(),
            supportedSyncModes = listOf(),
          ),
        fields =
          listOf(
            Field(name = "field1_1", type = FieldType.STRING),
            Field(name = "field1_2", type = FieldType.DATE),
          ),
      )

    val resultFields = destinationCatalogGeneratorWithoutMapper.applyMapperToFields(configuredUsersStream)

    assertEquals(configuredUsersStream.fields, resultFields)
  }

  @Test
  fun `test generateDestinationFieldsWithMappers`() {
    val configuredUsersStream =
      ConfiguredAirbyteStream(
        stream =
          AirbyteStream(
            name = "users",
            jsonSchema = Jsons.emptyObject(),
            supportedSyncModes = listOf(),
          ),
        fields =
          listOf(
            Field(name = "field1_1", type = FieldType.STRING),
            Field(name = "field1_2", type = FieldType.DATE),
          ),
        mappers = listOf(ConfiguredMapper("test", mapOf()), ConfiguredMapper("test", mapOf())),
      )

    val resultFields = destinationCatalogGeneratorWithMapper.applyMapperToFields(configuredUsersStream)

    assertEquals(
      listOf(
        Field(name = "field1_1_test_test", type = FieldType.STRING),
        Field(name = "field1_2_test_test", type = FieldType.STRING),
      ),
      resultFields,
    )
  }

  @Test
  fun `test generateDestinationCatalogWithMapper`() {
    val configuredUsersStream1 =
      ConfiguredAirbyteStream(
        stream =
          AirbyteStream(
            name = "users",
            jsonSchema =
              Jsons.jsonNode(
                mapOf(
                  "type" to "object",
                  "${'$'}schema" to "http://json-schema.org/schema#",
                  "properties" to
                    mapOf(
                      "field1_1" to mapOf("type" to "string"),
                      "field1_2" to mapOf("type" to "string", "format" to "date"),
                    ),
                ),
              ),
            supportedSyncModes = listOf(),
          ),
        fields =
          listOf(
            Field(name = "field1_1", type = FieldType.STRING),
            Field(name = "field1_2", type = FieldType.DATE),
          ),
        mappers = listOf(ConfiguredMapper("test", mapOf())),
      )

    val configuredUsersStream2 =
      ConfiguredAirbyteStream(
        stream =
          AirbyteStream(
            name = "users",
            jsonSchema =
              Jsons.jsonNode(
                mapOf(
                  "type" to "object",
                  "${'$'}schema" to "http://json-schema.org/schema#",
                  "properties" to
                    mapOf(
                      "field2_1" to mapOf("type" to "integer"),
                    ),
                ),
              ),
            supportedSyncModes = listOf(),
          ),
        fields =
          listOf(
            Field(name = "field2_1", type = FieldType.INTEGER),
          ),
        mappers = listOf(ConfiguredMapper("test", mapOf())),
      )

    val catalog = ConfiguredAirbyteCatalog(streams = listOf(configuredUsersStream1, configuredUsersStream2))

    val catalogCopy = destinationCatalogGeneratorWithMapper.generateDestinationCatalog(catalog)

    assertEquals(
      Jsons.jsonNode(
        mapOf(
          "type" to "object",
          "${'$'}schema" to "http://json-schema.org/schema#",
          "properties" to
            mapOf(
              "field1_1_test" to mapOf("type" to "string"),
              "field1_2_test" to mapOf("type" to "string"),
            ),
        ),
      ),
      catalogCopy.streams[0].stream.jsonSchema,
    )
    assertEquals(
      Jsons.jsonNode(
        mapOf(
          "type" to "object",
          "${'$'}schema" to "http://json-schema.org/schema#",
          "properties" to
            mapOf(
              "field2_1_test" to mapOf("type" to "string"),
            ),
        ),
      ),
      catalogCopy.streams[1].stream.jsonSchema,
    )
  }

  @Test
  fun `test fieldSerialization`() {
    val input =
      listOf(
        Field("fieldString", FieldType.STRING),
        Field("fieldBoolean", FieldType.BOOLEAN),
        Field("fieldInteger", FieldType.INTEGER),
        Field("fieldNumber", FieldType.NUMBER),
        Field("fieldDate", FieldType.DATE),
        Field("fieldTimestampWithoutTimezone", FieldType.TIMESTAMP_WITHOUT_TIMEZONE),
        Field("fieldTimestampWithTimezone", FieldType.TIMESTAMP_WITH_TIMEZONE),
        Field("fieldTimeWithoutTimezone", FieldType.TIME_WITHOUT_TIMEZONE),
        Field("fieldTimeWithTimezone", FieldType.TIME_WITH_TIMEZONE),
        Field("fieldObject", FieldType.OBJECT),
        Field("fieldArray", FieldType.OBJECT),
        Field("fieldMulti", FieldType.OBJECT),
        Field("fieldUnknown", FieldType.UNKNOWN),
      )
    val expectedOutputJson =
      Jsons.deserialize(
        """
      {
        "fieldString": {
          "type": "string"
        },
        "fieldBoolean": {
          "type": "boolean"
        },
        "fieldInteger": {
          "type": "integer"
        },
        "fieldNumber": {
          "type": "number"
        },
        "fieldDate": {
          "type": "string",
          "format": "date"
        },
        "fieldTimestampWithoutTimezone": {
          "type": "string",
          "format": "date-time",
          "airbyteType": "timestamp_without_timezone"
        },
        "fieldTimestampWithTimezone": {
          "type": "string",
          "format": "date-time",
          "airbyteType": "timestamp_with_timezone"
        },
        "fieldTimeWithoutTimezone": {
          "type": "string",
          "format": "time",
          "airbyteType": "time_without_timezone"
        },
        "fieldTimeWithTimezone": {
          "type": "string",
          "format": "time",
          "airbyteType": "time_with_timezone"
        },
        "fieldObject": {
          "type": "object"
        },
        "fieldArray": {
          "type": "array"
        },
        "fieldMulti": {
          "type": "oneOf"
        },
        "fieldUnknown": {
          "I": "don't",
          "follow": "specs"
        }
      }
    """,
      )

    val result =
      destinationCatalogGeneratorWithoutMapper.generateJsonSchemaFromFields(
        input,
        Jsons.jsonNode(
          mapOf(
            "fieldObject" to mapOf("type" to "object"),
            "fieldArray" to mapOf("type" to "array"),
            "fieldMulti" to mapOf("type" to "oneOf"),
            "fieldUnknown" to mapOf("I" to "don't", "follow" to "specs"),
          ),
        ),
      )
    assertEquals(expectedOutputJson, Jsons.deserialize(result))
  }
}
