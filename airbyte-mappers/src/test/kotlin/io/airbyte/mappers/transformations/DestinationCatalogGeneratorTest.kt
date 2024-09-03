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
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

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

  @ParameterizedTest
  @MethodSource("getFieldsAndExpectedOutput")
  fun `test fieldSerialization`(
    field: Field,
    expectedOutput: String,
  ) {
    val result =
      destinationCatalogGeneratorWithoutMapper.fieldSerialization(
        field,
        Jsons.jsonNode(
          mapOf(
            "fieldObject" to mapOf("type" to "object"),
            "fieldArray" to mapOf("type" to "array"),
            "fieldMulti" to mapOf("type" to "oneOf"),
          ),
        ),
      )
    assertEquals(expectedOutput, result)
  }

  companion object {
    @JvmStatic
    private fun getFieldsAndExpectedOutput(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(
          Field("fieldString", FieldType.STRING),
          """{"type":"string"}""",
        ),
        Arguments.of(
          Field("fieldBoolean", FieldType.BOOLEAN),
          """{"type":"boolean"}""",
        ),
        Arguments.of(
          Field("fieldInteger", FieldType.INTEGER),
          """{"type":"integer"}""",
        ),
        Arguments.of(
          Field("fieldNumber", FieldType.NUMBER),
          """{"type":"number"}""",
        ),
        Arguments.of(
          Field("fieldDate", FieldType.DATE),
          """{"type":"string","format":"date"}""",
        ),
        Arguments.of(
          Field("fieldTimestampWithoutTimezone", FieldType.TIMESTAMP_WITHOUT_TIMEZONE),
          """{"type":"string","format":"date-time","airbyteType":"timestamp_without_timezone"}""",
        ),
        Arguments.of(
          Field("fieldTimestampWithTimezone", FieldType.TIMESTAMP_WITH_TIMEZONE),
          """{"type":"string","format":"date-time","airbyteType":"timestamp_with_timezone"}""",
        ),
        Arguments.of(
          Field("fieldTimeWithoutTimezone", FieldType.TIME_WITHOUT_TIMEZONE),
          """{"type":"string","format":"time","airbyteType":"time_without_timezone"}""",
        ),
        Arguments.of(
          Field("fieldTimeWithTimezone", FieldType.TIME_WITH_TIMEZONE),
          """{"type":"string","format":"time","airbyteType":"time_with_timezone"}""",
        ),
        Arguments.of(
          Field("fieldObject", FieldType.OBJECT),
          """{"type":"object"}""",
        ),
        Arguments.of(
          Field("fieldArray", FieldType.OBJECT),
          """{"type":"array"}""",
        ),
        Arguments.of(
          Field("fieldMulti", FieldType.OBJECT),
          """{"type":"oneOf"}""",
        ),
      )
    }
  }
}
