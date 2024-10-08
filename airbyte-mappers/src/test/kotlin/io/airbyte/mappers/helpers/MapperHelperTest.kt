package io.airbyte.mappers.helpers

import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.config.MapperOperationName
import io.airbyte.mappers.transformations.HashingMapper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class MapperHelperTest {
  companion object {
    private const val STREAM_NAME = "my_stream"
    private const val FIELD_NAME = "my_field"

    fun buildCatalog(
      fields: List<Field>?,
      hashedFields: List<String>,
    ): ConfiguredAirbyteCatalog {
      return ConfiguredAirbyteCatalog(
        streams =
          listOf(
            ConfiguredAirbyteStream(
              stream = AirbyteStream(STREAM_NAME, Jsons.emptyObject(), emptyList()),
              fields = fields,
              mappers = hashedFields.map { createHashingMapper(it) },
            ),
          ),
      )
    }
  }

  @Test
  fun testCreateHashingMapper() {
    val fieldName = "my_field"
    val hashingMapper = createHashingMapper(fieldName)
    val expectedHashingMapper =
      ConfiguredMapper(
        name = MapperOperationName.HASHING,
        config =
          mapOf(
            "targetField" to fieldName,
            "method" to "SHA-256",
            "fieldNameSuffix" to "_hashed",
          ),
      )
    Assertions.assertEquals(expectedHashingMapper, hashingMapper)
  }

  @Test
  fun testGetHashedFieldName() {
    val fieldName = "my_field"
    val hashingMapper =
      ConfiguredMapper(
        name = MapperOperationName.HASHING,
        config =
          mapOf(
            HashingMapper.TARGET_FIELD_CONFIG_KEY to fieldName,
            HashingMapper.METHOD_CONFIG_KEY to "SHA-1",
            HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to "_hashed",
          ),
      )
    Assertions.assertEquals(fieldName, getHashedFieldName(hashingMapper))
  }

  @Test
  fun testValidateConfiguredMappers() {
    val validCatalog =
      buildCatalog(
        fields =
          listOf(
            Field(name = FIELD_NAME, type = FieldType.STRING),
          ),
        hashedFields = listOf(FIELD_NAME),
      )

    validateConfiguredMappers(validCatalog)
  }

  @Test
  fun testValidateConfiguredMappersForMissingField() {
    val catalog =
      buildCatalog(
        fields =
          listOf(
            Field(name = FIELD_NAME, type = FieldType.STRING),
          ),
        hashedFields = listOf("missing_field"),
      )

    val exception =
      assertThrows<IllegalArgumentException> {
        validateConfiguredMappers(catalog)
      }
    Assertions.assertEquals("Hashed field 'missing_field' not found in stream 'my_stream'.", exception.message)
  }

  @Test
  fun testValidateConfiguredMappersForExistingHashedField() {
    val catalog =
      buildCatalog(
        fields =
          listOf(
            Field(name = FIELD_NAME, type = FieldType.STRING),
            Field(name = "${FIELD_NAME}_hashed", type = FieldType.STRING),
          ),
        hashedFields = listOf(FIELD_NAME),
      )

    val exception =
      assertThrows<IllegalArgumentException> {
        validateConfiguredMappers(catalog)
      }
    Assertions.assertEquals("Hashed field 'my_field_hashed' already exists in stream 'my_stream'.", exception.message)
  }

  @Test
  fun testValidateConfiguredMappersForMissingFields() {
    val catalog =
      buildCatalog(
        fields = null,
        hashedFields = listOf(FIELD_NAME),
      )

    val exception =
      assertThrows<NullPointerException> {
        validateConfiguredMappers(catalog)
      }
    Assertions.assertEquals("Fields must be set in order to use mappers.", exception.message)
  }
}
