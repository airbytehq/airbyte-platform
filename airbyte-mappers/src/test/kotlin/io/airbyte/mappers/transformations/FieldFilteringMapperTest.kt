/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import TEST_OBJECT_MAPPER
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.config.MapperOperationName.FIELD_FILTERING
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.adapters.TestRecordAdapter
import io.airbyte.config.mapper.configs.FieldFilteringConfig
import io.airbyte.config.mapper.configs.FieldFilteringMapperConfig
import io.airbyte.mappers.transformations.FieldFilteringMapperTest.Fixtures.defaultConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test

class FieldFilteringMapperTest {
  private val fieldFilteringMapper = FieldFilteringMapper(TEST_OBJECT_MAPPER)

  @Test
  fun `map removes the targetField when present`() {
    val record =
      TestRecordAdapter(
        streamDescriptor = StreamDescriptor().withName("test-map"),
        data =
          mutableMapOf(
            "field1" to "value1",
            Fixtures.FIELD_TO_FILTER to "I am going away",
          ),
      )

    fieldFilteringMapper.map(defaultConfig, record)

    assertFalse(record.has(Fixtures.FIELD_TO_FILTER))
    assertEquals("value1", record.get("field1").asString())
    assertEquals(1, record.data.size)
  }

  @Test
  fun `map doesn't fail if the targetField is missing`() {
    val record =
      TestRecordAdapter(
        streamDescriptor = StreamDescriptor().withName("test-map"),
        data =
          mutableMapOf(
            "field1" to "value1",
            "field2" to "value2",
          ),
      )

    fieldFilteringMapper.map(defaultConfig, record)

    assertEquals("value1", record.get("field1").asString())
    assertEquals("value2", record.get("field2").asString())
    assertEquals(2, record.data.size)
  }

  @Test
  fun `schema returns a copy of the slimStream with the target field removed`() {
    val slimStream = SlimStream(listOf(Field(Fixtures.FIELD_TO_FILTER, FieldType.STRING), Field("other", FieldType.STRING)))
    val result = fieldFilteringMapper.schema(defaultConfig, slimStream)

    val expectedFields = listOf(Field("other", FieldType.STRING))
    assertEquals(expectedFields, result.fields)
    assertNotEquals(expectedFields, slimStream.fields)
  }

  object Fixtures {
    const val FIELD_TO_FILTER = "fieldToFilter"
    val defaultConfig =
      FieldFilteringMapperConfig(
        name = FIELD_FILTERING,
        documentationUrl = null,
        FieldFilteringConfig(targetField = FIELD_TO_FILTER),
      )
  }
}
