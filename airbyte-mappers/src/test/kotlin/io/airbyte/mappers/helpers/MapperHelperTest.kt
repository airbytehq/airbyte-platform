package io.airbyte.mappers.helpers

import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.MapperOperationName
import io.airbyte.mappers.transformations.HashingMapper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class MapperHelperTest {
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
}
