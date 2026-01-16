/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.helpers

import io.airbyte.config.MapperOperationName
import io.airbyte.config.mapper.configs.HashingConfig
import io.airbyte.config.mapper.configs.HashingMapperConfig
import io.airbyte.config.mapper.configs.HashingMethods
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class MapperHelperTest {
  @Test
  fun testCreateHashingMapper() {
    val fieldName = "my_field"
    val hashingMapper = createHashingMapper(fieldName)
    val expectedHashingMapper =
      HashingMapperConfig(
        name = MapperOperationName.HASHING,
        config =
          HashingConfig(
            fieldName,
            HashingMethods.SHA256,
            "_hashed",
          ),
      )
    Assertions.assertEquals(expectedHashingMapper, hashingMapper)
  }

  @Test
  fun testGetHashedFieldName() {
    val fieldName = "my_field"
    val hashingMapper =
      HashingMapperConfig(
        name = MapperOperationName.HASHING,
        config =
          HashingConfig(
            fieldName,
            HashingMethods.fromValue("SHA-1")!!,
            "_hashed",
          ),
      )
    Assertions.assertEquals(fieldName, getHashedFieldName(hashingMapper))
  }
}
