package io.airbyte.mappers.helpers

import io.airbyte.config.MapperOperationName
import io.airbyte.config.mapper.configs.HashingConfig
import io.airbyte.config.mapper.configs.HashingMapperConfig
import io.airbyte.config.mapper.configs.HashingMethods
import io.airbyte.mappers.transformations.HashingMapper

internal const val DEFAULT_HASHING_METHOD = HashingMapper.SHA256
internal const val DEFAULT_HASHING_SUFFIX = "_hashed"

/**
 * Create a hashing mapper for a given field.
 */
fun createHashingMapper(fieldName: String): HashingMapperConfig {
  return HashingMapperConfig(
    name = MapperOperationName.HASHING,
    config =
      HashingConfig(
        fieldName,
        HashingMethods.fromValue(DEFAULT_HASHING_METHOD)!!,
        DEFAULT_HASHING_SUFFIX,
      ),
  )
}

/**
 * Get the name of the field that is being hashed from a hashing mapper.
 */
fun getHashedFieldName(mapper: HashingMapperConfig): String {
  return mapper.config.targetField
}
