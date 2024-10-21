package io.airbyte.mappers.helpers

import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.MapperOperationName
import io.airbyte.mappers.transformations.HashingMapper

internal const val DEFAULT_HASHING_METHOD = HashingMapper.SHA256
internal const val DEFAULT_HASHING_SUFFIX = "_hashed"

/**
 * Create a hashing mapper for a given field.
 */
fun createHashingMapper(fieldName: String): ConfiguredMapper {
  return ConfiguredMapper(
    name = MapperOperationName.HASHING,
    config =
      mapOf(
        HashingMapper.TARGET_FIELD_CONFIG_KEY to fieldName,
        HashingMapper.METHOD_CONFIG_KEY to DEFAULT_HASHING_METHOD,
        HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY to DEFAULT_HASHING_SUFFIX,
      ),
  )
}

/**
 * Get the name of the field that is being hashed from a hashing mapper.
 */
fun getHashedFieldName(mapper: ConfiguredMapper): String {
  return mapper.config.getOrElse(HashingMapper.TARGET_FIELD_CONFIG_KEY) {
    throw IllegalStateException("Hashing mapper must have a target field")
  }
}
