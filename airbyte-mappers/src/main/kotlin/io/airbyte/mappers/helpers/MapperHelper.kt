package io.airbyte.mappers.helpers

import com.google.common.base.Preconditions
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.Field
import io.airbyte.config.MapperOperationName
import io.airbyte.mappers.transformations.HashingMapper

internal const val DEFAULT_HASHING_METHOD = HashingMapper.SHA1
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

/**
 * Validates that the configured mappers in a configured catalog are valid.
 * For now this only checks that the hashing mapper is correctly configured, but we should move to using mapper specs in the future.
 */
fun validateConfiguredMappers(configuredCatalog: ConfiguredAirbyteCatalog) {
  for (configuredStream in configuredCatalog.streams) {
    val fields = configuredStream.fields
    for (mapper in configuredStream.mappers) {
      Preconditions.checkArgument(MapperOperationName.HASHING == mapper.name, "Mapping operation %s is not supported.", mapper.name)
      Preconditions.checkNotNull(fields, "Fields must be set in order to use mappers.")

      val mappedField = getHashedFieldName(mapper)
      Preconditions.checkArgument(
        fields!!.stream().anyMatch { f: Field -> f.name == mappedField },
        "Hashed field %s not found in stream %s.",
        mappedField,
        configuredStream.stream.name,
      )

      val mappedFieldName =
        getHashedFieldName(
          mapper,
        ) + mapper.config.getOrDefault(HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY, DEFAULT_HASHING_SUFFIX)
      Preconditions.checkArgument(
        fields.stream().noneMatch { f: Field -> f.name == mappedFieldName },
        "Hashed field %s already exists in stream %s.",
        mappedFieldName,
        configuredStream.stream.name,
      )
    }
  }
}
