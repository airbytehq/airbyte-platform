package io.airbyte.mappers.helpers

import com.google.common.base.Preconditions
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.Field
import io.airbyte.config.MapperOperationName
import io.airbyte.mappers.transformations.FieldRenamingMapper.Companion.NEW_FIELD_NAME
import io.airbyte.mappers.transformations.FieldRenamingMapper.Companion.ORIGINAL_FIELD_NAME
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

/**
 * Validates that the configured mappers in a configured catalog are valid.
 */
fun validateConfiguredMappers(configuredCatalog: ConfiguredAirbyteCatalog) {
  for (configuredStream in configuredCatalog.streams) {
    validateConfiguredStream(configuredStream)
  }
}

private fun validateConfiguredStream(configuredStream: ConfiguredAirbyteStream) {
  val fields = configuredStream.fields
  Preconditions.checkNotNull(fields, "Fields must be set in order to use mappers.")

  for (mapper in configuredStream.mappers) {
    validateMapper(configuredStream, mapper, fields!!)
  }
}

private fun validateMapper(
  configuredStream: ConfiguredAirbyteStream,
  mapper: ConfiguredMapper,
  fields: List<Field>,
) {
  when (mapper.name) {
    MapperOperationName.HASHING -> validateHashingMapper(configuredStream, mapper, fields)
    MapperOperationName.FIELD_RENAMING -> validateFieldRenamingMapper(configuredStream, mapper, fields)
    else ->
      Preconditions.checkArgument(
        false,
        "Mapping operation %s is not supported.",
        mapper.name,
      )
  }
}

private fun validateHashingMapper(
  configuredStream: ConfiguredAirbyteStream,
  mapper: ConfiguredMapper,
  fields: List<Field>,
) {
  val mappedField = getHashedFieldName(mapper)

  Preconditions.checkArgument(
    fields.any { it.name == mappedField },
    "Hashed field '%s' not found in stream '%s'.",
    mappedField,
    configuredStream.stream.name,
  )

  val suffix =
    mapper.config.getOrDefault(
      HashingMapper.FIELD_NAME_SUFFIX_CONFIG_KEY,
      DEFAULT_HASHING_SUFFIX,
    )
  val mappedFieldName = "$mappedField$suffix"

  Preconditions.checkArgument(
    fields.none { it.name == mappedFieldName },
    "Hashed field '%s' already exists in stream '%s'.",
    mappedFieldName,
    configuredStream.stream.name,
  )
}

private fun validateFieldRenamingMapper(
  configuredStream: ConfiguredAirbyteStream,
  mapper: ConfiguredMapper,
  fields: List<Field>,
) {
  val originalFieldName =
    mapper.config[ORIGINAL_FIELD_NAME]
      ?: throw IllegalArgumentException("Config missing required key: originalFieldName")
  val newFieldName =
    mapper.config[NEW_FIELD_NAME]
      ?: throw IllegalArgumentException("Config missing required key: newFieldName")

  Preconditions.checkArgument(
    fields.any { it.name == originalFieldName },
    "Original field '%s' not found in stream '%s'.",
    originalFieldName,
    configuredStream.stream.name,
  )

  Preconditions.checkArgument(
    fields.none { it.name == newFieldName },
    "New field '%s' already exists in stream '%s'.",
    newFieldName,
    configuredStream.stream.name,
  )
}
