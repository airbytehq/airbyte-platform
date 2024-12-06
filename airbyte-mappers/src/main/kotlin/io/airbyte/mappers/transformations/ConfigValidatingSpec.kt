package io.airbyte.mappers.transformations

import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.MapperConfig

abstract class ConfigValidatingSpec<T : MapperConfig> : MapperSpec<T> {
  private val configuredMapperValidator: ConfiguredMapperValidator = ConfiguredMapperValidator()
  protected val simpleJsonSchemaGenerator: SimpleJsonSchemaGeneratorFromSpec = SimpleJsonSchemaGeneratorFromSpec()

  private fun validateConfig(configuredMapper: ConfiguredMapper) {
    configuredMapperValidator.validateMapperConfig(jsonSchema(), configuredMapper)
  }

  final override fun deserialize(configuredMapper: ConfiguredMapper): T {
    validateConfig(configuredMapper)
    return deserializeVerifiedConfig(configuredMapper)
  }

  abstract fun deserializeVerifiedConfig(configuredMapper: ConfiguredMapper): T
}
