/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.MapperConfig

abstract class ConfigValidatingSpec<T : MapperConfig>(
  private val objectMapper: ObjectMapper,
) : MapperSpec<T> {
  private val configuredMapperValidator: ConfiguredMapperValidator = ConfiguredMapperValidator()
  protected val simpleJsonSchemaGenerator: SimpleJsonSchemaGeneratorFromSpec = SimpleJsonSchemaGeneratorFromSpec()

  private fun validateConfig(configuredMapper: ConfiguredMapper) {
    configuredMapperValidator.validateMapperConfig(jsonSchema(), configuredMapper, objectMapper())
  }

  final override fun deserialize(configuredMapper: ConfiguredMapper): T {
    validateConfig(configuredMapper)
    return deserializeVerifiedConfig(configuredMapper)
  }

  abstract fun deserializeVerifiedConfig(configuredMapper: ConfiguredMapper): T

  final override fun objectMapper(): ObjectMapper = objectMapper
}
