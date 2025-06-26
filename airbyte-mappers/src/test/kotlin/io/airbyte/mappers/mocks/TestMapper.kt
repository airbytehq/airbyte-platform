/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.mocks

import TEST_OBJECT_MAPPER
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.FieldType
import io.airbyte.config.mapper.configs.TEST_MAPPER_NAME
import io.airbyte.config.mapper.configs.TestMapperConfig
import io.airbyte.mappers.adapters.AirbyteRecord
import io.airbyte.mappers.transformations.ConfigValidatingSpec
import io.airbyte.mappers.transformations.FilteredRecordsMapper
import io.airbyte.mappers.transformations.MapperSpec
import io.airbyte.mappers.transformations.SimpleJsonSchemaGeneratorFromSpec
import io.airbyte.mappers.transformations.SlimStream
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named("TestMapper")
open class TestMapper : FilteredRecordsMapper<TestMapperConfig>() {
  override fun schema(
    config: TestMapperConfig,
    slimStream: SlimStream,
  ): SlimStream =
    slimStream
      .deepCopy()
      .apply {
        fields.forEach { field -> redefineField(field.name, "${field.name}_test", FieldType.STRING) }
      }

  override fun mapForNonDiscardedRecords(
    config: TestMapperConfig,
    record: AirbyteRecord,
  ) {
    record.set("${config.config.field1}_test", record.get(config.config.field1).asString())
    record.remove(config.config.field1)
  }

  override val name: String
    get() = TEST_MAPPER_NAME

  override fun spec(): MapperSpec<TestMapperConfig> = TestMapperSpec()
}

class TestMapperSpec : ConfigValidatingSpec<TestMapperConfig>(TEST_OBJECT_MAPPER) {
  private val simpleJsonSchemaGeneratorFromSpec: SimpleJsonSchemaGeneratorFromSpec = SimpleJsonSchemaGeneratorFromSpec()

  override fun deserializeVerifiedConfig(configuredMapper: ConfiguredMapper): TestMapperConfig =
    objectMapper().convertValue(configuredMapper, TestMapperConfig::class.java)

  override fun jsonSchema(): JsonNode = simpleJsonSchemaGeneratorFromSpec.generateJsonSchema(specType())

  override fun specType(): Class<*> = TestMapperConfig::class.java
}
