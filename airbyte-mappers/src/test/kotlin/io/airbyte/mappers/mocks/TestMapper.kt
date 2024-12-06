package io.airbyte.mappers.mocks

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.FieldType
import io.airbyte.config.adapters.AirbyteRecord
import io.airbyte.config.mapper.configs.TEST_MAPPER_NAME
import io.airbyte.config.mapper.configs.TestMapperConfig
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
  ): SlimStream {
    return slimStream.deepCopy()
      .apply {
        fields.forEach { field -> redefineField(field.name, "${field.name}_test", FieldType.STRING) }
      }
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

  override fun spec(): MapperSpec<TestMapperConfig> {
    return TestMapperSpec()
  }
}

class TestMapperSpec : ConfigValidatingSpec<TestMapperConfig>() {
  private val simpleJsonSchemaGeneratorFromSpec: SimpleJsonSchemaGeneratorFromSpec = SimpleJsonSchemaGeneratorFromSpec()

  override fun deserializeVerifiedConfig(configuredMapper: ConfiguredMapper): TestMapperConfig {
    return Jsons.convertValue(configuredMapper, TestMapperConfig::class.java)
  }

  override fun jsonSchema(): JsonNode {
    return simpleJsonSchemaGeneratorFromSpec.generateJsonSchema(specType())
  }

  override fun specType(): Class<*> {
    return TestMapperConfig::class.java
  }
}
