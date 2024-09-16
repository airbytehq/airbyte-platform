package io.airbyte.mappers.mocks

import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.FieldType
import io.airbyte.config.MapperSpecification
import io.airbyte.config.adapters.AirbyteRecord
import io.airbyte.mappers.transformations.Mapper
import io.airbyte.mappers.transformations.SlimStream

class TestMapper : Mapper {
  override val name: String = "test"

  override fun spec(): MapperSpecification = MapperSpecification("test", "", mapOf())

  override fun schema(
    config: ConfiguredMapper,
    slimStream: SlimStream,
  ): SlimStream =
    slimStream.deepCopy()
      .apply {
        fields.forEach { field -> redefineField(field.name, "${field.name}_test", FieldType.STRING) }
      }

  override fun map(
    config: ConfiguredMapper,
    record: AirbyteRecord,
  ) {
    val targetField = config.config["target"] ?: throw IllegalArgumentException("target is not defined")
    record.set("${targetField}_test", record.get(targetField).asString())
    record.remove(targetField)
  }
}

class FailingTestMapper : Mapper {
  override val name: String = "test"

  override fun spec(): MapperSpecification = MapperSpecification("test", "", mapOf())

  override fun schema(
    config: ConfiguredMapper,
    slimStream: SlimStream,
  ): SlimStream = throw RuntimeException("Failed to generate schema")

  override fun map(
    config: ConfiguredMapper,
    record: AirbyteRecord,
  ) = throw RuntimeException("Failed to map record")
}
