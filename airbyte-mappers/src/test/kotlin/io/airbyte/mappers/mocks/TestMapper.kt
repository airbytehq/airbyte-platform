package io.airbyte.mappers.mocks

import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.config.MapperSpecification
import io.airbyte.mappers.transformations.Mapper
import io.airbyte.mappers.transformations.Record

class TestMapper : Mapper {
  override val name: String = "test"

  override fun spec(): MapperSpecification = MapperSpecification("test", "", mapOf())

  override fun schema(
    config: ConfiguredMapper,
    streamFields: List<Field>,
  ): List<Field> =
    streamFields.map {
      it.copy(it.name + "_test", FieldType.STRING)
    }

  override fun map(
    config: ConfiguredMapper,
    record: Record,
  ) {
    record.data.properties().forEach {
      record.data.putIfAbsent(it.key + "_test", it.value)
      record.data.remove(it.key)
    }
  }
}
