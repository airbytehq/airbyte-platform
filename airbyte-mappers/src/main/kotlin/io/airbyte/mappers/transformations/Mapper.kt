package io.airbyte.mappers.transformations

import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.Field
import io.airbyte.config.MapperSpecification

interface Mapper {
  val name: String

  fun spec(): MapperSpecification

  fun schema(
    config: ConfiguredMapper,
    streamFields: List<Field>,
  ): List<Field>

  fun map(
    config: ConfiguredMapper,
    record: Record,
  )
}
