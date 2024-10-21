package io.airbyte.mappers.transformations

import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.MapperSpecification
import io.airbyte.config.adapters.AirbyteRecord

interface Mapper {
  val name: String

  fun spec(): MapperSpecification

  fun schema(
    config: ConfiguredMapper,
    slimStream: SlimStream,
  ): SlimStream

  fun map(
    config: ConfiguredMapper,
    record: AirbyteRecord,
  )
}
