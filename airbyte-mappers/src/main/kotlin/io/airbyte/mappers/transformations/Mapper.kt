package io.airbyte.mappers.transformations

import io.airbyte.config.MapperConfig
import io.airbyte.config.adapters.AirbyteRecord

interface Mapper<T : MapperConfig> {
  val name: String

  fun spec(): MapperSpec<T>

  fun schema(
    config: T,
    slimStream: SlimStream,
  ): SlimStream

  fun map(
    config: T,
    record: AirbyteRecord,
  )
}
