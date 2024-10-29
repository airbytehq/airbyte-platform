package io.airbyte.mappers.transformations

import io.airbyte.config.MapperConfig
import io.airbyte.config.adapters.AirbyteRecord

abstract class FilteredRecordsMapper<T : MapperConfig> : Mapper<T> {
  final override fun map(
    config: T,
    record: AirbyteRecord,
  ) {
    if (!record.shouldInclude()) {
      return
    }
    mapForNonDiscardedRecords(config, record)
  }

  abstract fun mapForNonDiscardedRecords(
    config: T,
    record: AirbyteRecord,
  )
}
