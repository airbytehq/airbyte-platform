/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import io.airbyte.config.MapperConfig
import io.airbyte.mappers.adapters.AirbyteRecord

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
