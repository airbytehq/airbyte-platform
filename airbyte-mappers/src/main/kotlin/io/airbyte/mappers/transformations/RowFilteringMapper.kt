/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import io.airbyte.config.MapperOperationName.ROW_FILTERING
import io.airbyte.config.adapters.AirbyteRecord
import io.airbyte.config.mapper.configs.RowFilteringMapperConfig
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named("RowFilteringMapper")
class RowFilteringMapper : FilteredRecordsMapper<RowFilteringMapperConfig>() {
  private val rowFilteringMapperSpec = RowFilteringMapperSpec()
  override val name: String
    get() = ROW_FILTERING

  override fun spec(): MapperSpec<RowFilteringMapperConfig> = rowFilteringMapperSpec

  override fun schema(
    config: RowFilteringMapperConfig,
    slimStream: SlimStream,
  ): SlimStream = slimStream

  override fun mapForNonDiscardedRecords(
    config: RowFilteringMapperConfig,
    record: AirbyteRecord,
  ) {
    val conditionEvalResult = config.config.conditions.eval(record)
    record.setInclude(conditionEvalResult)
  }
}
