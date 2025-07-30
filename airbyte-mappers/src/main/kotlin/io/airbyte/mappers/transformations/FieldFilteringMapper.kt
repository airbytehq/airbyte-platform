/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.MapperOperationName
import io.airbyte.config.mapper.configs.FieldFilteringMapperConfig
import io.airbyte.mappers.adapters.AirbyteRecord
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named("FieldFilteringMapper")
class FieldFilteringMapper(
  objectMapper: ObjectMapper,
) : FilteredRecordsMapper<FieldFilteringMapperConfig>() {
  override fun mapForNonDiscardedRecords(
    config: FieldFilteringMapperConfig,
    record: AirbyteRecord,
  ) {
    if (record.has(config.config.targetField)) {
      record.remove(config.config.targetField)
    }
  }

  private val fieldFilteringMapperSpec = FieldFilteringMapperSpec(objectMapper)

  override val name: String
    get() = MapperOperationName.FIELD_FILTERING

  override fun spec(): MapperSpec<FieldFilteringMapperConfig> = fieldFilteringMapperSpec

  override fun schema(
    config: FieldFilteringMapperConfig,
    slimStream: SlimStream,
  ): SlimStream =
    slimStream
      .deepCopy()
      .apply { removeField(config.config.targetField) }
}
