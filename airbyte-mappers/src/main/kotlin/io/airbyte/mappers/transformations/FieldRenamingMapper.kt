/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.MapperOperationName
import io.airbyte.config.mapper.configs.FieldRenamingMapperConfig
import io.airbyte.mappers.adapters.AirbyteRecord
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named("FieldRenamingMapper")
class FieldRenamingMapper(
  private val objectMapper: ObjectMapper,
) : FilteredRecordsMapper<FieldRenamingMapperConfig>() {
  companion object {
    const val ORIGINAL_FIELD_NAME = "originalFieldName"
    const val NEW_FIELD_NAME = "newFieldName"
  }

  private val fieldRenamingMapperSpec = FieldRenamingMapperSpec(objectMapper)

  override val name: String
    get() = MapperOperationName.FIELD_RENAMING

  override fun spec(): MapperSpec<FieldRenamingMapperConfig> = fieldRenamingMapperSpec

  override fun schema(
    config: FieldRenamingMapperConfig,
    slimStream: SlimStream,
  ): SlimStream =
    slimStream
      .deepCopy()
      .apply { redefineField(config.config.originalFieldName, config.config.newFieldName) }

  override fun mapForNonDiscardedRecords(
    config: FieldRenamingMapperConfig,
    record: AirbyteRecord,
  ) {
    if (record.has(config.config.originalFieldName)) {
      try {
        record.rename(config.config.originalFieldName, config.config.newFieldName)
      } catch (_: Exception) {
        record.trackFieldError(
          config.config.newFieldName,
          AirbyteRecord.Change.NULLED,
          AirbyteRecord.Reason.PLATFORM_SERIALIZATION_ERROR,
        )
      }
    }
  }
}
