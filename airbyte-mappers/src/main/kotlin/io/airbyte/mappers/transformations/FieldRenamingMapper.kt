package io.airbyte.mappers.transformations

import io.airbyte.config.MapperOperationName
import io.airbyte.config.adapters.AirbyteRecord
import io.airbyte.config.mapper.configs.FieldRenamingMapperConfig
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named("FieldRenamingMapper")
class FieldRenamingMapper : FilteredRecordsMapper<FieldRenamingMapperConfig>() {
  companion object {
    const val ORIGINAL_FIELD_NAME = "originalFieldName"
    const val NEW_FIELD_NAME = "newFieldName"
  }

  private val fieldRenamingMapperSpec = FieldRenamingMapperSpec()

  override val name: String
    get() = MapperOperationName.FIELD_RENAMING

  override fun spec(): MapperSpec<FieldRenamingMapperConfig> {
    return fieldRenamingMapperSpec
  }

  override fun schema(
    config: FieldRenamingMapperConfig,
    slimStream: SlimStream,
  ): SlimStream {
    return slimStream
      .deepCopy()
      .apply { redefineField(config.config.originalFieldName, config.config.newFieldName) }
  }

  override fun mapForNonDiscardedRecords(
    config: FieldRenamingMapperConfig,
    record: AirbyteRecord,
  ) {
    if (record.has(config.config.originalFieldName)) {
      try {
        record.rename(config.config.originalFieldName, config.config.newFieldName)
      } catch (e: Exception) {
        record.trackFieldError(
          config.config.newFieldName,
          AirbyteRecord.Change.NULLED,
          AirbyteRecord.Reason.PLATFORM_SERIALIZATION_ERROR,
        )
      }
    }
  }
}
