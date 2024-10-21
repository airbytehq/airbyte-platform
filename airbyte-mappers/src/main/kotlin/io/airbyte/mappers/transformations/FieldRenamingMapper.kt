package io.airbyte.mappers.transformations

import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.MapperOperationName
import io.airbyte.config.MapperSpecification
import io.airbyte.config.MapperSpecificationFieldString
import io.airbyte.config.adapters.AirbyteRecord
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named("FieldRenamingMapper")
class FieldRenamingMapper : Mapper {
  companion object {
    const val ORIGINAL_FIELD_NAME = "originalFieldName"
    const val NEW_FIELD_NAME = "newFieldName"
  }

  override val name: String
    get() = MapperOperationName.FIELD_RENAMING

  override fun spec(): MapperSpecification {
    return MapperSpecification(
      name = name,
      documentationUrl = "",
      config =
        mapOf(
          ORIGINAL_FIELD_NAME to
            MapperSpecificationFieldString(
              title = "Original Field Name",
              description = "The current name of the field to rename.",
            ),
          NEW_FIELD_NAME to
            MapperSpecificationFieldString(
              title = "New Field Name",
              description = "The new name for the field after renaming.",
            ),
        ),
    )
  }

  override fun schema(
    config: ConfiguredMapper,
    slimStream: SlimStream,
  ): SlimStream {
    val (originalFieldName, newFieldName) = getConfigValues(config.config)

    return slimStream
      .deepCopy()
      .apply { redefineField(originalFieldName, newFieldName) }
  }

  private fun getConfigValues(config: Map<String, String>): FieldRenamingConfig {
    val originalFieldName =
      config[ORIGINAL_FIELD_NAME]
        ?: throw IllegalArgumentException("Config missing required key: $ORIGINAL_FIELD_NAME")
    val newFieldName =
      config[NEW_FIELD_NAME]
        ?: throw IllegalArgumentException("Config missing required key: $NEW_FIELD_NAME")
    return FieldRenamingConfig(originalFieldName, newFieldName)
  }

  override fun map(
    config: ConfiguredMapper,
    record: AirbyteRecord,
  ) {
    val (originalFieldName, newFieldName) = getConfigValues(config.config)

    if (record.has(originalFieldName)) {
      try {
        record.rename(originalFieldName, newFieldName)
      } catch (e: Exception) {
        record.trackFieldError(
          newFieldName,
          AirbyteRecord.Change.NULLED,
          AirbyteRecord.Reason.PLATFORM_SERIALIZATION_ERROR,
        )
      }
    }
  }

  data class FieldRenamingConfig(
    val originalFieldName: String,
    val newFieldName: String,
  )
}
