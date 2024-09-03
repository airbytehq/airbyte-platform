package io.airbyte.mappers.application

import io.airbyte.config.ConfiguredMapper
import io.airbyte.mappers.transformations.Mapper
import io.airbyte.mappers.transformations.Record
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

@Singleton
class RecordMapper(private val mappers: List<Mapper>) {
  private val mappersByName: Map<String, Mapper> = mappers.associateBy { it.name }

  fun applyMappers(
    record: Record,
    configuredMappers: List<ConfiguredMapper>,
  ) {
    try {
      configuredMappers.fold(record) { acc, configuredMapper ->
        val mapper =
          mappersByName[configuredMapper.name]

        if (mapper != null) {
          mapper.map(configuredMapper, acc)
        }
        acc
      }
    } catch (e: Exception) {
      log.debug { "Error applying mappers: ${e.message}" }
    }
  }
}
