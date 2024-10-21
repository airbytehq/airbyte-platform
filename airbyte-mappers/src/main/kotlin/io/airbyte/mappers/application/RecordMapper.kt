package io.airbyte.mappers.application

import io.airbyte.commons.timer.Stopwatch
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.adapters.AirbyteRecord
import io.airbyte.mappers.transformations.Mapper
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

@Singleton
class RecordMapper(
  mappers: List<Mapper>,
) {
  private data class MapperStopwatch(
    val mapper: Mapper,
    val stopwatch: Stopwatch = Stopwatch(),
  )

  private val mappersByName: Map<String, MapperStopwatch> = mappers.map { MapperStopwatch(it) }.associateBy { it.mapper.name }

  fun applyMappers(
    record: AirbyteRecord,
    configuredMappers: List<ConfiguredMapper>,
  ) {
    try {
      configuredMappers.fold(record) { acc, configuredMapper ->
        mappersByName[configuredMapper.name]?.let { (mapper, stopwatch) ->
          stopwatch.time {
            mapper.map(configuredMapper, acc)
          }
        }
        acc
      }
    } catch (e: Exception) {
      log.debug { "Error applying mappers: ${e.message}" }
    }
  }

  fun collectStopwatches(): Map<String, Stopwatch> =
    mappersByName
      .filterValues { it.stopwatch.getExecutionCount() > 0 }
      .map { Pair(it.key, it.value.stopwatch) }
      .toMap()
}
