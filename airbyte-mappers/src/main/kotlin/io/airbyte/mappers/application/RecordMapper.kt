package io.airbyte.mappers.application

import io.airbyte.commons.timer.Stopwatch
import io.airbyte.config.MapperConfig
import io.airbyte.config.adapters.AirbyteRecord
import io.airbyte.mappers.transformations.Mapper
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

@Singleton
class RecordMapper(mappers: List<Mapper<out MapperConfig>>) {
  private data class MapperStopwatch(val mapper: Mapper<out MapperConfig>, val stopwatch: Stopwatch = Stopwatch())

  private val mappersByName: Map<String, MapperStopwatch> = mappers.map { MapperStopwatch(it) }.associateBy { it.mapper.name }

  @Suppress("UNCHECKED_CAST")
  fun <T : MapperConfig> applyMappers(
    record: AirbyteRecord,
    configuredMappers: List<T>,
  ) {
    try {
      configuredMappers.fold(record) { acc, mapperConfig ->
        mappersByName[mapperConfig.name()]?.let { (mapper, stopwatch) ->
          stopwatch.time {
            (mapper as Mapper<T>).map(mapperConfig, acc)
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
      .map { Pair(it.key, it.value.stopwatch) }.toMap()
}
