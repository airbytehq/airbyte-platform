/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.application

import io.airbyte.config.MapperConfig
import io.airbyte.mappers.adapters.AirbyteRecord
import io.airbyte.mappers.transformations.Mapper
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import kotlin.time.DurationUnit
import kotlin.time.measureTime

private val log = KotlinLogging.logger {}

@Singleton
class RecordMapper(
  mappers: List<Mapper<out MapperConfig>>,
) {
  private data class MapperStopwatch(
    val mapper: Mapper<out MapperConfig>,
    var executionCount: Int = 0,
    var totalTimeMs: Long = 0L,
  )

  private val mappersByName: Map<String, MapperStopwatch> = mappers.map { MapperStopwatch(mapper = it) }.associateBy { it.mapper.name }

  @Suppress("UNCHECKED_CAST")
  fun <T : MapperConfig> applyMappers(
    record: AirbyteRecord,
    configuredMappers: List<T>,
  ) {
    try {
      configuredMappers.fold(record) { acc, mapperConfig ->
        mappersByName[mapperConfig.name()]?.let { stopwatch ->
          stopwatch.executionCount++
          stopwatch.totalTimeMs +=
            measureTime {
              (stopwatch.mapper as Mapper<T>).map(mapperConfig, acc)
            }.toLong(DurationUnit.MILLISECONDS)
        }
        acc
      }
    } catch (e: Exception) {
      log.debug { "Error applying mappers: ${e.message}" }
    }
  }

  fun collectStopwatches(): Map<String, Long> =
    mappersByName
      .filterValues { it.executionCount > 0 }
      .map { Pair(it.key, it.value.totalTimeMs) }
      .toMap()
}
