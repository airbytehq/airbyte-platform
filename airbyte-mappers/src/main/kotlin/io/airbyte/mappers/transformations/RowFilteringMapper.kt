/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.MapperOperationName.ROW_FILTERING
import io.airbyte.config.mapper.configs.AndOperation
import io.airbyte.config.mapper.configs.EqualOperation
import io.airbyte.config.mapper.configs.NotOperation
import io.airbyte.config.mapper.configs.Operation
import io.airbyte.config.mapper.configs.OrOperation
import io.airbyte.config.mapper.configs.RowFilteringMapperConfig
import io.airbyte.mappers.adapters.AirbyteRecord
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Named("RowFilteringMapper")
class RowFilteringMapper(
  private val objectMapper: ObjectMapper,
) : FilteredRecordsMapper<RowFilteringMapperConfig>() {
  private val rowFilteringMapperSpec = RowFilteringMapperSpec(objectMapper)
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
    val conditionEvalResult = eval(operation = config.config.conditions, record = record)
    record.setInclude(conditionEvalResult)
  }

  private fun eval(
    operation: Operation,
    record: AirbyteRecord,
  ): Boolean =
    when (operation) {
      is AndOperation -> operation.conditions.all { eval(it, record) }
      is EqualOperation -> {
        if (record.has(operation.fieldName)) {
          record.get(operation.fieldName).asString() == operation.comparisonValue
        } else {
          false
        }
      }
      is NotOperation -> operation.conditions.none { eval(operation = it, record = record) }
      is OrOperation -> operation.conditions.any { eval(it, record) }
    }
}
