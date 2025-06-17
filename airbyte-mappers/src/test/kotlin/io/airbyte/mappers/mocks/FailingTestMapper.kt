/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.mocks

import io.airbyte.config.mapper.configs.TestMapperConfig
import io.airbyte.mappers.adapters.AirbyteRecord
import io.airbyte.mappers.transformations.SlimStream

class FailingTestMapper : TestMapper() {
  override fun schema(
    config: TestMapperConfig,
    slimStream: SlimStream,
  ): SlimStream = throw RuntimeException("Failed to generate schema")

  override fun mapForNonDiscardedRecords(
    config: TestMapperConfig,
    record: AirbyteRecord,
  ) = throw RuntimeException(
    "Failed to map record",
  )
}
