/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.application

import io.airbyte.config.StreamDescriptor
import io.airbyte.config.adapters.TestRecordAdapter
import io.airbyte.config.mapper.configs.TEST_MAPPER_NAME
import io.airbyte.config.mapper.configs.TestConfig
import io.airbyte.config.mapper.configs.TestEnums
import io.airbyte.config.mapper.configs.TestMapperConfig
import io.airbyte.mappers.mocks.TestMapper
import io.mockk.spyk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class RecordMapperTest {
  private val mapper = spyk(TestMapper())
  private val recordMapper = RecordMapper(listOf(mapper))

  private val sampleRecord = createRecord(mutableMapOf("field1" to "value1"))

  @Test
  fun testMapperNoConfig() {
    val testRecord = sampleRecord.deepCopy()

    recordMapper.applyMappers(testRecord, listOf())

    assertEquals(sampleRecord.data, testRecord.data)
  }

  @Test
  fun testMapperWithConfig() {
    val testRecord = sampleRecord.deepCopy()

    recordMapper.applyMappers(
      testRecord,
      listOf(
        TestMapperConfig(TEST_MAPPER_NAME, null, null, TestConfig("field1", TestEnums.ONE, "field2")),
        TestMapperConfig(TEST_MAPPER_NAME, null, null, TestConfig("field1_test", TestEnums.ONE, "field2")),
      ),
    )

    val expectedRecord = createRecord(mutableMapOf("field1_test_test" to "value1"))
    assertEquals(expectedRecord.data, testRecord.data)
  }

  fun TestRecordAdapter.deepCopy() = TestRecordAdapter(streamDescriptor = this.streamDescriptor, data = this.data)

  fun createRecord(data: MutableMap<String, Any>) =
    TestRecordAdapter(
      streamDescriptor = StreamDescriptor(),
      data = data,
    )
}
