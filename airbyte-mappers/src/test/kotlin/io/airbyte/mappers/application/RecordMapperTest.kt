package io.airbyte.mappers.application

import io.airbyte.commons.json.Jsons
import io.airbyte.config.adapters.AirbyteJsonRecordAdapter
import io.airbyte.config.mapper.configs.TEST_MAPPER_NAME
import io.airbyte.config.mapper.configs.TestConfig
import io.airbyte.config.mapper.configs.TestEnums
import io.airbyte.config.mapper.configs.TestMapperConfig
import io.airbyte.mappers.mocks.TestMapper
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteRecordMessage
import io.mockk.spyk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class RecordMapperTest {
  private val mapper = spyk(TestMapper())
  private val recordMapper = RecordMapper(listOf(mapper))

  private val sampleRecord = createRecord(mapOf("field1" to "value1"))

  @Test
  fun testMapperNoConfig() {
    val testRecord = sampleRecord.deepCopy()

    recordMapper.applyMappers(testRecord, listOf())

    assertEquals(sampleRecord, testRecord)
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

    val expectedRecord = createRecord(mapOf("field1_test_test" to "value1"))
    assertEquals(expectedRecord, testRecord)
  }

  fun createRecord(data: Map<String, String>) =
    AirbyteJsonRecordAdapter(
      AirbyteMessage()
        .withType(AirbyteMessage.Type.RECORD)
        .withRecord(AirbyteRecordMessage().withStream("stream").withData(Jsons.jsonNode(data))),
    )

  fun AirbyteJsonRecordAdapter.deepCopy() = this.copy(message = Jsons.clone(this.asProtocol))
}
