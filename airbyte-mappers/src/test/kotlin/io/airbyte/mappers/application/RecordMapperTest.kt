package io.airbyte.mappers.application

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.adapters.AirbyteJsonRecordAdapter
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
        ConfiguredMapper("test", mapOf("target" to "field1")),
        ConfiguredMapper("test", mapOf("target" to "field1_test")),
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
