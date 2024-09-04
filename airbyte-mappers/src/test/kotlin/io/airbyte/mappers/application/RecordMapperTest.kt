package io.airbyte.mappers.application

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredMapper
import io.airbyte.mappers.mocks.TestMapper
import io.airbyte.mappers.transformations.Record
import io.mockk.spyk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class RecordMapperTest {
  private val mapper = spyk(TestMapper())
  private val recordMapper = RecordMapper(listOf(mapper))

  private val inputRecord = Record(Jsons.jsonNode(mapOf("field1" to "value1")) as ObjectNode)

  @Test
  fun testMapperNoConfig() {
    val copiedRecord = Jsons.clone(inputRecord)

    recordMapper.applyMappers(copiedRecord, listOf())

    assertEquals(copiedRecord, copiedRecord)
  }

  @Test
  fun testMapperWithConfig() {
    val copiedRecord = Jsons.clone(inputRecord)

    recordMapper.applyMappers(
      copiedRecord,
      listOf(
        ConfiguredMapper("test", mapOf()),
        ConfiguredMapper("test", mapOf()),
      ),
    )

    val expectedRecord = Record(Jsons.jsonNode(mapOf("field1_test_test" to "value1")) as ObjectNode)
    assertEquals(expectedRecord, copiedRecord)
  }
}
