package client.infrastructure

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.TextNode
import io.airbyte.api.client.infrastructure.JsonNodeAdapter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class JsonNodeAdapterTest {
  val jsonNode: JsonNode = JsonNodeFactory.instance.objectNode().set("key1", TextNode.valueOf("value1"))
  val jsonString = "{\"key1\":\"value1\"}"
  val adapter = JsonNodeAdapter()

  @Test
  fun toJson() {
    assertEquals(jsonString, adapter.toJson(jsonNode))
  }

  @Test
  fun fromJson() {
    assertEquals(jsonNode, adapter.fromJson(jsonString))
  }
}
