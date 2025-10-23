/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.yaml

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.yaml.Yamls.serializeWithoutQuotes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.Objects

internal class YamlsTest {
  @Test
  fun testSerialize() {
    assertEquals(
      (
        LINE_BREAK +
          STR_ABC +
          "num: 999\n" +
          "numLong: 888\n"
      ),
      Yamls.serialize<ToClass?>(ToClass(ABC, 999, 888L)),
    )

    assertEquals(
      (
        LINE_BREAK +
          "test: \"abc\"\n" +
          "test2: \"def\"\n"
      ),
      Yamls.serialize(mapOf("test" to ABC, "test2" to "def")),
    )
  }

  @Test
  fun testSerializeWithoutQuotes() {
    assertEquals(
      (
        LINE_BREAK +
          "str: abc\n" +
          "num: 999\n" +
          "numLong: 888\n"
      ),
      serializeWithoutQuotes(ToClass(ABC, 999, 888L)),
    )

    assertEquals(
      (
        LINE_BREAK +
          "test: abc\n" +
          "test2: def\n"
      ),
      serializeWithoutQuotes(
        mapOf("test" to ABC, "test2" to "def"),
      ),
    )
  }

  @Test
  fun testSerializeJsonNode() {
    assertEquals(
      (
        LINE_BREAK +
          STR_ABC +
          "num: 999\n" +
          "numLong: 888\n"
      ),
      Yamls.serialize<JsonNode?>(jsonNode<ToClass?>(ToClass(ABC, 999, 888L))),
    )

    assertEquals(
      (
        LINE_BREAK +
          "test: \"abc\"\n" +
          "test2: \"def\"\n"
      ),
      Yamls.serialize<JsonNode?>(
        jsonNode(
          mapOf("test" to ABC, "test2" to "def"),
        ),
      ),
    )
  }

  @Test
  fun testDeserialize() {
    assertEquals(
      ToClass(ABC, 999, 888L),
      Yamls.deserialize(
        (
          LINE_BREAK +
            STR_ABC +
            "num: \"999\"\n" +
            "numLong: \"888\"\n"
        ),
        ToClass::class.java,
      ),
    )
  }

  @Test
  fun testDeserializeToJsonNode() {
    assertEquals(
      "{\"str\":\"abc\"}",
      Yamls
        .deserialize(
          LINE_BREAK +
            STR_ABC,
        ).toString(),
    )

    assertEquals(
      "[{\"str\":\"abc\"},{\"str\":\"abc\"}]",
      Yamls
        .deserialize(
          (
            LINE_BREAK +
              "- str: \"abc\"\n" +
              "- str: \"abc\"\n"
          ),
        ).toString(),
    )
  }

  private class ToClass {
    @JsonProperty("str")
    var str: String? = null

    @JsonProperty("num")
    var num: Int? = null

    @JsonProperty("numLong")
    var numLong: Long = 0

    constructor()

    constructor(str: String?, num: Int?, numLong: Long) {
      this.str = str
      this.num = num
      this.numLong = numLong
    }

    override fun equals(other: Any?): Boolean {
      if (this === other) {
        return true
      }
      if (other == null || javaClass != other.javaClass) {
        return false
      }
      val toClass = other as ToClass
      return numLong == toClass.numLong &&
        str == toClass.str &&
        num == toClass.num
    }

    override fun hashCode(): Int = Objects.hash(str, num, numLong)
  }

  companion object {
    private const val LINE_BREAK = "---\n"
    private const val STR_ABC = "str: \"abc\"\n"
    private const val ABC = "abc"
  }
}
