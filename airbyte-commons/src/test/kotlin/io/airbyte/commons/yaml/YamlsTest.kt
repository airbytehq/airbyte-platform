/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.yaml

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.yaml.Yamls.serializeWithoutQuotes
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.Objects

internal class YamlsTest {
  @Test
  fun testSerialize() {
    Assertions.assertEquals(
      (
        LINE_BREAK +
          STR_ABC +
          "num: 999\n" +
          "numLong: 888\n"
      ),
      Yamls.serialize<ToClass?>(ToClass(ABC, 999, 888L)),
    )

    Assertions.assertEquals(
      (
        LINE_BREAK +
          "test: \"abc\"\n" +
          "test2: \"def\"\n"
      ),
      Yamls.serialize<ImmutableMap<String?, String?>?>(
        ImmutableMap.of<String?, String?>(
          "test",
          ABC,
          "test2",
          "def",
        ),
      ),
    )
  }

  @Test
  fun testSerializeWithoutQuotes() {
    Assertions.assertEquals(
      (
        LINE_BREAK +
          "str: abc\n" +
          "num: 999\n" +
          "numLong: 888\n"
      ),
      serializeWithoutQuotes(ToClass(ABC, 999, 888L)),
    )

    Assertions.assertEquals(
      (
        LINE_BREAK +
          "test: abc\n" +
          "test2: def\n"
      ),
      serializeWithoutQuotes(
        ImmutableMap.of<String?, String?>(
          "test",
          ABC,
          "test2",
          "def",
        ),
      ),
    )
  }

  @Test
  fun testSerializeJsonNode() {
    Assertions.assertEquals(
      (
        LINE_BREAK +
          STR_ABC +
          "num: 999\n" +
          "numLong: 888\n"
      ),
      Yamls.serialize<JsonNode?>(jsonNode<ToClass?>(ToClass(ABC, 999, 888L))),
    )

    Assertions.assertEquals(
      (
        LINE_BREAK +
          "test: \"abc\"\n" +
          "test2: \"def\"\n"
      ),
      Yamls.serialize<JsonNode?>(
        jsonNode<ImmutableMap<String?, String?>?>(
          ImmutableMap.of<String?, String?>(
            "test",
            ABC,
            "test2",
            "def",
          ),
        ),
      ),
    )
  }

  @Test
  fun testDeserialize() {
    Assertions.assertEquals(
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
    Assertions.assertEquals(
      "{\"str\":\"abc\"}",
      Yamls
        .deserialize(
          LINE_BREAK +
            STR_ABC,
        ).toString(),
    )

    Assertions.assertEquals(
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

    override fun equals(o: Any?): Boolean {
      if (this === o) {
        return true
      }
      if (o == null || javaClass != o.javaClass) {
        return false
      }
      val toClass = o as ToClass
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
