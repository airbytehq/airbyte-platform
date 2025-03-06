/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.yaml;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class YamlsTest {

  private static final String LINE_BREAK = "---\n";
  private static final String STR_ABC = "str: \"abc\"\n";
  private static final String ABC = "abc";

  @Test
  void testSerialize() {
    assertEquals(
        LINE_BREAK
            + STR_ABC
            + "num: 999\n"
            + "numLong: 888\n",
        Yamls.serialize(new ToClass(ABC, 999, 888L)));

    assertEquals(
        LINE_BREAK
            + "test: \"abc\"\n"
            + "test2: \"def\"\n",
        Yamls.serialize(
            ImmutableMap.of(
                "test", ABC,
                "test2", "def")));
  }

  @Test
  void testSerializeWithoutQuotes() {
    assertEquals(
        LINE_BREAK
            + "str: abc\n"
            + "num: 999\n"
            + "numLong: 888\n",
        Yamls.serializeWithoutQuotes(new ToClass(ABC, 999, 888L)));

    assertEquals(
        LINE_BREAK
            + "test: abc\n"
            + "test2: def\n",
        Yamls.serializeWithoutQuotes(
            ImmutableMap.of(
                "test", ABC,
                "test2", "def")));
  }

  @Test
  void testSerializeJsonNode() {
    assertEquals(
        LINE_BREAK
            + STR_ABC
            + "num: 999\n"
            + "numLong: 888\n",
        Yamls.serialize(Jsons.jsonNode(new ToClass(ABC, 999, 888L))));

    assertEquals(
        LINE_BREAK
            + "test: \"abc\"\n"
            + "test2: \"def\"\n",
        Yamls.serialize(Jsons.jsonNode(ImmutableMap.of(
            "test", ABC,
            "test2", "def"))));
  }

  @Test
  void testDeserialize() {
    assertEquals(
        new ToClass(ABC, 999, 888L),
        Yamls.deserialize(
            LINE_BREAK
                + STR_ABC
                + "num: \"999\"\n"
                + "numLong: \"888\"\n",
            ToClass.class));
  }

  @Test
  void testDeserializeToJsonNode() {
    assertEquals(
        "{\"str\":\"abc\"}",
        Yamls.deserialize(
            LINE_BREAK
                + STR_ABC)
            .toString());

    assertEquals(
        "[{\"str\":\"abc\"},{\"str\":\"abc\"}]",
        Yamls.deserialize(
            LINE_BREAK
                + "- str: \"abc\"\n"
                + "- str: \"abc\"\n")
            .toString());
  }

  private static class ToClass {

    @JsonProperty("str")
    String str;

    @JsonProperty("num")
    Integer num;

    @JsonProperty("numLong")
    long numLong;

    public ToClass() {}

    public ToClass(final String str, final Integer num, final long numLong) {
      this.str = str;
      this.num = num;
      this.numLong = numLong;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final ToClass toClass = (ToClass) o;
      return numLong == toClass.numLong
          && Objects.equals(str, toClass.str)
          && Objects.equals(num, toClass.num);
    }

    @Override
    public int hashCode() {
      return Objects.hash(str, num, numLong);
    }

  }

}
