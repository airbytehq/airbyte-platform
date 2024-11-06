/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class EnvUtilsTest {

  @ParameterizedTest
  @MethodSource("splitKVPairsFromEnvString")
  void splitKVPairsFromEnvString(String input, Map<String, String> expected) {
    final Map<String, String> result = EnvUtils.splitKVPairsFromEnvString(input);
    assertThat(result).isEqualTo(expected);
  }

  private static Stream<Arguments> splitKVPairsFromEnvString() {
    return Stream.of(
        // unmatched
        Arguments.of("key1", Collections.emptyMap()),
        Arguments.of("key1,value", Collections.emptyMap()),
        Arguments.of("key1-value", Collections.emptyMap()),
        Arguments.of("key1:value", Collections.emptyMap()),
        // matched k:v pairs
        Arguments.of("key1=value1", Map.of("key1", "value1")),
        Arguments.of("key1 = value1", Map.of("key1", "value1")),
        Arguments.of("key1=value1,key2=value2", Map.of("key1", "value1", "key2", "value2")),
        Arguments.of("key1   = value1, key2 =   value2", Map.of("key1", "value1", "key2", "value2")),
        // matched k:jsonV pairs
        Arguments.of("key1={value1}", Map.of("key1", "{value1}")),
        Arguments.of("key1={  value1  }", Map.of("key1", "{  value1  }")),
        Arguments.of("key1  =  {  value1  }", Map.of("key1", "{  value1  }")),
        Arguments.of("key1={value1},key2={value2}", Map.of("key1", "{value1}", "key2", "{value2}")),
        Arguments.of("key1=  {value1}  ,  key2={value2}", Map.of("key1", "{value1}", "key2", "{value2}")),
        Arguments.of("key1=  {value1  }  ,  key2=  {  value2}", Map.of("key1", "{value1  }", "key2", "{  value2}")),
        Arguments.of("key1={key11: value11},key2={key22: value22}", Map.of(
            "key1", "{key11: value11}",
            "key2", "{key22: value22}")),
        Arguments.of("key1={key11: value11, key12: value12},key2={key21: value21, key22: value22}", Map.of(
            "key1", "{key11: value11, key12: value12}",
            "key2", "{key21: value21, key22: value22}")),
        Arguments.of("key1={key11: value11, key12: value12, key13: {key131: value131}},"
            + "key2={key21: value21, key22: value22, key23: {key231: value231}}",
            Map.of(
                "key1", "{key11: value11, key12: value12, key13: {key131: value131}}",
                "key2", "{key21: value21, key22: value22, key23: {key231: value231}}")));
  }

}
