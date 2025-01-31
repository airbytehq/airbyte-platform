/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class EnvUtils {

  private static final Pattern KEY_JSON_VALUE_PATTERN = Pattern.compile("(?<key>[^=]+)\\s*?=\\s*?(?<value>\\{[^=]+})\\s*,?\\s*");
  private static final Pattern KEY_VALUE_PATTERN = Pattern.compile("(?<key>[^=]+)=(?<value>[^=,]+),?");
  private static final String KEY_GROUP_ALIAS = "key";
  private static final String VALUE_GROUP_ALIAS = "value";

  /**
   * Splits key value pairs from the input string into a map. Each kv-pair is separated by a ','.
   * </br>
   * The key and the value are separated by '='.
   * <p>
   * For example:- The following represents two map entries
   * </p>
   * - key1=value1,key2=value2 </br>
   * - key1={key11: value1},key2={key22: value2} </br>
   * - key1={key11: value11, key12: value12},key2={key21: value21, key22: value22}
   *
   * @param input string
   * @return map containing kv pairs
   */
  public static Map<String, String> splitKVPairsFromEnvString(final String input) {
    if (input == null || input.isBlank()) {
      return Map.of();
    }
    final Map<String, String> jsonValuesMatchResult = match(input, KEY_JSON_VALUE_PATTERN);
    return jsonValuesMatchResult.isEmpty()
        ? getKVPairsMatchedWithSimplePattern(input)
        : jsonValuesMatchResult;
  }

  private static Map<String, String> match(final String input, final Pattern pattern) {
    final Matcher matcher = pattern.matcher(input);
    final Map<String, String> kvResult = new HashMap<>();
    while (matcher.find()) {
      kvResult.put(matcher.group(KEY_GROUP_ALIAS).trim(), matcher.group(VALUE_GROUP_ALIAS).trim());
    }
    return kvResult;
  }

  private static Map<String, String> getKVPairsMatchedWithSimplePattern(final String input) {
    final Map<String, String> stringMatchResult = match(input, KEY_VALUE_PATTERN);
    if (stringMatchResult.isEmpty()) {
      log.warn("No valid key value pairs found in the input string: {}", input);
      return Collections.emptyMap();
    }
    return stringMatchResult;
  }

}
