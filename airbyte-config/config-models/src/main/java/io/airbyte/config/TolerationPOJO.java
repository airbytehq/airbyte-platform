/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a minimal {@link io.fabric8.kubernetes.api.model.Toleration}.
 */
@SuppressWarnings("PMD.ShortVariable")
public class TolerationPOJO {

  private static final Logger LOGGER = LoggerFactory.getLogger(TolerationPOJO.class);

  private final String key;
  private final String effect;
  private final String value;
  private final String operator;

  public TolerationPOJO(final String key, final String effect, final String value, final String operator) {
    this.key = key;
    this.effect = effect;
    this.value = value;
    this.operator = operator;
  }

  public String getKey() {
    return key;
  }

  public String getEffect() {
    return effect;
  }

  public String getValue() {
    return value;
  }

  public String getOperator() {
    return operator;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TolerationPOJO that = (TolerationPOJO) o;
    return Objects.equals(key, that.key) && Objects.equals(effect, that.effect)
        && Objects.equals(value, that.value) && Objects.equals(operator,
            that.operator);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, effect, value, operator);
  }

  /**
   * Returns worker pod tolerations parsed from the provided tolerationsStr. The tolerationsStr
   * represents one or more tolerations.
   * <ul>
   * <li>Tolerations are separated by a `;`
   * <li>Each toleration contains k=v pairs mentioning some/all of key, effect, operator and value and
   * separated by `,`
   * </ul>
   * <p>
   * For example:- The following represents two tolerations, one checking existence and another
   * matching a value
   * <p>
   * key=airbyte-server,operator=Exists,effect=NoSchedule;key=airbyte-server,operator=Equals,value=true,effect=NoSchedule
   *
   * @return list of WorkerKubeToleration parsed from tolerationsStr
   */
  public static List<TolerationPOJO> getJobKubeTolerations(final String tolerationsStr) {
    final Stream<String> tolerations = Strings.isNullOrEmpty(tolerationsStr) ? Stream.of()
        : Splitter.on(";")
            .splitToStream(tolerationsStr)
            .filter(tolerationStr -> !Strings.isNullOrEmpty(tolerationStr));

    return tolerations
        .map(TolerationPOJO::parseToleration)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private static TolerationPOJO parseToleration(final String singleTolerationStr) {
    final Map<String, String> tolerationMap = Splitter.on(",")
        .splitToStream(singleTolerationStr)
        .map(s -> s.split("="))
        .collect(Collectors.toMap(s -> s[0], s -> s[1]));

    if (tolerationMap.containsKey("key") && tolerationMap.containsKey("effect") && tolerationMap.containsKey("operator")) {
      return new TolerationPOJO(
          tolerationMap.get("key"),
          tolerationMap.get("effect"),
          tolerationMap.get("value"),
          tolerationMap.get("operator"));
    } else {
      LOGGER.warn(
          "Ignoring toleration {}, missing one of key,effect or operator",
          singleTolerationStr);
      return null;
    }
  }

}
