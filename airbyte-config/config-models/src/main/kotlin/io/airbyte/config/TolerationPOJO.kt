/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.google.common.base.Splitter
import com.google.common.base.Strings
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.Objects
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * Represents a minimal [io.fabric8.kubernetes.api.model.Toleration].
 */
data class TolerationPOJO(
  val key: String?,
  val effect: String?,
  val value: String?,
  val operator: String?,
) {
  companion object {
    private val log = KotlinLogging.logger {}

    /**
     * Returns worker pod tolerations parsed from the provided tolerationsStr. The tolerationsStr
     * represents one or more tolerations.
     *
     *  * Tolerations are separated by a `;`
     *  * Each toleration contains k=v pairs mentioning some/all of key, effect, operator and value and
     * separated by `,`
     *
     *
     *
     * For example:- The following represents two tolerations, one checking existence and another
     * matching a value
     *
     *
     * key=airbyte-server,operator=Exists,effect=NoSchedule;key=airbyte-server,operator=Equals,value=true,effect=NoSchedule
     *
     * @return list of WorkerKubeToleration parsed from tolerationsStr
     */
    @JvmStatic
    fun getJobKubeTolerations(tolerationsStr: String?): List<TolerationPOJO> {
      val tolerations =
        if (Strings.isNullOrEmpty(tolerationsStr)) {
          Stream.of()
        } else {
          Splitter
            .on(";")
            .splitToStream(tolerationsStr)
            .filter { tolerationStr: String? -> !Strings.isNullOrEmpty(tolerationStr) }
        }

      return tolerations
        .map { singleTolerationStr: String -> parseToleration(singleTolerationStr) }
        .filter { obj: TolerationPOJO? -> Objects.nonNull(obj) }
        .collect(Collectors.toList()) as List<TolerationPOJO>
    }

    private fun parseToleration(singleTolerationStr: String): TolerationPOJO? {
      val tolerationMap =
        Splitter
          .on(",")
          .splitToStream(singleTolerationStr)
          .map { s: String -> s.split("=".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray() }
          .collect(
            Collectors.toMap(
              { s: Array<String> -> s[0] },
              { s: Array<String> -> s[1] },
            ),
          )

      if (tolerationMap.containsKey("key") && tolerationMap.containsKey("effect") && tolerationMap.containsKey("operator")) {
        return TolerationPOJO(
          tolerationMap["key"],
          tolerationMap["effect"],
          tolerationMap["value"],
          tolerationMap["operator"],
        )
      } else {
        log.warn(
          "Ignoring toleration {}, missing one of key,effect or operator",
          singleTolerationStr,
        )
        return null
      }
    }
  }
}
