/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.model

import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.EnvVarSource
import io.fabric8.kubernetes.api.model.SecretKeySelector
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

class MapEnvVarExtensionsTest {
  @ParameterizedTest
  @MethodSource("envVarMatrix")
  fun `converts maps of string keys and values to fabric8 EnvVar`(envMap: Map<String, String>) {
    val result = envMap.toEnvVarList()

    assertEquals(envMap.size, result.size)
    envMap.entries.forEach {
      assertTrue(result.contains(EnvVar(it.key, it.value, null)))
    }
  }

  @ParameterizedTest
  @MethodSource("refEnvVarMatrix")
  fun `converts maps of string key and EnvVarSource values to fabric8 EnvVar`(envMap: Map<String, EnvVarSource>) {
    val result = envMap.toRefEnvVarList()

    assertEquals(envMap.size, result.size)
    envMap.entries.forEach {
      assertTrue(result.contains(EnvVar(it.key, null, it.value)))
    }
  }

  companion object {
    @JvmStatic
    private fun envVarMatrix() =
      listOf(
        Arguments.of(mapOf("cat" to "dog", "asdf" to "ghjk")),
        Arguments.of(mapOf("asdf" to "ghjk")),
        Arguments.of(mapOf<String, String>()),
      )

    @JvmStatic
    private fun refEnvVarMatrix() =
      listOf(
        Arguments.of(
          mapOf(
            "cat" to
              EnvVarSource().apply {
                secretKeyRef =
                  SecretKeySelector().apply {
                    name = "foo"
                    key = "barr"
                  }
              },
            "asdf" to
              EnvVarSource().apply {
                secretKeyRef =
                  SecretKeySelector().apply {
                    name = "baz"
                    key = "buzz"
                  }
              },
          ),
        ),
        Arguments.of(
          mapOf(
            "asdf" to
              EnvVarSource().apply {
                secretKeyRef =
                  SecretKeySelector().apply {
                    name = "baz"
                    key = "buzz"
                  }
              },
          ),
        ),
        Arguments.of(mapOf<String, EnvVarSource>()),
      )
  }
}
