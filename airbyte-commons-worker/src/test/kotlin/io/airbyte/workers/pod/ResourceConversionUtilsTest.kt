/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.pod

import io.airbyte.config.ResourceRequirements
import io.fabric8.kubernetes.api.model.Quantity
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class ResourceConversionUtilsTest {
  @Test
  fun `builds fabric resource requirements from airbyte model`() {
    val actualReqs =
      ResourceConversionUtils.buildResourceRequirements(
        ResourceRequirements()
          .withCpuRequest("1")
          .withCpuLimit("2")
          .withMemoryRequest("1000Mi")
          .withMemoryLimit("1Gi"),
      )
    Assertions.assertEquals(Quantity("1"), actualReqs.requests[CPU])
    Assertions.assertEquals(Quantity("2"), actualReqs.limits[CPU])
    Assertions.assertEquals(Quantity("1000Mi"), actualReqs.requests[MEMORY])
    Assertions.assertEquals(Quantity("1Gi"), actualReqs.limits[MEMORY])
  }

  @Test
  fun `builds fabric resource requirements from partial airbyte model`() {
    val actualReqs =
      ResourceConversionUtils.buildResourceRequirements(
        ResourceRequirements()
          .withCpuRequest("5")
          .withMemoryLimit("4Gi"),
      )
    Assertions.assertEquals(Quantity("5"), actualReqs.requests[CPU])
    Assertions.assertNull(actualReqs.limits[CPU])
    Assertions.assertNull(actualReqs.requests[MEMORY])
    Assertions.assertEquals(Quantity("4Gi"), actualReqs.limits[MEMORY])
  }

  @Test
  fun `resolves conflicts in limit and request values`() {
    val actualReqs =
      ResourceConversionUtils.buildResourceRequirements(
        ResourceRequirements()
          .withCpuRequest("1")
          .withCpuLimit("0.5")
          .withMemoryRequest("1000Mi")
          .withMemoryLimit("0.5Gi"),
      )
    Assertions.assertEquals(Quantity("0.5"), actualReqs.requests[CPU])
    Assertions.assertEquals(Quantity("0.5"), actualReqs.limits[CPU])
    Assertions.assertEquals(Quantity("0.5Gi"), actualReqs.requests[MEMORY])
    Assertions.assertEquals(Quantity("0.5Gi"), actualReqs.limits[MEMORY])
  }

  @ParameterizedTest
  @MethodSource("kubeQuantityStringBytesMatrix")
  fun `converts kube quantity strings to number of bytes long`(
    input: String,
    expected: Long,
  ) {
    val result = ResourceConversionUtils.kubeQuantityStringToBytes(input)

    Assertions.assertEquals(expected, result)
  }

  companion object {
    private const val CPU = "cpu"
    private const val MEMORY = "memory"

    @JvmStatic
    private fun kubeQuantityStringBytesMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of("1G", 1000000000),
        Arguments.of("2Mi", 2097152),
        Arguments.of("5G", 5000000000),
        Arguments.of("10G", 10000000000),
        Arguments.of("87Ki", 89088),
      )
    }
  }
}
