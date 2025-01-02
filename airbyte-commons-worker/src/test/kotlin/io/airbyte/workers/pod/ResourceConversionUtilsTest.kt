/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.pod

import io.airbyte.config.ResourceRequirements
import io.airbyte.workers.pod.PodConstants.CPU_RESOURCE_KEY
import io.airbyte.workers.pod.PodConstants.MEMORY_RESOURCE_KEY
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
      ResourceConversionUtils.domainToApi(
        ResourceRequirements()
          .withCpuRequest("1")
          .withCpuLimit("2")
          .withMemoryRequest("1000Mi")
          .withMemoryLimit("1Gi"),
      )
    Assertions.assertEquals(Quantity("1"), actualReqs.requests[CPU_RESOURCE_KEY])
    Assertions.assertEquals(Quantity("2"), actualReqs.limits[CPU_RESOURCE_KEY])
    Assertions.assertEquals(Quantity("1000Mi"), actualReqs.requests[MEMORY_RESOURCE_KEY])
    Assertions.assertEquals(Quantity("1Gi"), actualReqs.limits[MEMORY_RESOURCE_KEY])
  }

  @Test
  fun `builds fabric resource requirements from partial airbyte model`() {
    val actualReqs =
      ResourceConversionUtils.domainToApi(
        ResourceRequirements()
          .withCpuRequest("5")
          .withMemoryLimit("4Gi"),
      )
    Assertions.assertEquals(Quantity("5"), actualReqs.requests[CPU_RESOURCE_KEY])
    Assertions.assertNull(actualReqs.limits[CPU_RESOURCE_KEY])
    Assertions.assertNull(actualReqs.requests[MEMORY_RESOURCE_KEY])
    Assertions.assertEquals(Quantity("4Gi"), actualReqs.limits[MEMORY_RESOURCE_KEY])
  }

  @Test
  fun `resolves conflicts in limit and request values`() {
    val actualReqs =
      ResourceConversionUtils.domainToApi(
        ResourceRequirements()
          .withCpuRequest("1")
          .withCpuLimit("0.5")
          .withMemoryRequest("1000Mi")
          .withMemoryLimit("0.5Gi"),
      )
    Assertions.assertEquals(Quantity("0.5"), actualReqs.requests[CPU_RESOURCE_KEY])
    Assertions.assertEquals(Quantity("0.5"), actualReqs.limits[CPU_RESOURCE_KEY])
    Assertions.assertEquals(Quantity("0.5Gi"), actualReqs.requests[MEMORY_RESOURCE_KEY])
    Assertions.assertEquals(Quantity("0.5Gi"), actualReqs.limits[MEMORY_RESOURCE_KEY])
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

  @ParameterizedTest
  @MethodSource("resourceMatrix")
  fun `sums resources specified in the kube quantity DSL`(
    reqs1: ResourceRequirements,
    reqs2: ResourceRequirements,
    expected: ResourceRequirements,
  ) {
    val result = ResourceConversionUtils.sumResourceRequirements(reqs1, reqs2)

    Assertions.assertEquals(expected, result)
  }

  companion object {
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

    @JvmStatic
    private fun resourceMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(
          ResourceRequirements()
            .withCpuRequest("0.5")
            .withCpuLimit("1")
            .withMemoryRequest("100Mi")
            .withMemoryLimit("0.5Gi"),
          ResourceRequirements()
            .withCpuRequest("0.5")
            .withCpuLimit("0.5")
            .withMemoryRequest("1000Mi")
            .withMemoryLimit("1Gi"),
          ResourceRequirements()
            .withCpuRequest("1")
            .withCpuLimit("1.5")
            .withMemoryRequest("1100Mi")
            .withMemoryLimit("1.5Gi"),
        ),
        Arguments.of(
          ResourceRequirements()
            .withCpuRequest("2")
            .withCpuLimit("2")
            .withMemoryRequest("600Mi")
            .withMemoryLimit("600Mi"),
          ResourceRequirements()
            .withCpuRequest("0.5")
            .withCpuLimit("0.5")
            .withMemoryRequest("500Mi")
            .withMemoryLimit("500Mi")
            .withEphemeralStorageRequest("1G")
            .withEphemeralStorageLimit("1G"),
          ResourceRequirements()
            .withCpuRequest("2.5")
            .withCpuLimit("2.5")
            .withMemoryRequest("1100Mi")
            .withMemoryLimit("1100Mi")
            .withEphemeralStorageRequest("1G")
            .withEphemeralStorageLimit("1G"),
        ),
        Arguments.of(
          ResourceRequirements(),
          ResourceRequirements()
            .withCpuRequest("0.5")
            .withCpuLimit("0.5")
            .withMemoryRequest("500Mi")
            .withMemoryLimit("500Mi")
            .withEphemeralStorageRequest("1G")
            .withEphemeralStorageLimit("1G"),
          ResourceRequirements()
            .withCpuRequest("0.5")
            .withCpuLimit("0.5")
            .withMemoryRequest("500Mi")
            .withMemoryLimit("500Mi")
            .withEphemeralStorageRequest("1G")
            .withEphemeralStorageLimit("1G"),
        ),
      )
    }
  }
}
