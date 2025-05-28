/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import com.google.common.annotations.VisibleForTesting
import io.airbyte.workload.launcher.constants.PodConstants.CPU_RESOURCE_KEY
import io.airbyte.workload.launcher.constants.PodConstants.EPHEMERAL_STORAGE_RESOURCE_KEY
import io.airbyte.workload.launcher.constants.PodConstants.MEMORY_RESOURCE_KEY
import io.fabric8.kubernetes.api.model.Quantity
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.airbyte.config.ResourceRequirements as AirbyteResourceRequirements

private val logger = KotlinLogging.logger {}

/**
 * Utility functions for converting to / from Kube / Fabric layer abstractions
 *
 * Convert to singleton as necessary.
 */
object ResourceConversionUtils {
  fun domainToApi(resourceRequirements: AirbyteResourceRequirements?): ResourceRequirements {
    if (resourceRequirements != null) {
      var cpuLimit: Quantity? = null
      var memoryLimit: Quantity? = null
      var storageLimit: Quantity? = null
      val limitMap: MutableMap<String, Quantity?> = HashMap()
      if (!resourceRequirements.cpuLimit.isNullOrEmpty()) {
        cpuLimit = Quantity.parse(resourceRequirements.cpuLimit)
        limitMap[CPU_RESOURCE_KEY] = cpuLimit
      }
      if (!resourceRequirements.memoryLimit.isNullOrEmpty()) {
        memoryLimit = Quantity.parse(resourceRequirements.memoryLimit)
        limitMap[MEMORY_RESOURCE_KEY] = memoryLimit
      }
      if (!resourceRequirements.ephemeralStorageLimit.isNullOrEmpty()) {
        storageLimit = Quantity.parse(resourceRequirements.ephemeralStorageLimit)
        limitMap[EPHEMERAL_STORAGE_RESOURCE_KEY] = storageLimit
      }
      val requestMap: MutableMap<String, Quantity?> = HashMap()
      // if null then use unbounded resource allocation
      if (!resourceRequirements.cpuRequest.isNullOrEmpty()) {
        val cpuRequest = Quantity.parse(resourceRequirements.cpuRequest)
        requestMap[CPU_RESOURCE_KEY] = min(cpuRequest, cpuLimit)
      }
      if (!resourceRequirements.memoryRequest.isNullOrEmpty()) {
        val memoryRequest = Quantity.parse(resourceRequirements.memoryRequest)
        requestMap[MEMORY_RESOURCE_KEY] = min(memoryRequest, memoryLimit)
      }
      if (!resourceRequirements.ephemeralStorageRequest.isNullOrEmpty()) {
        val storageRequest = Quantity.parse(resourceRequirements.ephemeralStorageRequest)
        requestMap[EPHEMERAL_STORAGE_RESOURCE_KEY] = min(storageRequest, storageLimit)
      }
      return ResourceRequirementsBuilder()
        .withRequests<Any, Any>(requestMap)
        .withLimits<Any, Any>(limitMap)
        .build()
    }
    return ResourceRequirementsBuilder().build()
  }

  /**
   * Kube has a specific string DSL for specifying 'quantities' of bytes (e.g. 5G vs 5Gi vs 10m, vs 10K, etc.)
   * This converts those strings into the raw number of bytes for easier handling.
   */
  fun kubeQuantityStringToBytes(quantityStr: String?): Long? =
    try {
      Quantity.getAmountInBytes(Quantity(quantityStr)).toLong()
    } catch (_: Exception) {
      null
    }

  /**
   * Creates a domain resource requirement out of two resource requirements, summing their values.
   */
  fun sumResourceRequirements(
    reqs1: AirbyteResourceRequirements,
    reqs2: AirbyteResourceRequirements,
  ): AirbyteResourceRequirements =
    AirbyteResourceRequirements()
      .withCpuRequest(sum(reqs1.cpuRequest, reqs2.cpuRequest))
      .withCpuLimit(sum(reqs1.cpuLimit, reqs2.cpuLimit))
      .withMemoryRequest(sum(reqs1.memoryRequest, reqs2.memoryRequest))
      .withMemoryLimit(sum(reqs1.memoryLimit, reqs2.memoryLimit))
      .withEphemeralStorageRequest(sum(reqs1.ephemeralStorageRequest, reqs2.ephemeralStorageRequest))
      .withEphemeralStorageLimit(sum(reqs1.ephemeralStorageLimit, reqs2.ephemeralStorageLimit))

  /**
   * Kube has a specific string DSL for specifying 'quantities' of bytes (e.g. 5G vs 5Gi vs 10m, vs 10K, etc.)
   * This parses those values and sums them and returns them in the same DSL format.
   */
  @VisibleForTesting
  fun sum(
    quantityStr1: String?,
    quantityStr2: String?,
  ): String? {
    if (quantityStr1.isNullOrBlank()) {
      return quantityStr2
    }
    if (quantityStr2.isNullOrBlank()) {
      return quantityStr1
    }

    val q1 = Quantity(quantityStr1)
    val q2 = Quantity(quantityStr2)

    val sum = q1.add(q2)

    return sum.toString()
  }

  private fun min(
    request: Quantity?,
    limit: Quantity?,
  ): Quantity? {
    if (limit == null) {
      return request
    }
    if (request == null) {
      return limit
    }
    return if (request.numericalAmount <= limit.numericalAmount) {
      request
    } else {
      logger.info { "Invalid resource requirements detected, requested $request while limit is $limit, falling back to requesting $limit." }
      limit
    }
  }
}
