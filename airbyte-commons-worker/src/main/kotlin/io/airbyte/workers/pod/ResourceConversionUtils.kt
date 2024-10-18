/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.pod

import com.google.common.base.Strings
import io.fabric8.kubernetes.api.model.Quantity
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.airbyte.config.ResourceRequirements as AirbyteResourceRequirments

val logger = KotlinLogging.logger {}

/**
 * Utility functions for converting to / from Kube / Fabric layer abstractions
 *
 * Convert to singleton as necessary.
 */
object ResourceConversionUtils {
  fun buildResourceRequirements(resourceRequirements: AirbyteResourceRequirments?): ResourceRequirements {
    if (resourceRequirements != null) {
      var cpuLimit: Quantity? = null
      var memoryLimit: Quantity? = null
      var storageLimit: Quantity? = null
      val limitMap: MutableMap<String, Quantity?> = HashMap()
      if (!Strings.isNullOrEmpty(resourceRequirements.cpuLimit)) {
        cpuLimit = Quantity.parse(resourceRequirements.cpuLimit)
        limitMap["cpu"] = cpuLimit
      }
      if (!Strings.isNullOrEmpty(resourceRequirements.memoryLimit)) {
        memoryLimit = Quantity.parse(resourceRequirements.memoryLimit)
        limitMap["memory"] = memoryLimit
      }
      if (!Strings.isNullOrEmpty(resourceRequirements.ephemeralStorageLimit)) {
        storageLimit = Quantity.parse(resourceRequirements.ephemeralStorageLimit)
        limitMap["ephemeral-storage"] = storageLimit
      }
      val requestMap: MutableMap<String, Quantity?> = HashMap()
      // if null then use unbounded resource allocation
      if (!Strings.isNullOrEmpty(resourceRequirements.cpuRequest)) {
        val cpuRequest = Quantity.parse(resourceRequirements.cpuRequest)
        requestMap["cpu"] = min(cpuRequest, cpuLimit)
      }
      if (!Strings.isNullOrEmpty(resourceRequirements.memoryRequest)) {
        val memoryRequest = Quantity.parse(resourceRequirements.memoryRequest)
        requestMap["memory"] = min(memoryRequest, memoryLimit)
      }
      if (!Strings.isNullOrEmpty(resourceRequirements.ephemeralStorageRequest)) {
        val storageRequest = Quantity.parse(resourceRequirements.ephemeralStorageRequest)
        requestMap["ephemeral-storage"] = min(storageRequest, storageLimit)
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
    } catch (e: Exception) {
      null
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
