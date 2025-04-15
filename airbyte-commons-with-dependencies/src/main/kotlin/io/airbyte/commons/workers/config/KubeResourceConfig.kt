/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config

import io.micronaut.context.annotation.EachProperty
import io.micronaut.context.annotation.Parameter

/**
 * Encapsulates the configuration that is specific to Kubernetes.
 * This is meant for the WorkerConfigsProvider to be reading configs, not for direct use as fallback logic isn't implemented here.
 */
@EachProperty("airbyte.worker.kube-job-configs")
class KubeResourceConfig(
  @param:Parameter val name: String,
) {
  var annotations: String? = null
  var labels: String? = null
  var nodeSelectors: String? = null
  var cpuLimit: String? = null
  var cpuRequest: String? = null
  var memoryLimit: String? = null
  var memoryRequest: String? = null
  var ephemeralStorageLimit: String? = null
  var ephemeralStorageRequest: String? = null
}
