/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.constants

object PodConstants {
  const val KUBE_NAME_LEN_LIMIT = 63
  const val CPU_RESOURCE_KEY = "cpu"
  const val MEMORY_RESOURCE_KEY = "memory"
  const val EPHEMERAL_STORAGE_RESOURCE_KEY = "ephemeral-storage"
}
