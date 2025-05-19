/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.constants

object ContainerConstants {
  // shared containers
  const val INIT_CONTAINER_NAME = "init"
  const val PROFILER_CONTAINER_NAME = "profiler"

  // replication containers
  const val ORCHESTRATOR_CONTAINER_NAME = "orchestrator"
  const val SOURCE_CONTAINER_NAME = "source"
  const val DESTINATION_CONTAINER_NAME = "destination"

  // connector operation containers
  const val MAIN_CONTAINER_NAME = "main"
  const val SIDECAR_CONTAINER_NAME = "connector-sidecar"
}
