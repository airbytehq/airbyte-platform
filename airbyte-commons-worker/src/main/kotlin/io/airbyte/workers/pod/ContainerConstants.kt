package io.airbyte.workers.pod

object ContainerConstants {
  // shared containers
  const val INIT_CONTAINER_NAME = "init"

  // replication containers
  const val ORCHESTRATOR_CONTAINER_NAME = "orchestrator"
  const val SOURCE_CONTAINER_NAME = "source"
  const val DESTINATION_CONTAINER_NAME = "destination"

  // connector operation containers
  const val MAIN_CONTAINER_NAME = "main"
  const val SIDECAR_CONTAINER_NAME = "connector-sidecar"
}
