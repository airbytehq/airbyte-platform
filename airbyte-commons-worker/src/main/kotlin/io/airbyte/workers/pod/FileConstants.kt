/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.pod

object FileConstants {
  // dirs
  const val SOURCE_DIR = "/source"
  const val DEST_DIR = "/dest"
  const val CONFIG_DIR = "/config"

  // pipes
  const val STDIN_PIPE_FILE = "stdin"
  const val STDOUT_PIPE_FILE = "stdout"
  const val STDERR_PIPE_FILE = "stderr"

  // output files
  const val EXIT_CODE_FILE = "exitCode.txt"
  const val JOB_OUTPUT_FILE = "jobOutput.json"

  // input and configuration files
  const val CONNECTION_CONFIGURATION_FILE = "connectionConfiguration.json"
  const val INIT_INPUT_FILE = "input.json"
  const val SIDECAR_INPUT_FILE = "sidecarInput.json"
  const val CONNECTOR_CONFIG_FILE = "connectorConfig.json"
  const val CATALOG_FILE = "catalog.json"
  const val INPUT_STATE_FILE = "inputState.json"

  // marker files
  const val TERMINATION_MARKER_FILE = "TERMINATED"
  const val KUBE_CP_SUCCESS_MARKER_FILE = "FINISHED_UPLOADING"
}
