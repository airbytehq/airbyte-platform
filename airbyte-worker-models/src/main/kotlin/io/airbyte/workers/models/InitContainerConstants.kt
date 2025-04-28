/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models

object InitContainerConstants {
  const val SUCCESS_EXIT_CODE = 0
  const val UNEXPECTED_ERROR_EXIT_CODE = 1
  const val WORKLOAD_API_ERROR_EXIT_CODE = 2
  const val SECRET_HYDRATION_ERROR_EXIT_CODE = 10
}
