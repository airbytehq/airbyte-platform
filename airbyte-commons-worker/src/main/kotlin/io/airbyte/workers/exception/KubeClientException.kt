/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.exception

open class KubeClientException(
  override val message: String,
  override val cause: Throwable,
  val commandType: KubeCommandType,
  val podType: PodType? = null,
) : Throwable(message, cause)

enum class PodType {
  REPLICATION,
  RESET,
}

enum class KubeCommandType {
  WAIT_INIT,
  WAIT_MAIN,
  COPY,
  DELETE,
  CREATE,
}
