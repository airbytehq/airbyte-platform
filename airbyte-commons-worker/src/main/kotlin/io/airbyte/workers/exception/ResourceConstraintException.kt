/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.exception

class ResourceConstraintException(
  message: String,
  cause: Throwable,
  commandType: KubeCommandType,
  podType: PodType? = null,
) : KubeClientException(message, cause, commandType, podType)
