package io.airbyte.workers.exception

class ResourceConstraintException(message: String, cause: Throwable, commandType: KubeCommandType, podType: PodType? = null) :
  KubeClientException(message, cause, commandType, podType)
