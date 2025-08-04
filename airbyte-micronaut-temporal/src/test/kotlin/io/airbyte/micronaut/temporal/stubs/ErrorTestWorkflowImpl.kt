/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal.stubs

import io.airbyte.commons.temporal.exception.RetryableException

open class ErrorTestWorkflowImpl : TestWorkflow {
  @Throws(RetryableException::class)
  override fun run(): Unit = throw RetryableException(NullPointerException("test"))

  override fun cancel() {}

  override val state: Int
    get() = 1
}
