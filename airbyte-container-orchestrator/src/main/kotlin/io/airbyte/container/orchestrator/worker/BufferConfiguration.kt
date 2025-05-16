/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.commons.concurrency.ClosableLinkedBlockingQueue

const val DEFAULT_SOURCE_MAX_BUFFER_SIZE = 1000
const val DEFAULT_DESTINATION_MAX_BUFFER_SIZE = 1000
const val DEFAULT_POLL_TIME_OUT_DURATION_SECONDS = ClosableLinkedBlockingQueue.DEFAULT_POLL_TIME_OUT_DURATION_SECONDS

fun withBufferSize(bufferSize: Int) = BufferConfiguration(sourceMaxBufferSize = bufferSize, destinationMaxBufferSize = bufferSize)

fun withPollTimeout(pollTimeoutDuration: Int) = BufferConfiguration(pollTimeoutDuration = pollTimeoutDuration)

fun withDefaultConfiguration() = BufferConfiguration()

data class BufferConfiguration(
  val sourceMaxBufferSize: Int = DEFAULT_SOURCE_MAX_BUFFER_SIZE,
  val destinationMaxBufferSize: Int = DEFAULT_DESTINATION_MAX_BUFFER_SIZE,
  val pollTimeoutDuration: Int = DEFAULT_POLL_TIME_OUT_DURATION_SECONDS,
)
