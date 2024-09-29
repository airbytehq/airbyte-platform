package io.airbyte.workers.general

import io.airbyte.commons.concurrency.ClosableLinkedBlockingQueue

data class BufferConfiguration(
  val sourceMaxBufferSize: Int = DEFAULT_SOURCE_MAX_BUFFER_SIZE,
  val destinationMaxBufferSize: Int = DEFAULT_DESTINATION_MAX_BUFFER_SIZE,
  val pollTimeoutDuration: Int = DEFAULT_POLL_TIME_OUT_DURATION_SECONDS,
) {
  companion object {
    const val DEFAULT_SOURCE_MAX_BUFFER_SIZE = 1000
    const val DEFAULT_DESTINATION_MAX_BUFFER_SIZE = 1000
    const val DEFAULT_POLL_TIME_OUT_DURATION_SECONDS = ClosableLinkedBlockingQueue.DEFAULT_POLL_TIME_OUT_DURATION_SECONDS

    // Helpers for Java due to the lack of named parameters

    @JvmStatic
    fun withBufferSize(bufferSize: Int) = BufferConfiguration(sourceMaxBufferSize = bufferSize, destinationMaxBufferSize = bufferSize)

    @JvmStatic
    fun withPollTimeout(pollTimeoutDuration: Int): BufferConfiguration = BufferConfiguration(pollTimeoutDuration = pollTimeoutDuration)

    @JvmStatic
    fun withDefaultConfiguration() = BufferConfiguration()
  }
}
