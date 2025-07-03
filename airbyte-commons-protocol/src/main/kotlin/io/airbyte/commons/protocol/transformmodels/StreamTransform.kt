/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.transformmodels

import io.airbyte.config.StreamDescriptor
import java.util.Objects

/**
 * Represents the diff between two [io.airbyte.protocol.models.AirbyteStream].
 */
data class StreamTransform(
  @JvmField val transformType: StreamTransformType,
  @JvmField val streamDescriptor: StreamDescriptor,
  @JvmField val updateStreamTransform: UpdateStreamTransform?,
) {
  override fun equals(o: Any?): Boolean {
    if (o == null || javaClass != o.javaClass) {
      return false
    }
    val that = o as StreamTransform
    return transformType == that.transformType && streamDescriptor == that.streamDescriptor && updateStreamTransform == that.updateStreamTransform
  }

  override fun hashCode(): Int = Objects.hash(transformType, streamDescriptor, updateStreamTransform)

  override fun toString(): String =
    (
      "StreamTransform{" +
        "transformType=" + transformType +
        ", streamDescriptor=" + streamDescriptor +
        ", updateStreamTransform=" + updateStreamTransform +
        '}'
    )

  companion object {
    @JvmStatic
    fun createAddStreamTransform(streamDescriptor: StreamDescriptor): StreamTransform =
      StreamTransform(StreamTransformType.ADD_STREAM, streamDescriptor, null)

    @JvmStatic
    fun createRemoveStreamTransform(streamDescriptor: StreamDescriptor): StreamTransform =
      StreamTransform(StreamTransformType.REMOVE_STREAM, streamDescriptor, null)

    @JvmStatic
    fun createUpdateStreamTransform(
      streamDescriptor: StreamDescriptor,
      updateStreamTransform: UpdateStreamTransform?,
    ): StreamTransform = StreamTransform(StreamTransformType.UPDATE_STREAM, streamDescriptor, updateStreamTransform)
  }
}
