/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.transformmodels;

import io.airbyte.config.StreamDescriptor;
import java.util.Objects;

/**
 * Represents the diff between two {@link io.airbyte.protocol.models.AirbyteStream}.
 */
public final class StreamTransform {

  private final StreamTransformType transformType;
  private final StreamDescriptor streamDescriptor;
  private final UpdateStreamTransform updateStreamTransform;

  public StreamTransform(StreamTransformType transformType, StreamDescriptor streamDescriptor, UpdateStreamTransform updateStreamTransform) {
    this.transformType = transformType;
    this.streamDescriptor = streamDescriptor;
    this.updateStreamTransform = updateStreamTransform;
  }

  public static StreamTransform createAddStreamTransform(final StreamDescriptor streamDescriptor) {
    return new StreamTransform(StreamTransformType.ADD_STREAM, streamDescriptor, null);
  }

  public static StreamTransform createRemoveStreamTransform(final StreamDescriptor streamDescriptor) {
    return new StreamTransform(StreamTransformType.REMOVE_STREAM, streamDescriptor, null);
  }

  public static StreamTransform createUpdateStreamTransform(final StreamDescriptor streamDescriptor,
                                                            final UpdateStreamTransform updateStreamTransform) {
    return new StreamTransform(StreamTransformType.UPDATE_STREAM, streamDescriptor, updateStreamTransform);
  }

  public StreamTransformType getTransformType() {
    return transformType;
  }

  public StreamDescriptor getStreamDescriptor() {
    return streamDescriptor;
  }

  public UpdateStreamTransform getUpdateStreamTransform() {
    return updateStreamTransform;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamTransform that = (StreamTransform) o;
    return transformType == that.transformType && Objects.equals(streamDescriptor, that.streamDescriptor) && Objects.equals(
        updateStreamTransform, that.updateStreamTransform);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transformType, streamDescriptor, updateStreamTransform);
  }

  @Override
  public String toString() {
    return "StreamTransform{"
        + "transformType=" + transformType
        + ", streamDescriptor=" + streamDescriptor
        + ", updateStreamTransform=" + updateStreamTransform
        + '}';
  }

}
