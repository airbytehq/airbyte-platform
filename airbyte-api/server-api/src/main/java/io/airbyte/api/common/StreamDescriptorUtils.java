/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.common;

import io.airbyte.api.model.generated.StreamDescriptor;
import java.util.List;

public class StreamDescriptorUtils {

  public static String buildFullyQualifiedName(StreamDescriptor descriptor) {
    return descriptor.getNamespace() != null ? String.format("%s.%s", descriptor.getNamespace(), descriptor.getName())
        : descriptor.getName();
  }

  public static String buildFieldName(List<String> path) {
    return String.join(".", path);
  }

}
