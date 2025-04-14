/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.common

import io.airbyte.api.model.generated.StreamDescriptor

object StreamDescriptorUtils {
  @JvmStatic
  fun buildFullyQualifiedName(descriptor: StreamDescriptor): String =
    with(descriptor) {
      namespace?.let { "$it.$name" } ?: name
    }

  @JvmStatic
  fun buildFieldName(path: List<String>): String = path.joinToString(separator = ".")
}
