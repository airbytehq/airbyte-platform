/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.common

import io.airbyte.api.server.generated.models.StreamDescriptor

object StreamDescriptorUtils {
  @JvmStatic
  fun buildFullyQualifiedName(descriptor: StreamDescriptor): String =
    with(descriptor) {
      namespace?.let { "$it.$name" } ?: name
    }

  @JvmStatic
  fun buildFieldName(path: List<String>): String = path.joinToString(separator = ".")
}
