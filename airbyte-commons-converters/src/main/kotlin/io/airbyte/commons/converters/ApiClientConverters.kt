/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import io.airbyte.api.client.model.generated.StreamDescriptor as ApiClientStreamDescriptor
import io.airbyte.config.StreamDescriptor as InternalStreamDescriptor

/**
 * Utility class for converting between API client models and internal configuration models.
 */
class ApiClientConverters {
  companion object {
    /**
     * Converts an API client StreamDescriptor to an internal StreamDescriptor.
     *
     * @return InternalStreamDescriptor with the same name and namespace
     */
    @JvmStatic
    fun ApiClientStreamDescriptor.toInternal(): InternalStreamDescriptor = InternalStreamDescriptor().withNamespace(namespace).withName(name)
  }
}
