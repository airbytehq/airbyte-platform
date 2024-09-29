package io.airbyte.commons.converters

import io.airbyte.api.client.model.generated.StreamDescriptor as ApiClientStreamDescriptor
import io.airbyte.config.StreamDescriptor as InternalStreamDescriptor

class ApiClientConverters {
  companion object {
    @JvmStatic
    fun ApiClientStreamDescriptor.toInternal(): InternalStreamDescriptor = InternalStreamDescriptor().withNamespace(namespace).withName(name)
  }
}
