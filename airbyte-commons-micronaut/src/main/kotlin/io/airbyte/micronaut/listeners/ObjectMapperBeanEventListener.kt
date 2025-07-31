/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.listeners

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.jackson.MoreMappers
import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import jakarta.inject.Singleton

@Singleton
class ObjectMapperBeanEventListener : BeanCreatedEventListener<ObjectMapper> {
  override fun onCreated(event: BeanCreatedEvent<ObjectMapper>): ObjectMapper {
    /**
     * Configure the ObjectMapper created by Micronaut to use the same configuration
     * that we use for other serialization in the code base.
     */
    return MoreMappers.configure(event.bean)
  }
}
