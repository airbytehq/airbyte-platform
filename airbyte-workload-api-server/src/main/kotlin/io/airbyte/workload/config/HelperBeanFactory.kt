/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.config

import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID
import java.util.function.Supplier

@Factory
class HelperBeanFactory {
  @Singleton
  @Named("uuidGenerator")
  fun randomUUIDSupplier(): Supplier<UUID> = Supplier { UUID.randomUUID() }
}
