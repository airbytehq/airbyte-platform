/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.async.profiler

import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class ProfileStorageFactory {
  @Singleton
  @Named("profilerOutputStore")
  fun profilerOutputStoreClient(factory: StorageClientFactory): StorageClient = factory.create(DocumentType.PROFILER_OUTPUT)
}
