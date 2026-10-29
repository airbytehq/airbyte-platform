/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.config

import io.airbyte.commons.net.BlockedAddressMatcher
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClient
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.directjobrunner.connect.ConnectProbe
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class ApplicationFactory {
  @Singleton
  @Named("outputDocumentStore")
  fun workloadStorageClient(factory: StorageClientFactory): StorageClient = factory.create(DocumentType.WORKLOAD_OUTPUT)

  @Singleton
  fun blockedAddressMatcher(config: ConnectConfig): BlockedAddressMatcher = BlockedAddressMatcher(config.extraBlockedCidrs)

  @Singleton
  fun connectProbe(
    matcher: BlockedAddressMatcher,
    config: ConnectConfig,
  ): ConnectProbe = ConnectProbe(matcher = matcher, overallTimeout = config.overallTimeout)
}
