/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.workflows

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.workers.commands.CheckCommand
import io.airbyte.workers.commands.CheckCommandThroughApi
import io.airbyte.workers.commands.DiscoverCommand
import io.airbyte.workers.commands.DiscoverCommandV2
import io.airbyte.workers.commands.ReplicationCommand
import io.airbyte.workers.storage.activities.OutputStorageClient
import io.airbyte.workers.sync.WorkloadClient
import io.airbyte.workers.workload.WorkloadIdGenerator
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Replaces
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Named
import org.junit.jupiter.api.Test
import java.nio.file.Path

@MicronautTest
@Property(name = "STORAGE_TYPE", value = "yo")
class ConnectorCommandWorkflowMicronautTest {
  @Named("workspaceRoot")
  @Bean
  @Replaces(Path::class)
  var workspaceRoot: Path = mockk()

  @Bean
  @Replaces(AirbyteApiClient::class)
  var airbyteApiClient: AirbyteApiClient = mockk()

  @Bean
  @Replaces(WorkloadClient::class)
  var workloadClient: WorkloadClient = mockk()

  @Bean
  @Replaces(WorkloadIdGenerator::class)
  var workloadIdGenerator: WorkloadIdGenerator = mockk()

  @Bean
  @Replaces(LogClientManager::class)
  var logClientManager: LogClientManager = mockk()

  @Bean
  @Replaces(StorageClientFactory::class)
  var storageClientFactory: StorageClientFactory = mockk()

  @Bean
  @Replaces(OutputStorageClient::class)
  @Named("outputCatalogClient")
  var outputStorageClient: OutputStorageClient<ConfiguredAirbyteCatalog> = mockk()

  @Inject
  lateinit var checkCommand: CheckCommand

  @Inject
  lateinit var checkCommandThroughApi: CheckCommandThroughApi

  @Inject
  lateinit var discoverCommand: DiscoverCommand

  @Inject
  lateinit var discoverCommandV2: DiscoverCommandV2

  @Inject
  lateinit var replicationCommand: ReplicationCommand

  @Inject
  private lateinit var connectorCommmandActivity: ConnectorCommandActivity

  @Test
  fun `test that it worked`() {
  }
}
