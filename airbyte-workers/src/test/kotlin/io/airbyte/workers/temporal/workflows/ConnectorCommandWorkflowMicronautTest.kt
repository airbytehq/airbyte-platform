package io.airbyte.workers.temporal.workflows

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.workers.commands.CheckCommand
import io.airbyte.workers.commands.DiscoverCommand
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

  @Inject
  lateinit var checkCommand: CheckCommand

  @Inject
  lateinit var discoverCommand: DiscoverCommand

  @Inject
  private lateinit var connectorCommmandActivity: ConnectorCommandActivity

  @Test
  fun `test that it worked`() {
  }
}
