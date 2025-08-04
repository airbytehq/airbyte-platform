/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.DataplaneGroup
import io.airbyte.config.OperatorWebhook
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.Mockito
import java.io.IOException
import java.util.List
import java.util.UUID

internal class SyncOperationPersistenceTest : BaseConfigDatabaseTest() {
  private var operationService: OperationService? = null

  @BeforeEach
  @Throws(Exception::class)
  fun beforeEach() {
    truncateAllTables()

    operationService = OperationServiceJooqImpl(database)

    createWorkspace()

    for (op in OPS) {
      operationService!!.writeStandardSyncOperation(op)
    }
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class)
  fun testReadWrite() {
    for (op in OPS) {
      Assertions.assertEquals(op, operationService!!.getStandardSyncOperation(op.getOperationId()))
    }
  }

  @Test
  fun testReadNotExists() {
    Assertions.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      Executable {
        operationService!!.getStandardSyncOperation(
          UUID.randomUUID(),
        )
      },
    )
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class)
  fun testList() {
    Assertions.assertEquals(OPS, operationService!!.listStandardSyncOperations())
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class)
  fun testDelete() {
    for (op in OPS) {
      Assertions.assertEquals(op, operationService!!.getStandardSyncOperation(op.getOperationId()))
      operationService!!.deleteStandardSyncOperation(op.getOperationId())
      Assertions.assertThrows<ConfigNotFoundException?>(
        ConfigNotFoundException::class.java,
        Executable {
          operationService!!.getStandardSyncOperation(
            UUID.randomUUID(),
          )
        },
      )
    }
  }

  @Throws(IOException::class, JsonValidationException::class)
  private fun createWorkspace() {
    val featureFlagClient: FeatureFlagClient = Mockito.mock<TestClient>(TestClient::class.java)
    val secretsRepositoryReader = Mockito.mock<SecretsRepositoryReader>(SecretsRepositoryReader::class.java)
    val secretsRepositoryWriter = Mockito.mock<SecretsRepositoryWriter>(SecretsRepositoryWriter::class.java)
    val secretPersistenceConfigService = Mockito.mock<SecretPersistenceConfigService>(SecretPersistenceConfigService::class.java)
    val metricClient = Mockito.mock<MetricClient>(MetricClient::class.java)

    OrganizationServiceJooqImpl(database).writeOrganization(MockData.defaultOrganization())

    val dataplaneGroupService: DataplaneGroupService = DataplaneGroupServiceTestJooqImpl(database!!)
    dataplaneGroupService.writeDataplaneGroup(
      DataplaneGroup()
        .withId(UUID.randomUUID())
        .withOrganizationId(MockData.defaultOrganization()!!.getOrganizationId())
        .withName("test")
        .withEnabled(true)
        .withTombstone(false),
    )

    val workspace =
      StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("Another Workspace")
        .withSlug("another-workspace")
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDataplaneGroupId(UUID.randomUUID())
        .withOrganizationId(MockData.defaultOrganization()!!.getOrganizationId())
    WorkspaceServiceJooqImpl(
      database,
      featureFlagClient,
      secretsRepositoryReader,
      secretsRepositoryWriter,
      secretPersistenceConfigService,
      metricClient,
    ).writeStandardWorkspaceNoSecrets(workspace)
  }

  companion object {
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val WEBHOOK_CONFIG_ID: UUID = UUID.randomUUID()
    private const val WEBHOOK_OPERATION_EXECUTION_URL = "test-webhook-url"
    private const val WEBHOOK_OPERATION_EXECUTION_BODY = "test-webhook-body"

    private val WEBHOOK_OP: StandardSyncOperation =
      StandardSyncOperation()
        .withName("webhook-operation")
        .withTombstone(false)
        .withOperationId(UUID.randomUUID())
        .withWorkspaceId(WORKSPACE_ID)
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(
          OperatorWebhook()
            .withWebhookConfigId(WEBHOOK_CONFIG_ID)
            .withExecutionUrl(WEBHOOK_OPERATION_EXECUTION_URL)
            .withExecutionBody(WEBHOOK_OPERATION_EXECUTION_BODY),
        )
    private val OPS: MutableList<StandardSyncOperation> = List.of<StandardSyncOperation?>(WEBHOOK_OP)
  }
}
