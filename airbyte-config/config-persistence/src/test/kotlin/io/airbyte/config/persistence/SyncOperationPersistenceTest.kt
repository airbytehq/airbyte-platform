/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class SyncOperationPersistenceTest : BaseConfigDatabaseTest() {
  private var operationService: OperationService? = null

  @BeforeEach
  fun beforeEach() {
    truncateAllTables()

    operationService = OperationServiceJooqImpl(database)

    createWorkspace()

    for (op in OPS) {
      operationService!!.writeStandardSyncOperation(op)
    }
  }

  @Test
  fun testReadWrite() {
    for (op in OPS) {
      Assertions.assertEquals(op, operationService!!.getStandardSyncOperation(op.operationId))
    }
  }

  @Test
  fun testReadNotExists() {
    Assertions.assertThrows(
      ConfigNotFoundException::class.java,
    ) {
      operationService!!.getStandardSyncOperation(
        UUID.randomUUID(),
      )
    }
  }

  @Test
  fun testList() {
    Assertions.assertEquals(OPS, operationService!!.listStandardSyncOperations())
  }

  @Test
  fun testDelete() {
    for (op in OPS) {
      Assertions.assertEquals(op, operationService!!.getStandardSyncOperation(op.operationId))
      operationService!!.deleteStandardSyncOperation(op.operationId)
      Assertions.assertThrows(
        ConfigNotFoundException::class.java,
      ) {
        operationService!!.getStandardSyncOperation(
          UUID.randomUUID(),
        )
      }
    }
  }

  private fun createWorkspace() {
    val featureFlagClient: FeatureFlagClient = mockk<TestClient>(relaxed = true)
    val secretsRepositoryReader = mockk<SecretsRepositoryReader>(relaxed = true)
    val secretsRepositoryWriter = mockk<SecretsRepositoryWriter>(relaxed = true)
    val secretPersistenceConfigService = mockk<SecretPersistenceConfigService>(relaxed = true)
    val metricClient = mockk<MetricClient>(relaxed = true)

    OrganizationServiceJooqImpl(database).writeOrganization(MockData.defaultOrganization())

    val dataplaneGroupService: DataplaneGroupService = DataplaneGroupServiceTestJooqImpl(database!!)
    dataplaneGroupService.writeDataplaneGroup(
      DataplaneGroup()
        .withId(UUID.randomUUID())
        .withOrganizationId(MockData.defaultOrganization().organizationId)
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
        .withOrganizationId(MockData.defaultOrganization().organizationId)
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
    private val OPS: List<StandardSyncOperation> = listOf(WEBHOOK_OP)
  }
}
