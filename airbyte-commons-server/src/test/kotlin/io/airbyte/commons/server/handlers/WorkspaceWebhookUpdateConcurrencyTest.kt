/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.api.server.generated.models.WebhookConfigWrite
import io.airbyte.api.server.generated.models.WorkspaceUpdate
import io.airbyte.api.server.generated.models.WorkspaceUpdateName
import io.airbyte.api.server.generated.models.WorkspaceUpdateOrganization
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.config.secrets.hydration.RealSecretsHydrator
import io.airbyte.config.secrets.persistence.LocalTestingSecretPersistence
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Supplier

internal class WorkspaceWebhookUpdateConcurrencyTest : BaseConfigDatabaseTest() {
  @BeforeEach
  fun setUp() {
    truncateAllTables()
  }

  @ParameterizedTest
  @ValueSource(
    strings = [
      "settings",
      "omitted settings",
      "name",
      "organization",
      "webhook name",
      "other token",
      "new webhook",
      "removed webhook",
      "removed webhook replacement token",
      "removed webhook legacy replacement token",
    ],
  )
  fun `overlapping workspace updates preserve current webhooks and reject removed IDs`(update: String) {
    val organizationId = UUID.randomUUID()
    val destinationOrganizationId = UUID.randomUUID()
    val dataplaneGroupId = UUID.randomUUID()
    database!!.query { ctx ->
      for (id in listOf(organizationId, destinationOrganizationId)) {
        ctx
          .insertInto(Tables.ORGANIZATION)
          .set(Tables.ORGANIZATION.ID, id)
          .set(Tables.ORGANIZATION.NAME, "org")
          .set(Tables.ORGANIZATION.EMAIL, "org@example.com")
          .execute()
      }
      ctx
        .insertInto(Tables.DATAPLANE_GROUP)
        .set(Tables.DATAPLANE_GROUP.ID, dataplaneGroupId)
        .set(Tables.DATAPLANE_GROUP.ORGANIZATION_ID, organizationId)
        .set(Tables.DATAPLANE_GROUP.NAME, "dataplane group")
        .execute()
    }
    val secretPersistence = database!!.query { ctx -> LocalTestingSecretPersistence(ctx) }
    val metricClient = mockk<MetricClient>(relaxed = true)
    val service =
      WorkspaceServiceJooqImpl(
        database,
        mockk(relaxed = true),
        SecretsRepositoryReader(RealSecretsHydrator(secretPersistence)),
        SecretsRepositoryWriter(secretPersistence, metricClient),
        mockk(),
        metricClient,
        configTransactions,
      )
    val configId = UUID.randomUUID()
    val otherId = UUID.randomUUID()
    val newId = UUID.randomUUID()
    val workspace =
      StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withCustomerId(UUID.randomUUID())
        .withOrganizationId(organizationId)
        .withDataplaneGroupId(dataplaneGroupId)
        .withName("workspace")
        .withSlug("workspace")
        .withInitialSetupComplete(false)
        .withDisplaySetupWizard(true)
        .withAnonymousDataCollection(false)
        .withNews(false)
        .withSecurityUpdates(false)
        .withTombstone(false)
        .withWebhookOperationConfigs(
          Jsons.deserialize(
            """{"webhookConfigs":[
              {"id":"$configId","name":"dbt cloud","authToken":"old-token"},
              {"id":"$otherId","name":"other","authToken":"other-token"}
            ]}""",
          ),
        )
    service.writeWorkspaceWithSecrets(workspace)

    val updateRead = CountDownLatch(1)
    val allowUpdate = CountDownLatch(1)
    val firstRead = AtomicBoolean(true)
    val pausedService =
      object : WorkspaceService by service {
        override fun getStandardWorkspaceNoSecrets(
          workspaceId: UUID,
          includeTombstone: Boolean,
        ): StandardWorkspace {
          val result = service.getStandardWorkspaceNoSecrets(workspaceId, includeTombstone)
          if (firstRead.compareAndSet(true, false)) {
            updateRead.countDown()
            check(allowUpdate.await(10, TimeUnit.SECONDS))
          }
          return result
        }
      }
    val handler =
      WorkspacesHandler(
        workspacePersistence = mockk(relaxed = true),
        organizationService = mockk(relaxed = true),
        permissionHandler = mockk(relaxed = true),
        connectionsHandler = mockk(relaxed = true),
        destinationHandler = mockk(relaxed = true),
        sourceHandler = mockk(relaxed = true),
        uuidSupplier = Supplier { newId },
        workspaceService = pausedService,
        workspaceHelper = mockk(relaxed = true),
        dataplaneGroupService = mockk(relaxed = true),
        entitlementService = mockk(relaxed = true),
        trackingClient = mockk(relaxed = true),
        limitsProvider = mockk(relaxed = true),
        consumptionService = mockk(relaxed = true),
        ffClient = mockk(relaxed = true),
        airbyteEdition = AirbyteEdition.CLOUD,
        roleResolver = mockk(relaxed = true),
        scimConfigurationService = mockk(relaxed = true),
        scimAccessGate = mockk(relaxed = true),
        operationsHandler = mockk(relaxed = true),
      )
    val executor = Executors.newSingleThreadExecutor()
    val removesWebhook = update.startsWith("removed webhook")
    try {
      val pendingUpdate =
        executor.submit {
          when (update) {
            "name" -> handler.updateWorkspaceName(WorkspaceUpdateName(workspaceId = workspace.workspaceId, name = "renamed"))
            "organization" ->
              handler.updateWorkspaceOrganization(
                WorkspaceUpdateOrganization(workspaceId = workspace.workspaceId, organizationId = destinationOrganizationId),
              )
            "omitted settings" ->
              handler.updateWorkspace(
                Jsons.deserialize(
                  """{"workspaceId":"${workspace.workspaceId}","anonymousDataCollection":true}""",
                  WorkspaceUpdate::class.java,
                ),
              )
            else ->
              handler.updateWorkspace(
                WorkspaceUpdate(
                  workspaceId = workspace.workspaceId,
                  anonymousDataCollection = true,
                  webhookConfigs =
                    if (update == "settings") {
                      null
                    } else {
                      listOf(
                        WebhookConfigWrite(id = otherId, name = "other", authToken = if (update == "other token") "new-other-token" else null),
                        WebhookConfigWrite(
                          id = if (update == "removed webhook legacy replacement token") null else configId,
                          name = if (update == "removed webhook legacy replacement token") "dbt cloud" else "renamed dbt",
                          authToken = if (removesWebhook && update != "removed webhook") "replacement-token" else null,
                        ),
                      ) + if (update == "new webhook") listOf(WebhookConfigWrite(name = "new", authToken = "new-webhook-token")) else emptyList()
                    },
                ),
              )
          }
        }
      assertTrue(updateRead.await(10, TimeUnit.SECONDS), "The update must read the old configuration before the rotation")
      val rotation = service.getStandardWorkspaceNoSecrets(workspace.workspaceId, false)
      (rotation.webhookOperationConfigs["webhookConfigs"][0] as ObjectNode).put("authToken", "new-token")
      service.writeWorkspaceWithSecrets(rotation)
      if (removesWebhook) {
        val removal = service.getStandardWorkspaceNoSecrets(workspace.workspaceId, false)
        (removal.webhookOperationConfigs["webhookConfigs"] as ArrayNode).remove(0)
        service.writeStandardWorkspaceNoSecrets(removal)
      }
      allowUpdate.countDown()
      if (removesWebhook) {
        val failure = assertThrows(ExecutionException::class.java) { pendingUpdate.get(10, TimeUnit.SECONDS) }
        assertInstanceOf(StateConflictProblem::class.java, failure.cause)
      } else {
        pendingUpdate.get(10, TimeUnit.SECONDS)
      }

      val saved = service.getWorkspaceWithSecrets(workspace.workspaceId, false)
      val configs = saved.webhookOperationConfigs["webhookConfigs"].associateBy { it["id"].asText() }
      if (removesWebhook) {
        assertEquals(setOf(otherId.toString()), configs.keys)
      } else {
        assertTrue(configId.toString() in configs, "The update must preserve the dbt webhook")
        assertEquals("new-token", configs.getValue(configId.toString())["authToken"].asText())
      }
      if (update == "new webhook") {
        assertEquals(setOf(configId.toString(), otherId.toString(), newId.toString()), configs.keys)
        assertEquals("new-webhook-token", configs.getValue(newId.toString())["authToken"].asText())
      }
      assertEquals(if (update == "other token") "new-other-token" else "other-token", configs.getValue(otherId.toString())["authToken"].asText())
      assertEquals(if (update == "organization") destinationOrganizationId else organizationId, saved.organizationId)
    } finally {
      allowUpdate.countDown()
      executor.shutdownNow()
    }
  }
}
