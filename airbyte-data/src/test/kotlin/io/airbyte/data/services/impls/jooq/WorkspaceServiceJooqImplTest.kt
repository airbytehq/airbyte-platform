/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.commons.json.Jsons
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.UUID

internal class WorkspaceServiceJooqImplTest : BaseConfigDatabaseTest() {
  private val workspaceService =
    WorkspaceServiceJooqImpl(database, mockk(), mockk(), mockk(), mockk(), mockk(), configTransactions)

  @BeforeEach
  fun setUp() {
    truncateAllTables()
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `moving a workspace to another existing organization preserves webhook configs`(withWebhookConfigs: Boolean) {
    val fixture = JooqTestDbSetupHelper()
    fixture.createOrganization()
    fixture.createDataplaneGroup()
    val workspace = fixture.createWorkspace()
    if (withWebhookConfigs) {
      workspace.webhookOperationConfigs =
        Jsons.deserialize(
          """{"webhookConfigs":[
            {"id":"${UUID.randomUUID()}","name":"dbt cloud","authToken":{"_secret":"dbt-token_v1"}},
            {"id":"${UUID.randomUUID()}","name":"other","authToken":{"_secret":"other-token_v2"}}
          ]}""",
        )
      workspaceService.writeStandardWorkspaceNoSecrets(workspace)
    }
    val destinationOrganization = fixture.createOrganization(organizationId = UUID.randomUUID())
    val persistedWorkspace = workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false)
    val previousConfigs = persistedWorkspace.webhookOperationConfigs
    val previousOrganizationId = persistedWorkspace.organizationId
    persistedWorkspace.organizationId = destinationOrganization.organizationId

    workspaceService.writeStandardWorkspaceNoSecrets(persistedWorkspace, previousOrganizationId)

    val movedWorkspace = workspaceService.getStandardWorkspaceNoSecrets(workspace.workspaceId, false)
    assertEquals(destinationOrganization.organizationId, movedWorkspace.organizationId)
    assertEquals(previousConfigs, movedWorkspace.webhookOperationConfigs)
  }
}
