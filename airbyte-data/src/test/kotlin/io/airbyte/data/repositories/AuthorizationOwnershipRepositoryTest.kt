/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.AuthorizationCommand
import io.airbyte.data.repositories.entities.DataplaneGroup
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.Workspace
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

class AuthorizationOwnershipRepositoryTest : AbstractConfigRepositoryTest() {
  private val commands = context.getBean(AuthorizationCommandRepository::class.java)
  private val org = organizationRepository.save(Organization(name = "owner", email = "owner@example.com")).id!!
  private val otherOrg = organizationRepository.save(Organization(name = "other", email = "other@example.com")).id!!
  private val group =
    dataplaneGroupRepository
      .save(
        DataplaneGroup(organizationId = org, name = "region-${UUID.randomUUID()}", enabled = true, tombstone = false),
      ).id!!
  private val workspace =
    workspaceRepository
      .save(
        Workspace(name = "workspace", slug = UUID.randomUUID().toString(), dataplaneGroupId = group, organizationId = org),
      ).id!!

  @Test
  fun `command metadata requires expected organization and omits command payload`() {
    val id = workload(workspace, org)
    command(id, workspace, org)
    assertEquals(AuthorizationCommand(id, workspace, org), commands.findByIdAndOrganizationId(id, org).orElseThrow())
    assertTrue(commands.findByIdAndOrganizationId(id, otherOrg).isEmpty)
    assertTrue(commands.findByIdAndOrganizationId("missing", org).isEmpty)
  }

  @Test
  fun `contradictory saved workspace and organization do not produce a command target`() {
    val id = workload(workspace, otherOrg)
    command(id, workspace, otherOrg)
    assertTrue(commands.findByIdAndOrganizationId(id, otherOrg).isEmpty)
  }

  private fun workload(
    workspaceId: UUID?,
    organizationId: UUID?,
  ): String {
    val id = UUID.randomUUID().toString()
    jooqDslContext.execute(
      "INSERT INTO workload (id, status, input_payload, log_path, type, workspace_id, organization_id) VALUES (?, 'pending', 'private payload', 'logs', 'sync', ?, ?)",
      id,
      workspaceId,
      organizationId,
    )
    return id
  }

  private fun command(
    id: String,
    workspaceId: UUID,
    organizationId: UUID,
  ) {
    jooqDslContext.execute(
      "INSERT INTO commands (id, workload_id, command_type, command_input, workspace_id, organization_id) VALUES (?, ?, 'replicate', '{\"private\":true}', ?, ?)",
      id,
      id,
      workspaceId,
      organizationId,
    )
  }
}
