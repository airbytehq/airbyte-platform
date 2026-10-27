/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataplaneGroup
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.Workspace
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

class ExecutionAuthorizationRepositoryTest : AbstractConfigRepositoryTest() {
  @Test
  fun `execution metadata discovery selects exact IDs and tenant rereads exclude foreign owners while supporting legacy SPEC`() {
    val organization = organizationRepository.save(Organization(name = "execution owner", email = "owner@example.com")).id!!
    val group =
      dataplaneGroupRepository
        .save(
          DataplaneGroup(organizationId = organization, name = "execution-${UUID.randomUUID()}", enabled = true, tombstone = false),
        ).id!!
    val workspace =
      workspaceRepository
        .save(
          Workspace(name = "execution workspace", slug = UUID.randomUUID().toString(), dataplaneGroupId = group, organizationId = organization),
        ).id!!
    val workload = UUID.randomUUID().toString()
    jooqDslContext.execute(
      "INSERT INTO workload (id, status, input_payload, log_path, type, workspace_id, organization_id) VALUES (?, 'pending', 'not-read', 'not-read', 'spec', ?, ?)",
      workload,
      workspace,
      organization,
    )
    val command = UUID.randomUUID().toString()
    jooqDslContext.execute(
      "INSERT INTO commands (id, workload_id, command_type, command_input, workspace_id, organization_id) VALUES (?, ?, 'SPEC', '{\"secret\":\"not-read\"}'::jsonb, ?, ?)",
      command,
      workload,
      workspace,
      organization,
    )
    val commands = context.getBean(CommandAuthorizationRepository::class.java)
    val owner = commands.findOwnerById(command).orElseThrow()
    assertEquals(workload, owner.workloadId)
    assertEquals(workspace, owner.workspaceId)
    assertEquals(organization, owner.organizationId)
    assertEquals("SPEC", owner.commandType)
    assertEquals(owner, commands.findByIdAndOrganizationId(command, organization).orElseThrow())
    assertTrue(commands.findByIdAndOrganizationId(command, UUID.randomUUID()).isEmpty)
    assertTrue(commands.findOwnerById("missing").isEmpty)
    val workloads = context.getBean(WorkloadAuthorizationRepository::class.java)
    assertEquals(organization, workloads.findOwnerById(workload).orElseThrow().organizationId)
    assertTrue(workloads.findByIdAndOrganizationId(workload, UUID.randomUUID()).isEmpty)
    jooqDslContext.execute("UPDATE workload SET organization_id = NULL WHERE id = ?", workload)
    assertEquals(workspace, workloads.findByIdAndOrganizationId(workload, organization).orElseThrow().workspaceId)
    assertTrue(workloads.findByIdAndOrganizationId(workload, UUID.randomUUID()).isEmpty)
    jooqDslContext.execute("UPDATE workload SET type = 'check' WHERE id = ?", workload)
    assertTrue(workloads.findByIdAndOrganizationId(workload, organization).isEmpty)
    assertTrue(workloads.findOwnerById("missing").isEmpty)
  }
}
