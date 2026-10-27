/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.data.repositories.CommandAuthorizationRepository
import io.airbyte.data.repositories.WorkloadAuthorizationRepository
import io.airbyte.data.repositories.entities.CommandAuthorizationMetadata
import io.airbyte.data.repositories.entities.WorkloadAuthorizationMetadata
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class SavedExecutionAuthorizationDataServiceTest {
  private val workspace = UUID(0, 1)
  private val organization = UUID(0, 2)
  private val other = UUID(0, 3)
  private val owner = WorkspaceAndOrganization(workspace, organization)
  private val commands = mockk<CommandAuthorizationRepository>()
  private val workloads = mockk<WorkloadAuthorizationRepository>()
  private val data = mockk<AuthorizationDataService>()
  private val service = SavedExecutionAuthorizationDataService(commands, workloads, data)

  @Test
  fun `command discovery validates exact ID saved workspace organization and scoped row without reading input`() {
    val saved = CommandAuthorizationMetadata("command", "workload", workspace, organization, "SPEC")
    every { commands.findOwnerById("command") } returns Optional.of(saved)
    every { commands.findByIdAndOrganizationId("command", organization) } returns Optional.of(saved)
    every { data.savedWorkspaceAndOrganization(workspace, true) } returns owner
    every { workloads.findByIdAndOrganizationId("workload", organization) } returns
      Optional.of(WorkloadAuthorizationMetadata("workload", workspace, organization, "spec"))
    assertEquals(owner, service.command("command"))
    assertEquals(owner, service.specCommand("command"))
    every { workloads.findByIdAndOrganizationId("workload", organization) } returns
      Optional.of(WorkloadAuthorizationMetadata("workload", workspace, null, "spec"))
    assertEquals(owner, service.command("command"))
    every { commands.findOwnerById("command") } returns Optional.of(saved.copy(id = "wrong"))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.command("command") }
    every { commands.findOwnerById("command") } returns Optional.of(saved.copy(organizationId = other))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.command("command") }
    every { commands.findOwnerById("command") } returns Optional.of(saved)
    every { commands.findByIdAndOrganizationId("command", organization) } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.command("command") }
    every { commands.findByIdAndOrganizationId("command", organization) } returns Optional.of(saved.copy(commandType = "CHECK"))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.command("command") }
    every { commands.findOwnerById("command") } returns Optional.of(saved.copy(commandType = "CHECK"))
    assertEquals(owner, service.command("command"))
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.specCommand("command") }
    every { commands.findOwnerById("missing") } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.command("missing") }
    val failure = IOException("commands unavailable")
    every { commands.findOwnerById("command") } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.command("command") })
  }

  @Test
  fun `saved executions retain ownership through tombstoned workspaces without requiring active creation scope`() {
    val command = CommandAuthorizationMetadata("command", "workload", workspace, organization, "SPEC")
    val workload = WorkloadAuthorizationMetadata("workload", workspace, organization, "spec")
    every { commands.findOwnerById("command") } returns Optional.of(command)
    every { commands.findByIdAndOrganizationId("command", organization) } returns Optional.of(command)
    every { workloads.findOwnerById("workload") } returns Optional.of(workload)
    every { workloads.findByIdAndOrganizationId("workload", organization) } returns Optional.of(workload)
    every { data.workspaceAndOrganization(workspace) } throws AuthorizationResourceNotFoundException()
    every { data.savedWorkspaceAndOrganization(workspace, true) } returns owner
    assertAll(
      { assertEquals(owner, service.command("command")) },
      { assertEquals(owner, service.specCommand("command")) },
      { assertEquals(AuthorizationTarget.Workspace(owner), service.workload("workload")) },
    )
  }

  @Test
  fun `saved command ownership rejects missing or foreign associated workload metadata`() {
    val command = CommandAuthorizationMetadata("command", "workload", workspace, organization, "SPEC")
    every { commands.findOwnerById("command") } returns Optional.of(command)
    every { commands.findByIdAndOrganizationId("command", organization) } returns Optional.of(command)
    every { data.savedWorkspaceAndOrganization(workspace, true) } returns owner
    val workload = WorkloadAuthorizationMetadata("workload", workspace, organization, "spec")
    assertAll(
      listOf(
        Optional.empty(),
        Optional.of(workload.copy(id = "wrong")),
        Optional.of(workload.copy(workspaceId = other)),
        Optional.of(workload.copy(organizationId = other)),
      ).map { candidate ->
        org.junit.jupiter.api.function.Executable {
          every { workloads.findByIdAndOrganizationId(any(), organization) } returns candidate
          assertThrows(AuthorizationResourceNotFoundException::class.java) { service.command("command") }
        }
      },
    )
    val failure = IOException("associated workload metadata unavailable")
    every { workloads.findByIdAndOrganizationId("workload", organization) } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.command("command") })
  }

  @Test
  fun `workload discovery accepts saved owners and legacy workspace SPEC but never ownerless or inconsistent rows`() {
    val saved = WorkloadAuthorizationMetadata("workload", workspace, organization, "spec")
    every { workloads.findOwnerById("workload") } returns Optional.of(saved)
    every { workloads.findByIdAndOrganizationId("workload", organization) } returns Optional.of(saved)
    every { data.savedWorkspaceAndOrganization(workspace, true) } returns owner
    assertEquals(AuthorizationTarget.Workspace(owner), service.workload("workload"))
    for (candidate in listOf(saved.copy(organizationId = null), saved.copy(workspaceId = null))) {
      every { workloads.findOwnerById("workload") } returns Optional.of(candidate)
      every { workloads.findByIdAndOrganizationId("workload", organization) } returns Optional.of(candidate)
      every { data.organization(organization) } returns organization
      assertEquals(
        if (candidate.workspaceId ==
          null
        ) {
          AuthorizationTarget.Organization(organization)
        } else {
          AuthorizationTarget.Workspace(owner)
        },
        service.workload("workload"),
      )
    }
    for (candidate in listOf(
      saved.copy(id = "wrong"),
      saved.copy(organizationId = other),
      saved.copy(workspaceId = null, organizationId = null),
      saved.copy(organizationId = null, type = "check"),
    )) {
      every { workloads.findOwnerById("workload") } returns Optional.of(candidate)
      assertThrows(AuthorizationResourceNotFoundException::class.java) { service.workload("workload") }
    }
    every { workloads.findOwnerById("missing") } returns Optional.empty()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { service.workload("missing") }
    val failure = IOException("workloads unavailable")
    every { workloads.findOwnerById("workload") } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { service.workload("workload") })
  }
}
