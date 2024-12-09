package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.Workspace
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

@MicronautTest
class OrganizationRepositoryTest : AbstractConfigRepositoryTest() {
  @Test
  fun `save and retrieve organization by id`() {
    val organization =
      Organization(
        name = "Test Organization",
        email = "test@example.com",
        userId = UUID.randomUUID(),
      )
    organizationRepository.save(organization)

    val retrievedOrganization = organizationRepository.findById(organization.id!!)
    assertTrue(retrievedOrganization.isPresent)
    assertThat(retrievedOrganization.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(organization)
  }

  @Test
  fun `save with provided primary key`() {
    val providedId = UUID.randomUUID()
    val organization =
      Organization(
        id = providedId,
        name = "Test Organization",
        email = "test@example.com",
        userId = UUID.randomUUID(),
      )

    organizationRepository.save(organization)

    val retrievedOrganization = organizationRepository.findById(providedId)
    assertTrue(retrievedOrganization.isPresent)
    assertThat(retrievedOrganization.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(organization)
  }

  @Test
  fun `update organization`() {
    val organization =
      Organization(
        name = "Test Organization",
        email = "test@example.com",
        userId = UUID.randomUUID(),
      )
    organizationRepository.save(organization)

    organization.name = "Updated Organization"
    organizationRepository.update(organization)

    val updatedOrganization = organizationRepository.findById(organization.id!!)
    assertTrue(updatedOrganization.isPresent)
    assertEquals("Updated Organization", updatedOrganization.get().name)
  }

  @Test
  fun `delete organization`() {
    val organization =
      Organization(
        name = "Test Organization",
        email = "test@airbyte.io",
      )
    organizationRepository.save(organization)

    organizationRepository.deleteById(organization.id!!)

    val deletedOrganization = organizationRepository.findById(organization.id!!)
    assertTrue(deletedOrganization.isEmpty)
  }

  @Test
  fun `findById returns empty when id does not exist`() {
    val nonExistentId = UUID.randomUUID()

    val result = organizationRepository.findById(nonExistentId)
    assertTrue(result.isEmpty)
  }

  @Test
  fun `findByWorkspaceId returns organization when workspaceId exists`() {
    val organization =
      Organization(
        name = "Test Organization",
        email = "test@example.com",
        userId = UUID.randomUUID(),
      )
    val savedOrg = organizationRepository.save(organization)

    val workspace =
      Workspace(
        name = "Test Workspace",
        slug = "test-workspace",
        organizationId = savedOrg.id!!,
      )
    val savedWorkspace = workspaceRepository.save(workspace)

    val result: Optional<Organization> = organizationRepository.findByWorkspaceId(savedWorkspace.id!!)
    assertTrue(result.isPresent)
    assertThat(result.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(organization)
  }

  @Test
  fun `findByWorkspaceId returns empty when workspaceId does not exist`() {
    val workspaceId = UUID.randomUUID()

    val result: Optional<Organization> = organizationRepository.findByWorkspaceId(workspaceId)
    assertTrue(result.isEmpty)
  }
}
