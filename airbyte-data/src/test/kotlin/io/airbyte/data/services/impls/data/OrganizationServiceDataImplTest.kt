package io.airbyte.data.services.impls.data

import io.airbyte.config.Organization
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.services.impls.data.mappers.EntityOrganization
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

private val ORGANIZATION_ID = UUID.randomUUID()

private val BASE_ORGANIZATION =
  Organization().apply {
    userId = UUID.randomUUID()
    name = "Test Organization"
    email = "test@airbyte.io"
  }

private val ORGANIZATION_WITH_ID =
  Organization().apply {
    organizationId = ORGANIZATION_ID
    userId = BASE_ORGANIZATION.userId
    name = BASE_ORGANIZATION.name
    email = BASE_ORGANIZATION.email
  }

class OrganizationServiceDataImplTest {
  private lateinit var organizationServiceDataImpl: OrganizationServiceDataImpl
  private val organizationRepository: OrganizationRepository = mockk()

  @BeforeEach
  fun setUp() {
    organizationServiceDataImpl = OrganizationServiceDataImpl(organizationRepository)
  }

  @Test
  fun `getOrganization returns organization when id exists`() {
    every { organizationRepository.findById(ORGANIZATION_ID) } returns Optional.of(ORGANIZATION_WITH_ID.toEntity())

    val result = organizationServiceDataImpl.getOrganization(ORGANIZATION_ID)

    assertTrue(result.isPresent)
    assertEquals(ORGANIZATION_WITH_ID, result.get())
  }

  @Test
  fun `getOrganization returns empty when id does not exist`() {
    every { organizationRepository.findById(ORGANIZATION_ID) } returns Optional.empty()

    val result = organizationServiceDataImpl.getOrganization(ORGANIZATION_ID)

    assertTrue(result.isEmpty)
  }

  @Test
  fun `writeOrganization saves new organization when id is null`() {
    every { organizationRepository.save(any()) } returns null

    organizationServiceDataImpl.writeOrganization(BASE_ORGANIZATION)

    verify {
      organizationRepository.save(
        match<EntityOrganization> {
          it.id == null &&
            it.name == BASE_ORGANIZATION.name
        },
      )
    }
  }

  @Test
  fun `writeOrganization updates existing organization when id exists`() {
    every { organizationRepository.existsById(ORGANIZATION_ID) } returns true
    every { organizationRepository.update(any()) } returns null

    organizationServiceDataImpl.writeOrganization(ORGANIZATION_WITH_ID)

    verify {
      organizationRepository.update(
        match<EntityOrganization> {
          it.id == ORGANIZATION_ID &&
            it.name == ORGANIZATION_WITH_ID.name
        },
      )
    }
  }

  @Test
  fun `writeOrganization saves new organization with provided id for non-existing organization`() {
    every { organizationRepository.existsById(ORGANIZATION_ID) } returns false
    every { organizationRepository.save(any()) } returns null

    organizationServiceDataImpl.writeOrganization(ORGANIZATION_WITH_ID)

    verify {
      organizationRepository.save(
        match<EntityOrganization> {
          it.id == ORGANIZATION_ID &&
            it.name == ORGANIZATION_WITH_ID.name
        },
      )
    }
  }

  @Test
  fun `getOrganizationForWorkspaceId returns organization when workspaceId exists`() {
    val workspaceId = UUID.randomUUID()
    every { organizationRepository.findByWorkspaceId(workspaceId) } returns Optional.of(ORGANIZATION_WITH_ID.toEntity())

    val result = organizationServiceDataImpl.getOrganizationForWorkspaceId(workspaceId)

    assertTrue(result.isPresent)
    assertEquals(ORGANIZATION_WITH_ID, result.get())
  }

  @Test
  fun `getOrganizationForWorkspaceId returns empty when workspaceId does not exist`() {
    val workspaceId = UUID.randomUUID()
    every { organizationRepository.findByWorkspaceId(workspaceId) } returns Optional.empty()

    val result = organizationServiceDataImpl.getOrganizationForWorkspaceId(workspaceId)

    assertTrue(result.isEmpty)
  }
}
