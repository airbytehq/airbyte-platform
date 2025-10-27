/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.config.Organization
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.SsoConfigRepository
import io.airbyte.data.repositories.entities.OrganizationWithSsoRealm
import io.airbyte.data.repositories.entities.SsoConfig
import io.airbyte.data.services.impls.data.mappers.EntityOrganization
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated
import io.airbyte.db.instance.configs.jooq.generated.enums.SsoConfigStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

private val ORGANIZATION_ID = UUID.randomUUID()
private val CONNECTION_ID = UUID.randomUUID()
private val USER_ID = UUID.randomUUID()
private const val SSO_REALM = "test-realm"

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

private val ENTITY_ORGANIZATION =
  EntityOrganization(
    id = ORGANIZATION_ID,
    userId = BASE_ORGANIZATION.userId!!,
    name = BASE_ORGANIZATION.name,
    email = BASE_ORGANIZATION.email,
  )

private val SSO_CONFIG =
  SsoConfig(
    id = UUID.randomUUID(),
    organizationId = ORGANIZATION_ID,
    keycloakRealm = SSO_REALM,
    status = SsoConfigStatus.active,
  )

class OrganizationServiceDataImplTest {
  private lateinit var organizationServiceDataImpl: OrganizationServiceDataImpl
  private val organizationRepository: OrganizationRepository = mockk()
  private val ssoConfigRepository: SsoConfigRepository = mockk()
  private val permissionRepository: PermissionRepository = mockk()

  @BeforeEach
  fun setUp() {
    organizationServiceDataImpl = OrganizationServiceDataImpl(organizationRepository, ssoConfigRepository, permissionRepository)
  }

  @Test
  fun `getOrganization returns organization with SSO realm when id exists`() {
    every { organizationRepository.findById(ORGANIZATION_ID) } returns Optional.of(ENTITY_ORGANIZATION)
    every { ssoConfigRepository.findByOrganizationId(ORGANIZATION_ID) } returns SSO_CONFIG

    val result = organizationServiceDataImpl.getOrganization(ORGANIZATION_ID)

    assertTrue(result.isPresent)
    assertEquals(ORGANIZATION_WITH_ID.name, result.get().name)
    assertEquals(SSO_REALM, result.get().ssoRealm)
  }

  @Test
  fun `getOrganization returns organization without SSO realm when no SSO config exists`() {
    every { organizationRepository.findById(ORGANIZATION_ID) } returns Optional.of(ENTITY_ORGANIZATION)
    every { ssoConfigRepository.findByOrganizationId(ORGANIZATION_ID) } returns null

    val result = organizationServiceDataImpl.getOrganization(ORGANIZATION_ID)

    assertTrue(result.isPresent)
    assertEquals(ORGANIZATION_WITH_ID.name, result.get().name)
    assertEquals(null, result.get().ssoRealm)
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
    every { organizationRepository.findByWorkspaceId(workspaceId) } returns Optional.of(ENTITY_ORGANIZATION)
    every { ssoConfigRepository.findByOrganizationId(ORGANIZATION_ID) } returns SSO_CONFIG

    val result = organizationServiceDataImpl.getOrganizationForWorkspaceId(workspaceId)

    assertTrue(result.isPresent)
    assertEquals(ORGANIZATION_WITH_ID.name, result.get().name)
    assertEquals(SSO_REALM, result.get().ssoRealm)
  }

  @Test
  fun `getOrganizationForWorkspaceId returns empty when workspaceId does not exist`() {
    val workspaceId = UUID.randomUUID()
    every { organizationRepository.findByWorkspaceId(workspaceId) } returns Optional.empty()

    val result = organizationServiceDataImpl.getOrganizationForWorkspaceId(workspaceId)

    assertTrue(result.isEmpty)
  }

  @Test
  fun `getOrganizationForConnectionId returns organization when connectionId exists`() {
    every { organizationRepository.findByConnectionId(CONNECTION_ID) } returns Optional.of(ENTITY_ORGANIZATION)
    every { ssoConfigRepository.findByOrganizationId(ORGANIZATION_ID) } returns SSO_CONFIG

    val result = organizationServiceDataImpl.getOrganizationForConnectionId(CONNECTION_ID)

    assertTrue(result.isPresent)
    assertEquals(ORGANIZATION_WITH_ID.name, result.get().name)
    assertEquals(SSO_REALM, result.get().ssoRealm)
  }

  @Test
  fun `getOrganizationForConnectionId returns empty when connectionId does not exist`() {
    every { organizationRepository.findByConnectionId(CONNECTION_ID) } returns Optional.empty()

    val result = organizationServiceDataImpl.getOrganizationForConnectionId(CONNECTION_ID)

    assertTrue(result.isEmpty)
  }

  @Test
  fun `getDefaultOrganization returns default organization`() {
    every { organizationRepository.findById(DEFAULT_ORGANIZATION_ID) } returns Optional.of(ENTITY_ORGANIZATION)
    every { ssoConfigRepository.findByOrganizationId(DEFAULT_ORGANIZATION_ID) } returns null

    val result = organizationServiceDataImpl.getDefaultOrganization()

    assertTrue(result.isPresent)
    assertEquals(ORGANIZATION_WITH_ID.name, result.get().name)
    verify { organizationRepository.findById(DEFAULT_ORGANIZATION_ID) }
  }

  @Test
  fun `getOrganizationBySsoConfigRealm returns organization when realm exists`() {
    every { organizationRepository.findBySsoConfigRealm(SSO_REALM) } returns Optional.of(ENTITY_ORGANIZATION)
    every { ssoConfigRepository.findByOrganizationId(ORGANIZATION_ID) } returns SSO_CONFIG

    val result = organizationServiceDataImpl.getOrganizationBySsoConfigRealm(SSO_REALM)

    assertTrue(result.isPresent)
    assertEquals(ORGANIZATION_WITH_ID.name, result.get().name)
    assertEquals(SSO_REALM, result.get().ssoRealm)
  }

  @Test
  fun `getOrganizationBySsoConfigRealm returns empty when realm does not exist`() {
    every { organizationRepository.findBySsoConfigRealm(SSO_REALM) } returns Optional.empty()

    val result = organizationServiceDataImpl.getOrganizationBySsoConfigRealm(SSO_REALM)

    assertTrue(result.isEmpty)
  }

  @Test
  fun `listOrganizationsByUserId returns organizations for user`() {
    val keyword = Optional.of("test")
    val includeDeleted = false
    val organizationWithSsoRealm =
      OrganizationWithSsoRealm(
        id = ORGANIZATION_ID,
        name = ORGANIZATION_WITH_ID.name,
        userId = BASE_ORGANIZATION.userId,
        email = BASE_ORGANIZATION.email,
        tombstone = false,
        createdAt = null,
        updatedAt = null,
        keycloakRealm = SSO_REALM,
      )

    every { permissionRepository.isInstanceAdmin(USER_ID) } returns false
    every { organizationRepository.findByUserIdWithSsoRealm(USER_ID, "test", includeDeleted) } returns listOf(organizationWithSsoRealm)

    val result = organizationServiceDataImpl.listOrganizationsByUserId(USER_ID, keyword, includeDeleted)

    assertEquals(1, result.size)
    assertEquals(ORGANIZATION_WITH_ID.name, result[0].name)
    assertEquals(SSO_REALM, result[0].ssoRealm)
  }

  @Test
  fun `listOrganizationsByUserId returns empty list when no organizations found`() {
    val keyword = Optional.empty<String>()
    val includeDeleted = false

    every { permissionRepository.isInstanceAdmin(USER_ID) } returns false
    every { organizationRepository.findByUserIdWithSsoRealm(USER_ID, null, includeDeleted) } returns emptyList()

    val result = organizationServiceDataImpl.listOrganizationsByUserId(USER_ID, keyword, includeDeleted)

    assertTrue(result.isEmpty())
  }

  @Test
  fun `listOrganizationsByUserIdPaginated returns paginated organizations for user`() {
    val query = ResourcesByUserQueryPaginated(USER_ID, false, 10, 0)
    val keyword = Optional.of("test")
    val organizationWithSsoRealm =
      OrganizationWithSsoRealm(
        id = ORGANIZATION_ID,
        name = ORGANIZATION_WITH_ID.name,
        userId = BASE_ORGANIZATION.userId,
        email = BASE_ORGANIZATION.email,
        tombstone = false,
        createdAt = null,
        updatedAt = null,
        keycloakRealm = SSO_REALM,
      )

    every { permissionRepository.isInstanceAdmin(USER_ID) } returns false
    every { organizationRepository.findByUserIdPaginatedWithSsoRealm(USER_ID, "test", false, 10, 0) } returns listOf(organizationWithSsoRealm)

    val result = organizationServiceDataImpl.listOrganizationsByUserIdPaginated(query, keyword)

    assertEquals(1, result.size)
    assertEquals(ORGANIZATION_WITH_ID.name, result[0].name)
    assertEquals(SSO_REALM, result[0].ssoRealm)
  }

  @Test
  fun `listOrganizationsByUserIdPaginated returns empty list when no organizations found`() {
    val query = ResourcesByUserQueryPaginated(USER_ID, true, 5, 10)
    val keyword = Optional.empty<String>()

    every { permissionRepository.isInstanceAdmin(USER_ID) } returns false
    every { organizationRepository.findByUserIdPaginatedWithSsoRealm(USER_ID, null, true, 5, 10) } returns emptyList()

    val result = organizationServiceDataImpl.listOrganizationsByUserIdPaginated(query, keyword)

    assertTrue(result.isEmpty())
  }

  @Test
  fun `listOrganizationsByUserId uses optimized query for instance admin`() {
    val keyword = Optional.of("test")
    val includeDeleted = false
    val organizationWithSsoRealm =
      OrganizationWithSsoRealm(
        id = ORGANIZATION_ID,
        name = ORGANIZATION_WITH_ID.name,
        userId = BASE_ORGANIZATION.userId,
        email = BASE_ORGANIZATION.email,
        tombstone = false,
        createdAt = null,
        updatedAt = null,
        keycloakRealm = SSO_REALM,
      )

    every { permissionRepository.isInstanceAdmin(USER_ID) } returns true
    every { organizationRepository.findAllWithSsoRealm("test", includeDeleted) } returns listOf(organizationWithSsoRealm)

    val result = organizationServiceDataImpl.listOrganizationsByUserId(USER_ID, keyword, includeDeleted)

    assertEquals(1, result.size)
    assertEquals(ORGANIZATION_WITH_ID.name, result[0].name)
    assertEquals(SSO_REALM, result[0].ssoRealm)
    verify(exactly = 0) { organizationRepository.findByUserIdWithSsoRealm(any(), any(), any()) }
  }

  @Test
  fun `listOrganizationsByUserIdPaginated uses optimized query for instance admin`() {
    val query = ResourcesByUserQueryPaginated(USER_ID, false, 10, 0)
    val keyword = Optional.of("test")
    val organizationWithSsoRealm =
      OrganizationWithSsoRealm(
        id = ORGANIZATION_ID,
        name = ORGANIZATION_WITH_ID.name,
        userId = BASE_ORGANIZATION.userId,
        email = BASE_ORGANIZATION.email,
        tombstone = false,
        createdAt = null,
        updatedAt = null,
        keycloakRealm = SSO_REALM,
      )

    every { permissionRepository.isInstanceAdmin(USER_ID) } returns true
    every { organizationRepository.findAllPaginatedWithSsoRealm("test", false, 10, 0) } returns listOf(organizationWithSsoRealm)

    val result = organizationServiceDataImpl.listOrganizationsByUserIdPaginated(query, keyword)

    assertEquals(1, result.size)
    assertEquals(ORGANIZATION_WITH_ID.name, result[0].name)
    assertEquals(SSO_REALM, result[0].ssoRealm)
    verify(exactly = 0) { organizationRepository.findByUserIdPaginatedWithSsoRealm(any(), any(), any(), any(), any()) }
  }
}
