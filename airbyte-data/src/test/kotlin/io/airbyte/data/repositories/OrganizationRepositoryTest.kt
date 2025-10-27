/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.Permission
import io.airbyte.data.repositories.entities.SsoConfig
import io.airbyte.data.repositories.entities.Workspace
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.db.instance.configs.jooq.generated.enums.SsoConfigStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

@MicronautTest
class OrganizationRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // Drop foreign key constraints so we don't have to deal with creating related entities
      jooqDslContext
        .alterTable(Tables.ACTOR)
        .dropForeignKey(Keys.ACTOR__ACTOR_ACTOR_DEFINITION_ID_FKEY.constraint())
        .execute()
      jooqDslContext
        .alterTable(Tables.ACTOR)
        .dropForeignKey(Keys.ACTOR__ACTOR_WORKSPACE_ID_FKEY.constraint())
        .execute()
      jooqDslContext
        .alterTable(Tables.CONNECTION)
        .dropForeignKey(Keys.CONNECTION__CONNECTION_SOURCE_ID_FKEY.constraint())
        .execute()
      jooqDslContext
        .alterTable(Tables.CONNECTION)
        .dropForeignKey(Keys.CONNECTION__CONNECTION_DESTINATION_ID_FKEY.constraint())
        .execute()
      jooqDslContext
        .alterTable(Tables.PERMISSION)
        .dropForeignKey(Keys.PERMISSION__PERMISSION_USER_ID_FKEY.constraint())
        .execute()
    }
  }

  // Test helper methods
  private fun createOrganization(
    name: String,
    email: String = "$name@example.com",
    tombstone: Boolean = false,
  ): Organization =
    organizationRepository.save(
      Organization(
        name = name,
        email = email,
        userId = UUID.randomUUID(),
        tombstone = tombstone,
      ),
    )

  private fun createOrganizationWithSso(
    name: String,
    ssoRealm: String,
  ): Organization {
    val org = createOrganization(name)
    ssoConfigRepository.save(
      SsoConfig(
        id = UUID.randomUUID(),
        organizationId = org.id!!,
        keycloakRealm = ssoRealm,
        status = SsoConfigStatus.active,
      ),
    )
    return org
  }

  private fun grantOrganizationPermission(
    userId: UUID,
    organizationId: UUID,
    permissionType: PermissionType = PermissionType.organization_admin,
  ) {
    permissionRepository.save(
      Permission(
        userId = userId,
        organizationId = organizationId,
        permissionType = permissionType,
      ),
    )
  }

  private fun grantWorkspacePermission(
    userId: UUID,
    workspaceId: UUID,
    permissionType: PermissionType = PermissionType.workspace_admin,
  ) {
    permissionRepository.save(
      Permission(
        userId = userId,
        workspaceId = workspaceId,
        permissionType = permissionType,
      ),
    )
  }

  private fun grantInstanceAdminPermission(userId: UUID) {
    permissionRepository.save(
      Permission(
        userId = userId,
        permissionType = PermissionType.instance_admin,
      ),
    )
  }

  @AfterEach
  fun cleanup() {
    // Clean up test data to prevent interference between tests
    jooqDslContext.deleteFrom(Tables.CONNECTION).execute()
    jooqDslContext.deleteFrom(Tables.ACTOR).execute()
    jooqDslContext.deleteFrom(Tables.PERMISSION).execute()
    jooqDslContext.deleteFrom(Tables.SSO_CONFIG).execute()
    jooqDslContext.deleteFrom(Tables.WORKSPACE).execute()
    jooqDslContext.deleteFrom(Tables.ORGANIZATION).execute()
  }

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
        dataplaneGroupId = UUID.randomUUID(),
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

  @Test
  fun `findByConnectionId returns organization when connectionId exists`() {
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
        dataplaneGroupId = UUID.randomUUID(),
      )
    val savedWorkspace = workspaceRepository.save(workspace)

    // Create test data directly via JOOQ to avoid complex entity dependencies
    val actorId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()
    val destinationActorId = UUID.randomUUID()

    // Insert minimal actor record
    jooqDslContext
      .insertInto(Tables.ACTOR)
      .set(Tables.ACTOR.ID, actorId)
      .set(Tables.ACTOR.WORKSPACE_ID, savedWorkspace.id!!)
      .set(Tables.ACTOR.ACTOR_DEFINITION_ID, UUID.randomUUID())
      .set(Tables.ACTOR.NAME, "Test Source Actor")
      .set(Tables.ACTOR.CONFIGURATION, org.jooq.JSONB.valueOf("{}"))
      .set(
        Tables.ACTOR.ACTOR_TYPE,
        ActorType.source,
      ).execute()

    // Insert minimal destination actor record
    jooqDslContext
      .insertInto(Tables.ACTOR)
      .set(Tables.ACTOR.ID, destinationActorId)
      .set(Tables.ACTOR.WORKSPACE_ID, savedWorkspace.id!!)
      .set(Tables.ACTOR.ACTOR_DEFINITION_ID, UUID.randomUUID())
      .set(Tables.ACTOR.NAME, "Test Destination Actor")
      .set(Tables.ACTOR.CONFIGURATION, org.jooq.JSONB.valueOf("{}"))
      .set(
        Tables.ACTOR.ACTOR_TYPE,
        ActorType.destination,
      ).execute()

    // Insert minimal connection record
    jooqDslContext
      .insertInto(Tables.CONNECTION)
      .set(Tables.CONNECTION.ID, connectionId)
      .set(Tables.CONNECTION.SOURCE_ID, actorId)
      .set(Tables.CONNECTION.DESTINATION_ID, destinationActorId)
      .set(Tables.CONNECTION.NAME, "Test Connection")
      .set(
        Tables.CONNECTION.STATUS,
        StatusType.active,
      ).set(
        Tables.CONNECTION.NAMESPACE_DEFINITION,
        NamespaceDefinitionType.source,
      ).set(Tables.CONNECTION.CATALOG, org.jooq.JSONB.valueOf("{}"))
      .execute()

    val result: Optional<Organization> = organizationRepository.findByConnectionId(connectionId)
    assertTrue(result.isPresent)
    assertThat(result.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(organization)
  }

  @Test
  fun `findByConnectionId returns empty when connectionId does not exist`() {
    val connectionId = UUID.randomUUID()

    val result: Optional<Organization> = organizationRepository.findByConnectionId(connectionId)
    assertTrue(result.isEmpty)
  }

  @Test
  fun `findBySsoConfigRealm returns organization when sso config realm exists`() {
    val organization =
      Organization(
        name = "Test Organization",
        email = "test@example.com",
        userId = UUID.randomUUID(),
      )
    val savedOrg = organizationRepository.save(organization)

    val ssoConfig =
      SsoConfig(
        id = UUID.randomUUID(),
        organizationId = savedOrg.id!!,
        keycloakRealm = "test-realm",
        status = SsoConfigStatus.active,
      )
    ssoConfigRepository.save(ssoConfig)

    val result: Optional<Organization> = organizationRepository.findBySsoConfigRealm("test-realm")
    assertTrue(result.isPresent)
    assertThat(result.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(organization)
  }

  @Test
  fun `findBySsoConfigRealm returns empty when sso config realm does not exist`() {
    val result: Optional<Organization> = organizationRepository.findBySsoConfigRealm("nonexistent-realm")
    assertTrue(result.isEmpty)
  }

  @Test
  fun `findByUserIdWithSsoRealm returns organizations with direct organization permissions`() {
    val userId = UUID.randomUUID()
    val org1 = createOrganizationWithSso("Org 1", "test-realm")
    val org2 = createOrganization("Org 2")

    grantOrganizationPermission(userId, org1.id!!, PermissionType.organization_admin)
    grantOrganizationPermission(userId, org2.id!!, PermissionType.organization_member)

    val result = organizationRepository.findByUserIdWithSsoRealm(userId, null, false)

    assertEquals(2, result.size)
    assertEquals("Org 1", result[0].name)
    assertEquals("test-realm", result[0].keycloakRealm)
    assertEquals("Org 2", result[1].name)
    assertEquals(null, result[1].keycloakRealm)
  }

  @Test
  fun `findByUserIdWithSsoRealm returns organizations with workspace permissions`() {
    val userId = UUID.randomUUID()
    val org = createOrganization("Test Organization")
    val workspace =
      workspaceRepository.save(
        Workspace(
          name = "Test Workspace",
          slug = "test-workspace",
          organizationId = org.id!!,
          dataplaneGroupId = UUID.randomUUID(),
        ),
      )

    grantWorkspacePermission(userId, workspace.id!!)

    val result = organizationRepository.findByUserIdWithSsoRealm(userId, null, false)

    assertEquals(1, result.size)
    assertEquals("Test Organization", result[0].name)
  }

  @Test
  fun `findByUserIdWithSsoRealm returns all organizations for instance admin`() {
    val userId = UUID.randomUUID()
    createOrganization("Org 1")
    createOrganization("Org 2")

    grantInstanceAdminPermission(userId)

    val result = organizationRepository.findByUserIdWithSsoRealm(userId, null, false)

    assertEquals(2, result.size)
  }

  @Test
  fun `findByUserIdWithSsoRealm filters by keyword`() {
    val userId = UUID.randomUUID()
    val org1 = createOrganization("Apple Organization")
    val org2 = createOrganization("Banana Company")

    grantOrganizationPermission(userId, org1.id!!)
    grantOrganizationPermission(userId, org2.id!!)

    val result = organizationRepository.findByUserIdWithSsoRealm(userId, "apple", false)

    assertEquals(1, result.size)
    assertEquals("Apple Organization", result[0].name)
  }

  @Test
  fun `findByUserIdWithSsoRealm excludes tombstoned organizations by default`() {
    val userId = UUID.randomUUID()
    val activeOrg = createOrganization("Active Organization", tombstone = false)
    val deletedOrg = createOrganization("Deleted Organization", tombstone = true)

    grantOrganizationPermission(userId, activeOrg.id!!)
    grantOrganizationPermission(userId, deletedOrg.id!!)

    val result = organizationRepository.findByUserIdWithSsoRealm(userId, null, false)

    assertEquals(1, result.size)
    assertEquals("Active Organization", result[0].name)
  }

  @Test
  fun `findByUserIdWithSsoRealm includes tombstoned organizations when requested`() {
    val userId = UUID.randomUUID()
    val activeOrg = createOrganization("Active Organization", tombstone = false)
    val deletedOrg = createOrganization("Deleted Organization", tombstone = true)

    grantOrganizationPermission(userId, activeOrg.id!!)
    grantOrganizationPermission(userId, deletedOrg.id!!)

    val result = organizationRepository.findByUserIdWithSsoRealm(userId, null, true)

    assertEquals(2, result.size)
  }

  @Test
  fun `findByUserIdWithSsoRealm returns empty list when user has no permissions`() {
    val userId = UUID.randomUUID()
    createOrganization("Test Organization")

    val result = organizationRepository.findByUserIdWithSsoRealm(userId, null, false)

    assertTrue(result.isEmpty())
  }

  @Test
  fun `findByUserIdPaginatedWithSsoRealm returns paginated results`() {
    val userId = UUID.randomUUID()

    // Create 5 organizations with permissions
    repeat(5) { i ->
      val org = createOrganization("Organization ${i + 1}")
      grantOrganizationPermission(userId, org.id!!)
    }

    val page1 = organizationRepository.findByUserIdPaginatedWithSsoRealm(userId, null, false, 2, 0)
    assertEquals(2, page1.size)
    assertEquals("Organization 1", page1[0].name)
    assertEquals("Organization 2", page1[1].name)

    val page2 = organizationRepository.findByUserIdPaginatedWithSsoRealm(userId, null, false, 2, 2)
    assertEquals(2, page2.size)
    assertEquals("Organization 3", page2[0].name)
    assertEquals("Organization 4", page2[1].name)

    val page3 = organizationRepository.findByUserIdPaginatedWithSsoRealm(userId, null, false, 2, 4)
    assertEquals(1, page3.size)
    assertEquals("Organization 5", page3[0].name)
  }

  @Test
  fun `findByUserIdPaginatedWithSsoRealm respects filters`() {
    val userId = UUID.randomUUID()
    val activeOrg = createOrganization("Active Organization", tombstone = false)
    val deletedOrg = createOrganization("Another Active", tombstone = true)

    grantOrganizationPermission(userId, activeOrg.id!!)
    grantOrganizationPermission(userId, deletedOrg.id!!)

    val result = organizationRepository.findByUserIdPaginatedWithSsoRealm(userId, "active", false, 10, 0)

    assertEquals(1, result.size)
    assertEquals("Active Organization", result[0].name)
  }
}
