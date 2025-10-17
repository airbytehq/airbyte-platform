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
  fun `findByUserId returns organizations when user has permissions`() {
    val user1Id = UUID.randomUUID()
    val user2Id = UUID.randomUUID()

    val organization1 =
      Organization(
        name = "Organization 1",
        email = "org1@example.com",
        userId = user1Id,
      )
    val savedOrg1 = organizationRepository.save(organization1)

    val organization2 =
      Organization(
        name = "Organization 2",
        email = "org2@example.com",
        userId = user2Id,
      )
    val savedOrg2 = organizationRepository.save(organization2)

    // Create organization permission for user1 on org1
    val permission1 =
      Permission(
        id = UUID.randomUUID(),
        userId = user1Id,
        organizationId = savedOrg1.id!!,
        permissionType = PermissionType.organization_admin,
      )
    permissionRepository.save(permission1)

    // Create workspace and workspace permission for user1 on org2
    val workspace =
      Workspace(
        name = "Test Workspace",
        slug = "test-workspace",
        organizationId = savedOrg2.id!!,
        dataplaneGroupId = UUID.randomUUID(),
      )
    val savedWorkspace = workspaceRepository.save(workspace)

    val permission2 =
      Permission(
        id = UUID.randomUUID(),
        userId = user1Id,
        workspaceId = savedWorkspace.id!!,
        permissionType = PermissionType.workspace_admin,
      )
    permissionRepository.save(permission2)

    val result = organizationRepository.findByUserId(user1Id, null, false)
    assertEquals(2, result.size)

    val orgNames = result.map { it.name }.toSet()
    assertTrue(orgNames.contains("Organization 1"))
    assertTrue(orgNames.contains("Organization 2"))
  }

  @Test
  fun `findByUserId filters by keyword`() {
    val userId = UUID.randomUUID()

    val organization1 =
      Organization(
        name = "Test Organization Alpha",
        email = "org1@example.com",
        userId = userId,
      )
    val savedOrg1 = organizationRepository.save(organization1)

    val organization2 =
      Organization(
        name = "Test Organization Beta",
        email = "org2@example.com",
        userId = userId,
      )
    val savedOrg2 = organizationRepository.save(organization2)

    // Create permissions for both organizations
    val permission1 =
      Permission(
        id = UUID.randomUUID(),
        userId = userId,
        organizationId = savedOrg1.id!!,
        permissionType = PermissionType.organization_admin,
      )
    permissionRepository.save(permission1)

    val permission2 =
      Permission(
        id = UUID.randomUUID(),
        userId = userId,
        organizationId = savedOrg2.id!!,
        permissionType = PermissionType.organization_admin,
      )
    permissionRepository.save(permission2)

    // Search with keyword "Alpha"
    val result = organizationRepository.findByUserId(userId, "Alpha", false)
    assertEquals(1, result.size)
    assertEquals("Test Organization Alpha", result[0].name)
  }

  @Test
  fun `findByUserId includes instance admin user for all organizations`() {
    val instanceAdminUserId = UUID.randomUUID()
    val regularUserId = UUID.randomUUID()

    val organization1 =
      Organization(
        name = "Organization 1",
        email = "org1@example.com",
        userId = regularUserId,
      )
    organizationRepository.save(organization1)

    val organization2 =
      Organization(
        name = "Organization 2",
        email = "org2@example.com",
        userId = regularUserId,
      )
    organizationRepository.save(organization2)

    // Create instance admin permission
    val instanceAdminPermission =
      Permission(
        id = UUID.randomUUID(),
        userId = instanceAdminUserId,
        permissionType = PermissionType.instance_admin,
      )
    permissionRepository.save(instanceAdminPermission)

    // Instance admin should see all organizations
    val result = organizationRepository.findByUserId(instanceAdminUserId, null, false)
    assertEquals(2, result.size)

    val orgNames = result.map { it.name }.toSet()
    assertTrue(orgNames.contains("Organization 1"))
    assertTrue(orgNames.contains("Organization 2"))
  }

  @Test
  fun `findByUserId excludes deleted organizations by default`() {
    val userId = UUID.randomUUID()

    val organization1 =
      Organization(
        name = "Active Organization",
        email = "org1@example.com",
        userId = userId,
        tombstone = false,
      )
    val savedOrg1 = organizationRepository.save(organization1)

    val organization2 =
      Organization(
        name = "Deleted Organization",
        email = "org2@example.com",
        userId = userId,
        tombstone = true,
      )
    val savedOrg2 = organizationRepository.save(organization2)

    // Create permissions for both organizations
    val permission1 =
      Permission(
        id = UUID.randomUUID(),
        userId = userId,
        organizationId = savedOrg1.id!!,
        permissionType = PermissionType.organization_admin,
      )
    permissionRepository.save(permission1)

    val permission2 =
      Permission(
        id = UUID.randomUUID(),
        userId = userId,
        organizationId = savedOrg2.id!!,
        permissionType = PermissionType.organization_admin,
      )
    permissionRepository.save(permission2)

    // Should only return active organization
    val result = organizationRepository.findByUserId(userId, null, false)
    assertEquals(1, result.size)
    assertEquals("Active Organization", result[0].name)

    // Should return both when includeDeleted is true
    val resultWithDeleted = organizationRepository.findByUserId(userId, null, true)
    assertEquals(2, resultWithDeleted.size)
  }

  @Test
  fun `findByUserIdPaginated returns paginated results`() {
    val userId = UUID.randomUUID()

    // Create 5 organizations
    (1..5).map { i ->
      val org =
        Organization(
          name = "Organization $i",
          email = "org$i@example.com",
          userId = userId,
        )
      val savedOrg = organizationRepository.save(org)

      // Create permission for each organization
      val permission =
        Permission(
          id = UUID.randomUUID(),
          userId = userId,
          organizationId = savedOrg.id!!,
          permissionType = PermissionType.organization_admin,
        )
      permissionRepository.save(permission)
      savedOrg
    }

    // Test pagination: limit 2, offset 0
    val firstPage = organizationRepository.findByUserIdPaginated(userId, null, false, 2, 0)
    assertEquals(2, firstPage.size)

    // Test pagination: limit 2, offset 2
    val secondPage = organizationRepository.findByUserIdPaginated(userId, null, false, 2, 2)
    assertEquals(2, secondPage.size)

    // Test pagination: limit 2, offset 4
    val thirdPage = organizationRepository.findByUserIdPaginated(userId, null, false, 2, 4)
    assertEquals(1, thirdPage.size)

    // Verify no overlap between pages
    val firstPageIds = firstPage.map { it.id }.toSet()
    val secondPageIds = secondPage.map { it.id }.toSet()
    val thirdPageIds = thirdPage.map { it.id }.toSet()

    assertTrue(firstPageIds.intersect(secondPageIds).isEmpty())
    assertTrue(firstPageIds.intersect(thirdPageIds).isEmpty())
    assertTrue(secondPageIds.intersect(thirdPageIds).isEmpty())
  }

  @Test
  fun `findByUserId returns empty when user has no permissions`() {
    val userWithNoPermissions = UUID.randomUUID()
    val userWithPermissions = UUID.randomUUID()

    // Create some organizations that exist in the database
    val organization1 =
      Organization(
        name = "Organization 1",
        email = "org1@example.com",
        userId = userWithPermissions,
      )
    val savedOrg1 = organizationRepository.save(organization1)

    val organization2 =
      Organization(
        name = "Organization 2",
        email = "org2@example.com",
        userId = userWithPermissions,
      )
    val savedOrg2 = organizationRepository.save(organization2)

    // Give permissions to a different user (not the one we're testing)
    val permission1 =
      Permission(
        id = UUID.randomUUID(),
        userId = userWithPermissions,
        organizationId = savedOrg1.id!!,
        permissionType = PermissionType.organization_admin,
      )
    permissionRepository.save(permission1)

    val permission2 =
      Permission(
        id = UUID.randomUUID(),
        userId = userWithPermissions,
        organizationId = savedOrg2.id!!,
        permissionType = PermissionType.organization_admin,
      )
    permissionRepository.save(permission2)

    // Test that user with no permissions gets empty result even though organizations exist
    val result = organizationRepository.findByUserId(userWithNoPermissions, null, false)
    assertTrue(result.isEmpty())

    // Verify the other user can see the organizations (to prove they exist)
    val resultWithPermissions = organizationRepository.findByUserId(userWithPermissions, null, false)
    assertEquals(2, resultWithPermissions.size)
  }
}
